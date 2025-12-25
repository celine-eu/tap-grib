import numpy as np
from datetime import datetime, timezone, timedelta
from singer_sdk.streams import Stream
from singer_sdk import typing as th
import typing as t
from tap_grib.storage import Storage
import tempfile
import shutil
import os
import pygrib

SDC_INCREMENTAL_KEY = "_sdc_last_modified"
SDC_FILENAME = "_sdc_filename"

INSTANTANEOUS_PDTS = {0, 1, 2, 3}


def parse_bookmark(val: str | None) -> datetime | None:
    if not val:
        return None
    clean = val.replace("Z", "+00:00")
    return datetime.fromisoformat(clean).astimezone(timezone.utc)


def safe_get(msg, key, default=None):
    try:
        return getattr(msg, key)
    except (AttributeError, RuntimeError):
        return default


def _compute_time_metadata(msg: t.Any, valid_dt: datetime | None):
    """
    Best-effort extraction of forecast time metadata in a generic way.

    Returns:
        base_datetime: analysis/reference time of the forecast (if known)
        forecast_time: numeric forecast time (if known)
        forecast_time_units: units of forecast_time (string, if known)
    """
    # 1) Start with pygrib's analDate (analysis/reference time)
    base_dt = safe_get(msg, "analDate", None)

    # 2) Fallback to dataDate/dataTime if present
    if base_dt is None:
        data_date = safe_get(msg, "dataDate", None)
        data_time = safe_get(msg, "dataTime", None)
        if data_date is not None:
            year = data_date // 10000
            month = (data_date // 100) % 100
            day = data_date % 100
            if data_time is None:
                data_time = 0
            hour = data_time // 100
            minute = data_time % 100
            base_dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

    # 3) Raw forecast time and its units (generic GRIB/pygrib interface)
    forecast_time = safe_get(msg, "forecastTime", None)
    forecast_time_units = safe_get(msg, "fcstimeunits", None)

    # 4) As a last resort, derive base_dt from valid_dt and forecast_time
    #    using the declared forecast_time_units.
    if (
        base_dt is None
        and valid_dt is not None
        and isinstance(forecast_time, (int, float))
        and forecast_time_units
    ):
        units = forecast_time_units.lower()
        try:
            if "hour" in units:
                base_dt = valid_dt - timedelta(hours=float(forecast_time))
            elif "min" in units:
                base_dt = valid_dt - timedelta(minutes=float(forecast_time))
            elif "sec" in units:
                base_dt = valid_dt - timedelta(seconds=float(forecast_time))
            elif "day" in units:
                base_dt = valid_dt - timedelta(days=float(forecast_time))
        except Exception:
            # If anything goes wrong, just leave base_dt as None
            pass

    return base_dt, forecast_time, forecast_time_units


def _extract_grid(msg: t.Any):
    """Return (lats, lons, vals) as 1-D numpy arrays for any GRIB message."""
    try:
        lats, lons = msg.latlons()
        vals = msg.values
    except Exception:
        # Fallback for single-point messages
        lat = getattr(msg, "latitude", None)
        lon = getattr(msg, "longitude", None)
        val = getattr(msg, "value", None) or getattr(msg, "data", None)
        if lat is None or lon is None or val is None:
            return np.array([]), np.array([]), np.array([])
        return (
            np.array([float(lat)]),
            np.array([float(lon)]),
            np.array([float(val)]),
        )

    # Normalize scalars to arrays
    if np.isscalar(vals):
        vals = np.array([float(t.cast(float, vals))])
        lat0 = float(lats.flat[0]) if hasattr(lats, "flat") else float(lats)
        lon0 = float(lons.flat[0]) if hasattr(lons, "flat") else float(lons)
        return np.array([lat0]), np.array([lon0]), vals

    return lats.ravel(), lons.ravel(), vals.ravel()


def to_iso8601(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


class GribStream(Stream):
    """Stream that reads records from a GRIB file in normalized (long) format."""

    DEFAULT_PKEY = [
        "base_datetime",
        "datetime",
        "forecast_step",
        "lat",
        "lon",
        "name",
    ]

    CORE_FIELDS = {"datetime", "lat", "lon", "name", "value"}

    def __init__(
        self,
        tap,
        name: str,
        file_path: str | None = None,
        primary_keys: list[str] | None = None,
        skip_past_reference: str | None = None,
        skip_past: bool | None = False,
        ignore_fields: set[str] | None = None,
        extra_files: list[str] | None = None,
        bboxes: list[tuple[float, float, float, float]] | None = None,
        **kwargs,
    ):

        super().__init__(tap=tap, name=name, **kwargs)

        self.file_path = file_path
        self.extra_files = extra_files or ([file_path] if file_path else [])

        self.primary_keys = primary_keys or self.DEFAULT_PKEY

        self.bboxes = bboxes

        self.skip_past = skip_past

        ref_dt: datetime | None = None
        if skip_past_reference:
            try:
                ref_dt = datetime.fromisoformat(skip_past_reference)
                if ref_dt.tzinfo is None:
                    ref_dt = ref_dt.replace(tzinfo=timezone.utc)
                else:
                    ref_dt = ref_dt.astimezone(timezone.utc)
            except Exception:
                tap.logger.warning(
                    "Invalid skip_past_reference_datetime '%s', ignoring",
                    skip_past_reference,
                )

        self.skip_past_reference: datetime | None = ref_dt

        ignore_fields = ignore_fields or set()
        invalid = ignore_fields & self.CORE_FIELDS
        if invalid:
            raise ValueError(f"Cannot ignore core fields: {', '.join(sorted(invalid))}")
        self.ignore_fields = ignore_fields

        self.state_partitioning_keys = [SDC_FILENAME]
        self.replication_key = SDC_INCREMENTAL_KEY
        self.forced_replication_method = "INCREMENTAL"

    @property
    def is_sorted(self) -> bool:
        """The stream returns records in order."""
        return False

    # --------------------------
    # Schema
    # --------------------------
    @property
    def schema(self) -> dict:
        props: t.List[th.Property] = [
            th.Property("datetime", th.DateTimeType()),
            th.Property("base_datetime", th.DateTimeType(nullable=True)),
            th.Property("lat", th.NumberType()),
            th.Property("lon", th.NumberType()),
            th.Property("level_type", th.StringType(nullable=True)),
            th.Property("level", th.IntegerType(nullable=True)),
            th.Property("name", th.StringType()),
            th.Property("value", th.NumberType()),
            th.Property("ensemble", th.IntegerType(nullable=True)),
            th.Property("forecast_step", th.NumberType(nullable=True)),
            th.Property("forecast_time", th.NumberType(nullable=True)),
            th.Property("forecast_time_units", th.StringType(nullable=True)),
            th.Property("edition", th.IntegerType(nullable=True)),
            th.Property("centre", th.StringType(nullable=True)),
            th.Property("data_type", th.StringType(nullable=True)),
            th.Property("grid_type", th.StringType(nullable=True)),
            th.Property(
                SDC_INCREMENTAL_KEY,
                th.DateTimeType(nullable=True),
                description="Replication checkpoint (file mtime or row date)",
            ),
            th.Property(
                SDC_FILENAME,
                th.StringType(nullable=True),
                description="Filename reference",
            ),
        ]
        # filter out ignored fields
        props = [p for p in props if p.name not in self.ignore_fields]
        return th.PropertiesList(*props).to_dict()

    # --------------------------
    # Record extraction
    # --------------------------
    def get_records(
        self, context: t.Mapping[str, t.Any] | None
    ) -> t.Iterable[
        dict[str, t.Any] | tuple[dict[t.Any, t.Any], dict[t.Any, t.Any] | None]
    ]:
        for path in self.extra_files:
            self.logger.info(f"[{self.name}] Streaming records from {path}")
            storage = Storage(path)
            info = storage.describe(path)
            mtime = info.mtime
            filename = info.path

            last_bookmark = self.get_starting_replication_key_value(context)

            bookmark_dt = parse_bookmark(last_bookmark)

            mtime = info.mtime

            self.logger.debug(
                "Partition context: %s, last_bookmark=%s, mtime=%s",
                context,
                bookmark_dt,
                mtime,
            )

            # skip if already processed
            if bookmark_dt and mtime <= bookmark_dt:
                self.logger.info(
                    "Skipping %s (mtime=%s <= bookmark=%s)",
                    filename,
                    mtime,
                    bookmark_dt,
                )
                continue

            # open GRIB file (works for remote by copying to tmp first)
            tmp_path: str | None = None
            with storage.open(path, "rb") as src:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".grib") as tmp:
                    shutil.copyfileobj(src, tmp)
                    tmp_path = tmp.name

            try:
                if tmp_path is None:
                    raise Exception(f"temporary file path (tmp_path) is not available")

                with pygrib.open(tmp_path) as grbs:  # type: ignore[attr-defined]

                    # Compute cutoff once per file
                    cutoff: datetime | None = None
                    if self.skip_past:
                        cutoff = self.skip_past_reference or datetime.now(timezone.utc)

                    for msg in grbs:
                        try:
                            lats, lons, vals = _extract_grid(msg)
                        except Exception as e:
                            self.logger.warning(f"Skipping message: {e}")
                            continue
                        if lats.size == 0:
                            continue

                        # safe datetime extraction
                        valid_dt = getattr(msg, "validDate", None)
                        if valid_dt is None:
                            date = getattr(msg, "dataDate", None)
                            time = getattr(msg, "dataTime", 0)
                            if date:
                                year = date // 10000
                                month = (date // 100) % 100
                                day = date % 100
                                hour = time // 100
                                minute = time % 100
                                valid_dt = datetime(
                                    year, month, day, hour, minute, tzinfo=timezone.utc
                                )

                        if isinstance(valid_dt, datetime):
                            if valid_dt.tzinfo is None:
                                valid_dt = valid_dt.replace(tzinfo=timezone.utc)
                            else:
                                valid_dt = valid_dt.astimezone(timezone.utc)
                        else:
                            valid_dt = None

                        drop_record = False
                        # Attempt to filter by istantaneous values otherwise keep the record
                        # https://codes.ecmwf.int/grib/format/grib2/ctables/4/0/
                        if cutoff is not None:
                            pdt = safe_get(msg, "productDefinitionTemplateNumber")
                            is_instantaneous = (
                                pdt in INSTANTANEOUS_PDTS if pdt else False
                            )
                            drop_record = (
                                is_instantaneous
                                and valid_dt is not None
                                and valid_dt < cutoff
                            )

                        if drop_record:
                            # this forecast is already in the past; drop the whole message
                            continue

                        raw_step = safe_get(msg, "step", None)
                        forecast_step = None
                        if isinstance(raw_step, (int, float)):
                            forecast_step = raw_step
                        elif isinstance(raw_step, str):
                            # normalize string like "3h", "12H", "15m"
                            s = raw_step.strip().lower()
                            try:
                                if s.endswith("h"):
                                    forecast_step = float(
                                        s[:-1]
                                    )  # convert hours to seconds
                                elif s.endswith("m"):
                                    forecast_step = (
                                        float(s[:-1]) * 60
                                    )  # convert minutes to seconds
                                else:
                                    forecast_step = float(s)
                            except Exception:
                                forecast_step = None

                        base_dt, forecast_time, forecast_time_units = (
                            _compute_time_metadata(msg, valid_dt)
                        )

                        base_record = {
                            "datetime": valid_dt,
                            "base_datetime": base_dt,
                            "level_type": safe_get(msg, "typeOfLevel", None),
                            "level": safe_get(msg, "level", None),
                            "name": safe_get(msg, "shortName", None),
                            "ensemble": safe_get(msg, "perturbationNumber", None),
                            "forecast_step": forecast_step,
                            "forecast_time": forecast_time,
                            "forecast_time_units": forecast_time_units,
                            "edition": safe_get(msg, "edition", None),
                            "centre": safe_get(msg, "centre", None),
                            "data_type": safe_get(msg, "dataType", None),
                            "grid_type": safe_get(msg, "gridType", None),
                            SDC_INCREMENTAL_KEY: to_iso8601(mtime),
                            SDC_FILENAME: filename,
                        }

                        for lat, lon, val in zip(lats, lons, vals):
                            if val is None or (hasattr(val, "mask") and val.mask):
                                continue

                            if self.bboxes:
                                valid_bbox = False
                                for bbox in self.bboxes:
                                    min_lon, min_lat, max_lon, max_lat = bbox
                                    if (
                                        min_lon <= lon <= max_lon
                                        and min_lat <= lat <= max_lat
                                    ):
                                        valid_bbox = True
                                        break  # found one match, keep the record
                                if not valid_bbox:
                                    # skip record
                                    continue

                            rec = dict(base_record)
                            rec["lat"] = float(lat)
                            rec["lon"] = float(lon)
                            rec["value"] = float(val)

                            # drop ignored fields
                            for f in self.ignore_fields:
                                rec.pop(f, None)

                            yield rec

                    # advance bookmark with the latest seen mtime
                    self._increment_stream_state(
                        {SDC_INCREMENTAL_KEY: to_iso8601(mtime)},
                        context=context,
                    )
            except Exception as e:
                self.logger.error(f"Failed to process grib {self.file_path}: {e}")
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

            self._increment_stream_state(
                {SDC_INCREMENTAL_KEY: to_iso8601(mtime)},
                context=context,
            )
