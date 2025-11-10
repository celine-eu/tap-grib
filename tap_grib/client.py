import numpy as np
from datetime import datetime, timezone
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


def safe_get(msg, key, default=None):
    try:
        return getattr(msg, key)
    except (AttributeError, RuntimeError):
        return default


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


class GribStream(Stream):
    """Stream that reads records from a GRIB file in normalized (long) format."""

    CORE_FIELDS = {"datetime", "lat", "lon", "name", "value"}

    def __init__(
        self,
        tap,
        name: str,
        *,
        file_path: str | None,
        primary_keys: list[str] | None = None,
        ignore_fields: set[str] | None = None,
        extra_files: list[str] | None = None,
        bboxes: list[tuple[float, float, float, float]] | None = None,
        **kwargs,
    ):
        self.file_path = file_path
        self.extra_files = extra_files or ([file_path] if file_path else [])
        self.primary_keys = primary_keys or ["datetime", "lat", "lon", "name"]
        self.bboxes = bboxes

        ignore_fields = ignore_fields or set()
        invalid = ignore_fields & self.CORE_FIELDS
        if invalid:
            raise ValueError(f"Cannot ignore core fields: {', '.join(sorted(invalid))}")
        self.ignore_fields = ignore_fields

        # now call parent init with only tap/name/kwargs
        super().__init__(tap=tap, name=name, **kwargs)

        self.state_partitioning_keys = [SDC_FILENAME]
        self.replication_key = SDC_INCREMENTAL_KEY
        self.forced_replication_method = "INCREMENTAL"

    # --------------------------
    # Schema
    # --------------------------
    @property
    def schema(self) -> dict:
        props: t.List[th.Property] = [
            th.Property("datetime", th.DateTimeType()),
            th.Property("lat", th.NumberType()),
            th.Property("lon", th.NumberType()),
            th.Property("level_type", th.StringType(nullable=True)),
            th.Property("level", th.IntegerType(nullable=True)),
            th.Property("name", th.StringType()),
            th.Property("value", th.NumberType()),
            th.Property("ensemble", th.IntegerType(nullable=True)),
            th.Property("forecast_step", th.NumberType(nullable=True)),
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
            partition_context = {SDC_FILENAME: filename}
            last_bookmark = self.get_starting_replication_key_value(partition_context)

            bookmark_dt: datetime | None = None
            if last_bookmark:
                bookmark_dt = datetime.fromisoformat(last_bookmark)
                if bookmark_dt.tzinfo is None:
                    bookmark_dt = bookmark_dt.replace(tzinfo=timezone.utc)
                else:
                    bookmark_dt = bookmark_dt.astimezone(timezone.utc)

                # skip file entirely if mtime <= bookmark

                self.logger.info(
                    "%s (mtime=%s VS bookmark=%s)",
                    self.file_path,
                    mtime,
                    bookmark_dt,
                )

            if bookmark_dt and mtime <= bookmark_dt:
                self.logger.info(
                    "Skipping %s (mtime=%s <= bookmark=%s)",
                    self.file_path,
                    mtime,
                    bookmark_dt,
                )
                return

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

                        base_record = {
                            "datetime": valid_dt,
                            "level_type": safe_get(msg, "typeOfLevel", None),
                            "level": safe_get(msg, "level", None),
                            "name": safe_get(msg, "shortName", None),
                            "ensemble": safe_get(msg, "perturbationNumber", None),
                            "forecast_step": forecast_step,
                            "edition": safe_get(msg, "edition", None),
                            "centre": safe_get(msg, "centre", None),
                            "data_type": safe_get(msg, "dataType", None),
                            "grid_type": safe_get(msg, "gridType", None),
                            SDC_INCREMENTAL_KEY: mtime,
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
                        {SDC_INCREMENTAL_KEY: mtime.isoformat()},
                        context=partition_context,
                    )
            except Exception as e:
                self.logger.error(f"Failed to process grib {self.file_path}: {e}")
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
