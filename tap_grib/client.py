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

# GRIB2 instantaneous PDTs (per your original intent)
INSTANTANEOUS_PDTS = {0, 1, 2, 3}


def parse_bookmark(val: str | None) -> datetime | None:
    if not val:
        return None
    clean = val.replace("Z", "+00:00")
    return datetime.fromisoformat(clean).astimezone(timezone.utc)


def to_iso8601(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def safe_get(msg, key, default=None):
    try:
        return getattr(msg, key)
    except Exception:
        return default


def _normalize_dt(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# GRIB stepUnits mapping (GRIB2 Code Table 4.4)
# Keep minimal set; extend if you meet more codes.
def step_to_timedelta(value: float, unit_code: int) -> timedelta | None:
    # 0 minutes, 1 hours, 2 days, 13 seconds
    if unit_code == 0:
        return timedelta(minutes=value)
    if unit_code == 1:
        return timedelta(hours=value)
    if unit_code == 2:
        return timedelta(days=value)
    if unit_code == 13:
        return timedelta(seconds=value)
    return None


def _extract_grid(msg: t.Any):
    """Return (lats, lons, vals) as 1-D numpy arrays for any GRIB message."""
    try:
        lats, lons = msg.latlons()
        vals = msg.values
    except Exception:
        lat = safe_get(msg, "latitude", None)
        lon = safe_get(msg, "longitude", None)
        val = safe_get(msg, "value", None) or safe_get(msg, "data", None)
        if lat is None or lon is None or val is None:
            return np.array([]), np.array([]), np.array([])
        return (
            np.array([float(lat)]),
            np.array([float(lon)]),
            np.array([float(val)]),
        )

    if np.isscalar(vals):
        vals = np.array([float(t.cast(float, vals))])
        lat0 = float(lats.flat[0]) if hasattr(lats, "flat") else float(lats)
        lon0 = float(lons.flat[0]) if hasattr(lons, "flat") else float(lons)
        return np.array([lat0]), np.array([lon0]), vals

    return lats.ravel(), lons.ravel(), vals.ravel()


def _compute_run_datetime(msg: t.Any) -> datetime | None:
    """
    Authoritative run time:
      1) analDate (best)
      2) dataDate/dataTime fallback
    """
    run_dt = safe_get(msg, "analDate", None)
    run_dt = _normalize_dt(run_dt)

    if run_dt is None:
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
            run_dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

    return run_dt


def _compute_interval_semantics(
    msg: t.Any, run_dt: datetime | None
) -> tuple[str | None, int | None, datetime | None, datetime | None]:
    """
    Returns:
      step_range (raw)
      step_units (raw int code)
      interval_start_datetime
      interval_end_datetime

    Prefers stepRange + stepUnits.
    Falls back to validDate as instantaneous if interval can't be parsed.
    """
    step_range = safe_get(msg, "stepRange", None)
    step_units = safe_get(msg, "stepUnits", None)

    interval_start_dt = None
    interval_end_dt = None

    if run_dt is not None and step_range and step_units is not None:
        try:
            # stepRange commonly "0-45" (as str). Sometimes single value "45".
            if isinstance(step_range, str):
                s = step_range.strip()
                if "-" in s:
                    a, b = s.split("-", 1)
                    start_val = float(a)
                    end_val = float(b)
                else:
                    # interpret single value as instantaneous at that lead time
                    start_val = float(s)
                    end_val = float(s)

                td_start = step_to_timedelta(start_val, int(step_units))
                td_end = step_to_timedelta(end_val, int(step_units))
                if td_start is not None and td_end is not None:
                    interval_start_dt = run_dt + td_start
                    interval_end_dt = run_dt + td_end
        except Exception:
            interval_start_dt = None
            interval_end_dt = None

    # Fallback: use validDate as instantaneous interval
    if interval_start_dt is None or interval_end_dt is None:
        valid_dt = safe_get(msg, "validDate", None)
        valid_dt = _normalize_dt(valid_dt)
        if valid_dt is not None:
            interval_start_dt = valid_dt
            interval_end_dt = valid_dt

    return step_range, step_units, interval_start_dt, interval_end_dt


def _compute_forecast_step_hours(msg: t.Any) -> float | None:
    """
    Keep a diagnostic 'forecast_step' similar to original.
    Prefer msg.step if numeric; parse strings like "12h", "15m".
    This is NOT authoritative for accumulated fields (stepRange is).
    """
    raw_step = safe_get(msg, "step", None)
    if isinstance(raw_step, (int, float)):
        return float(raw_step)

    if isinstance(raw_step, str):
        s = raw_step.strip().lower()
        try:
            if s.endswith("h"):
                return float(s[:-1])
            if s.endswith("m"):
                return float(s[:-1]) / 60.0
            if s.endswith("s"):
                return float(s[:-1]) / 3600.0
            return float(s)
        except Exception:
            return None

    return None


class GribStream(Stream):
    """Stream that reads records from a GRIB file in normalized (long) format, with interval semantics."""

    # Updated default PK: interval-aware
    DEFAULT_PKEY = [
        "run_datetime",
        "interval_start_datetime",
        "interval_end_datetime",
        "lat",
        "lon",
        "name",
    ]

    CORE_FIELDS = {
        "run_datetime",
        "interval_start_datetime",
        "interval_end_datetime",
        "lat",
        "lon",
        "name",
        "value",
    }

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
        self.skip_past = bool(skip_past)

        # parse skip_past_reference
        ref_dt: datetime | None = None
        if skip_past_reference:
            try:
                ref_dt = datetime.fromisoformat(skip_past_reference)
                ref_dt = _normalize_dt(ref_dt) or ref_dt.replace(tzinfo=timezone.utc)
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
        return False

    # --------------------------
    # Schema
    # --------------------------
    @property
    def schema(self) -> dict:
        props: t.List[th.Property] = [
            # Authoritative time model
            th.Property("run_datetime", th.DateTimeType()),
            th.Property("interval_start_datetime", th.DateTimeType()),
            th.Property("interval_end_datetime", th.DateTimeType()),
            th.Property("step_range", th.StringType(nullable=True)),
            th.Property("step_units", th.IntegerType(nullable=True)),
            # Diagnostics / metadata
            th.Property("forecast_step_hours", th.NumberType(nullable=True)),
            # Spatial + value
            th.Property("lat", th.NumberType()),
            th.Property("lon", th.NumberType()),
            th.Property("level_type", th.StringType(nullable=True)),
            th.Property("level", th.IntegerType(nullable=True)),
            th.Property("name", th.StringType()),
            th.Property("value", th.NumberType()),
            th.Property("ensemble", th.IntegerType(nullable=True)),
            th.Property("edition", th.IntegerType(nullable=True)),
            th.Property("centre", th.StringType(nullable=True)),
            th.Property("data_type", th.StringType(nullable=True)),
            th.Property("grid_type", th.StringType(nullable=True)),
            # Singer state
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

        props = [p for p in props if p.name not in self.ignore_fields]
        return th.PropertiesList(*props).to_dict()

    # --------------------------
    # Record extraction
    # --------------------------
    def get_records(self, context: t.Mapping[str, t.Any] | None):
        for path in self.extra_files:
            self.logger.info(f"[{self.name}] Streaming records from {path}")
            storage = Storage(path)
            info = storage.describe(path)
            mtime = info.mtime
            filename = info.path

            last_bookmark = self.get_starting_replication_key_value(context)
            bookmark_dt = parse_bookmark(last_bookmark)

            self.logger.debug(
                "Partition context: %s, last_bookmark=%s, mtime=%s",
                context,
                bookmark_dt,
                mtime,
            )

            # Skip whole file if already processed
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
                    raise Exception("temporary file path (tmp_path) is not available")

                with pygrib.open(tmp_path) as grbs:  # type: ignore[attr-defined]

                    # Cutoff once per file
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

                        # Compute run time + interval semantics
                        run_dt = _compute_run_datetime(msg)
                        if run_dt is None:
                            continue

                        step_range, step_units, interval_start_dt, interval_end_dt = (
                            _compute_interval_semantics(msg, run_dt)
                        )
                        if interval_start_dt is None or interval_end_dt is None:
                            continue

                        # Past-date filtering: keep original intent:
                        # filter ONLY instantaneous messages that are in the past.
                        if cutoff is not None:
                            pdt = safe_get(msg, "productDefinitionTemplateNumber")
                            is_instantaneous = (
                                (pdt in INSTANTANEOUS_PDTS)
                                if pdt is not None
                                else False
                            )
                            if is_instantaneous and interval_end_dt < cutoff:
                                continue

                        forecast_step_hours = _compute_forecast_step_hours(msg)

                        base_record = {
                            "run_datetime": run_dt,
                            "interval_start_datetime": interval_start_dt,
                            "interval_end_datetime": interval_end_dt,
                            "step_range": step_range,
                            "step_units": step_units,
                            "forecast_step_hours": forecast_step_hours,
                            "level_type": safe_get(msg, "typeOfLevel", None),
                            "level": safe_get(msg, "level", None),
                            "name": safe_get(msg, "shortName", None),
                            "ensemble": safe_get(msg, "perturbationNumber", None),
                            "edition": safe_get(msg, "edition", None),
                            "centre": safe_get(msg, "centre", None),
                            "data_type": safe_get(msg, "dataType", None),
                            "grid_type": safe_get(msg, "gridType", None),
                            SDC_INCREMENTAL_KEY: to_iso8601(mtime),
                            SDC_FILENAME: filename,
                        }

                        # Drop ignored fields at base_record-level too
                        for f in self.ignore_fields:
                            base_record.pop(f, None)

                        for lat, lon, val in zip(lats, lons, vals):
                            if val is None or (hasattr(val, "mask") and val.mask):
                                continue

                            if self.bboxes:
                                valid_bbox = False
                                for bbox in self.bboxes:
                                    min_lon, min_lat, max_lon, max_lat = bbox
                                    if (min_lon <= lon <= max_lon) and (
                                        min_lat <= lat <= max_lat
                                    ):
                                        valid_bbox = True
                                        break
                                if not valid_bbox:
                                    continue

                            rec = dict(base_record)
                            rec["lat"] = float(lat)
                            rec["lon"] = float(lon)
                            rec["value"] = float(val)

                            # Drop ignored fields at record-level too (safety)
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
                    if tmp_path:
                        os.remove(tmp_path)
                except Exception:
                    pass

            # ensure state advanced even if errors happened
            self._increment_stream_state(
                {SDC_INCREMENTAL_KEY: to_iso8601(mtime)},
                context=context,
            )
