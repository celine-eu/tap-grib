import numpy as np
import pygrib
from datetime import datetime, timezone
from singer_sdk.streams import Stream

import typing as t

import numpy as np


def _extract_grid(msg):
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
        return np.array([float(lat)]), np.array([float(lon)]), np.array([float(val)])

    # Normalize scalars to arrays
    if np.isscalar(vals):
        vals = np.array([float(vals)])
        lat0 = float(lats.flat[0]) if hasattr(lats, "flat") else float(lats)
        lon0 = float(lons.flat[0]) if hasattr(lons, "flat") else float(lons)
        return np.array([lat0]), np.array([lon0]), vals

    return lats.ravel(), lons.ravel(), vals.ravel()


class GribStream(Stream):
    def __init__(
        self,
        *args,
        file_path: str,
        filter_group: dict[str, t.Any],
        skip_fields: set[str] | None = None,
        primary_keys: list[str] | None = None,
        cached_schema: dict | None = None,
        **kwargs,
    ):
        self.file_path = file_path
        self.filter_group = filter_group
        self.skip_fields = skip_fields or set()
        self.primary_keys = primary_keys or ["latitude", "longitude", "ts"]
        self.schema = cached_schema
        super().__init__(*args, **kwargs)

    def schema(self) -> dict:
        if self._schema is None:
            raise RuntimeError(
                f"Schema not passed from Tap for stream {self.name}. "
                "Tap must build schema during discovery."
            )
        return self._schema

    def get_records(self, context=None):
        self.logger.info(f"[{self.name}] Streaming records from {self.file_path}")
        pivot: dict[tuple[float, float, datetime], dict[str, t.Any]] = {}
        batch_size = 5000

        with pygrib.open(self.file_path) as grbs:
            for msg in grbs:
                if not all(
                    getattr(msg, k, None) == v for k, v in self.filter_group.items()
                ):
                    continue

                shortName = msg.shortName
                typeOfLevel = msg.typeOfLevel if hasattr(msg, "typeOfLevel") else None
                level = msg.level if hasattr(msg, "level") else None
                var_name = f"{typeOfLevel}_" if typeOfLevel else ""
                var_name += f"{level}_" if level else ""
                var_name += shortName

                if var_name in self.skip_fields:
                    continue

                lats, lons, vals = _extract_grid(msg)
                if lats.size == 0:
                    continue

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

                for lat, lon, val in zip(lats, lons, vals):
                    if val is None or (hasattr(val, "mask") and val.mask):
                        continue
                    rec_key = (float(lat), float(lon), valid_dt)
                    if rec_key not in pivot:
                        pivot[rec_key] = {
                            "latitude": float(lat),
                            "longitude": float(lon),
                            "ts": valid_dt,
                        }

                    pivot[rec_key][var_name] = float(val)

                if len(pivot) >= batch_size:
                    yield from pivot.values()
                    pivot.clear()

        if pivot:
            yield from pivot.values()
