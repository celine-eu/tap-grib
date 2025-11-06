"""Tap implementation for GRIB files (TapGrib)."""

from __future__ import annotations
import os
import re
import typing as t
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from singer_sdk.helpers.capabilities import TapCapabilities, CapabilitiesEnum
from tap_grib.client import GribStream
from tap_grib.storage import Storage


class TapGrib(Tap):
    """Singer tap that extracts data from GRIB files."""

    name = "tap-grib"

    capabilities: t.ClassVar[list[CapabilitiesEnum]] = [
        TapCapabilities.CATALOG,
        TapCapabilities.DISCOVER,
    ]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "paths",
            th.ArrayType(
                th.ObjectType(
                    th.Property("path", th.StringType, required=True),
                    th.Property(
                        "table_name",
                        th.StringType,
                        required=False,
                        description="Custom table name for the stream (default = pattern basename).",
                    ),
                    th.Property(
                        "ignore_fields",
                        th.ArrayType(th.StringType),
                        required=False,
                        description="List of schema fields to exclude from output.",
                    ),
                    th.Property(
                        "bbox",
                        th.ArrayType(th.NumberType()),
                        required=False,
                        description="Optional geographic bounding box [min_lon, min_lat, max_lon, max_lat]. "
                        "Records outside this range will be skipped.",
                    ),
                )
            ),
            required=True,
            description="List of GRIB file path definitions (supports globs).",
        ),
    ).to_dict()

    def _parse_bbox(self, bbox) -> tuple[float, float, float, float] | None:
        """Parse and validate bbox in north, west, south, east order."""
        if not bbox:
            return None

        if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
            self.logger.warning(
                "Ignoring invalid bbox: must be [north, west, south, east]"
            )
            return None

        north, west, south, east = bbox
        try:
            north, west, south, east = map(float, (north, west, south, east))
        except Exception:
            self.logger.warning("Ignoring invalid bbox: all values must be numeric")
            return None

        # Validate coordinate ranges
        if not (
            -90 <= south <= 90
            and -90 <= north <= 90
            and -180 <= west <= 180
            and -180 <= east <= 180
        ):
            self.logger.warning("Ignoring invalid bbox: coordinates out of range")
            return None
        if south >= north:
            self.logger.warning("Ignoring invalid bbox: south must be < north")
            return None
        if west >= east:
            self.logger.warning("Ignoring invalid bbox: west must be < east")
            return None

        # Convert to internal form (min_lon, min_lat, max_lon, max_lat)
        min_lon, min_lat, max_lon, max_lat = west, south, east, north
        return min_lon, min_lat, max_lon, max_lat

    def default_stream_name(self, pattern: str) -> str:
        base = os.path.splitext(os.path.basename(pattern))[0]
        safe = re.sub(r"[^0-9a-zA-Z]+", "_", base)
        return safe.strip("_").lower()

    def discover_streams(self) -> list[Stream]:
        """Discover a single stream per path pattern (merging all matching files)."""
        streams: list[Stream] = []

        for entry in self.config.get("paths", []):
            pattern = entry["path"]
            ignore_fields = set(entry.get("ignore_fields", []))
            table_name = entry.get("table_name")
            bbox = self._parse_bbox(entry.get("bbox"))

            storage = Storage(pattern)
            file_list = list(storage.glob())
            if not file_list:
                self.logger.warning(f"No files found for pattern: {pattern}")
                continue

            stream_name = table_name or self.default_stream_name(pattern)
            self.logger.info(
                f"Creating stream '{stream_name}' for {len(file_list)} files under pattern {pattern}"
            )
            if bbox:
                min_lon, min_lat, max_lon, max_lat = bbox
                self.logger.info(
                    f"bbox filter min_lon={min_lon}, min_lat={min_lat}, max_lon={max_lon}, max_lat={max_lat}"
                )

            streams.append(
                GribStream(
                    tap=self,
                    name=stream_name,
                    file_path=None,
                    primary_keys=["datetime", "lat", "lon", "variable"],
                    ignore_fields=ignore_fields,
                    extra_files=file_list,
                    bbox=bbox,
                )
            )

        return streams
