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
                        "bboxes",
                        th.ArrayType(th.ArrayType(th.NumberType())),
                        required=False,
                        description="Optional list of geographic bounding box [[min_lon, min_lat, max_lon, max_lat]]. "
                        "Records outside this range will be skipped.",
                    ),
                    th.Property(
                        "skip_past",
                        th.BooleanType(),
                        required=False,
                        description="Optional skip forecast that are past now",
                    ),
                    th.Property(
                        "skip_past_reference",
                        th.DateTimeType(),
                        required=False,
                        description="Optional reference date for skip past",
                    ),
                )
            ),
            required=True,
            description="List of GRIB file path definitions (supports globs).",
        ),
    ).to_dict()

    def _parse_bboxes(
        self, bboxes: list[tuple[float, float, float, float]]
    ) -> list[tuple[float, float, float, float]] | None:
        """Parse and validate bbox in north, west, south, east order."""
        if not bboxes:
            return None

        valid_bboxes: list[tuple[float, float, float, float]] = []
        for bbox in bboxes:
            if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
                self.logger.warning(
                    "Ignoring invalid bbox: must be [north, west, south, east]"
                )
                continue

            north, west, south, east = bbox
            try:
                north, west, south, east = map(float, (north, west, south, east))
            except Exception:
                self.logger.warning("Ignoring invalid bbox: all values must be numeric")
                continue

            # Validate coordinate ranges
            if not (
                -90 <= south <= 90
                and -90 <= north <= 90
                and -180 <= west <= 180
                and -180 <= east <= 180
            ):
                self.logger.warning("Ignoring invalid bbox: coordinates out of range")
                continue
            if south >= north:
                self.logger.warning("Ignoring invalid bbox: south must be < north")
                continue
            if west >= east:
                self.logger.warning("Ignoring invalid bbox: west must be < east")
                continue

            # Convert to internal form (min_lon, min_lat, max_lon, max_lat)
            min_lon, min_lat, max_lon, max_lat = west, south, east, north

            valid_bboxes.append((min_lon, min_lat, max_lon, max_lat))
        return valid_bboxes

    def default_stream_name(self, pattern: str) -> str:
        base = os.path.splitext(os.path.basename(pattern))[0]

        # sanitize
        safe = re.sub(r"[^0-9a-zA-Z]+", "_", base).strip("_").lower()

        # Fallback if empty
        if not safe:
            safe = "grib_stream"

        return safe

    def discover_streams(self) -> list[Stream]:
        """Discover a single stream per path pattern (merging all matching files)."""
        streams: list[Stream] = []

        for entry in self.config.get("paths", []):
            pattern = entry["path"]
            ignore_fields = set(entry.get("ignore_fields", []))
            table_name = entry.get("table_name")
            bboxes = self._parse_bboxes(entry.get("bboxes"))

            skip_past = entry.get("skip_past", False)
            skip_past_reference = entry.get("skip_past_reference", None)

            storage = Storage(pattern)
            file_list = list(storage.glob())
            if not file_list:
                self.logger.warning(f"No files found for pattern: {pattern}")
                continue

            stream_name = table_name or self.default_stream_name(pattern)
            self.logger.info(
                f"Creating stream '{stream_name}' for {len(file_list)} files under pattern {pattern}"
            )
            if bboxes:
                for bbox in bboxes:
                    min_lon, min_lat, max_lon, max_lat = bbox
                    self.logger.info(
                        f"bbox filter min_lon={min_lon}, min_lat={min_lat}, max_lon={max_lon}, max_lat={max_lat}"
                    )

            streams.append(
                GribStream(
                    tap=self,
                    name=stream_name,
                    file_path=None,
                    primary_keys=self.config.get("primary_keys", None),
                    ignore_fields=ignore_fields,
                    extra_files=file_list,
                    bboxes=bboxes,
                    skip_past=skip_past,
                    skip_past_reference=skip_past_reference,
                )
            )

        return streams
