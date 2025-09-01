"""Tap implementation for GRIB files (TapGrib)."""

from __future__ import annotations
import os
import glob
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from singer_sdk.helpers.capabilities import TapCapabilities, CapabilitiesEnum
import re
from tap_grib.client import GribStream
import typing as t


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
                        description="Custom table name for the stream (default = file basename).",
                    ),
                    th.Property(
                        "ignore_fields",
                        th.ArrayType(th.StringType),
                        required=False,
                        description="List of schema fields to exclude from output.",
                    ),
                )
            ),
            required=True,
            description="List of GRIB file path definitions.",
        ),
    ).to_dict()

    def default_stream_name(self, file_path: str) -> str:
        base = os.path.splitext(os.path.basename(file_path))[0]
        # replace all non-alphanumeric characters with underscore
        safe = re.sub(r"[^0-9a-zA-Z]+", "_", base)
        return safe.strip("_").lower()

    def discover_streams(self) -> list[Stream]:
        streams: list[Stream] = []
        for entry in self.config.get("paths", []):
            path = entry["path"]
            ignore_fields = set(entry.get("ignore_fields", []))
            table_name = entry.get("table_name")

            for file_path in glob.glob(path):
                stream_name = table_name or self.default_stream_name(file_path)

                streams.append(
                    GribStream(
                        tap=self,
                        name=stream_name,
                        file_path=file_path,
                        primary_keys=["datetime", "lat", "lon", "variable"],
                        ignore_fields=ignore_fields,
                    )
                )
        return streams
