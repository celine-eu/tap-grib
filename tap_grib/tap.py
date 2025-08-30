"""Tap implementation for GRIB files (TapGrib)."""

from __future__ import annotations
import os
import glob
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import TapCapabilities

from tap_grib.client import GribStream


class TapGrib(Tap):
    """Singer tap that extracts data from GRIB files."""

    name = "tap-grib"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "paths",
            th.ArrayType(
                th.ObjectType(
                    th.Property("path", th.StringType, required=True),
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

    @classproperty
    def capabilities(cls) -> list[TapCapabilities]:
        return [TapCapabilities.CATALOG, TapCapabilities.DISCOVER]

    def discover_streams(self) -> list[Stream]:
        streams: list[Stream] = []
        for entry in self.config.get("paths", []):
            path = entry["path"]
            ignore_fields = set(entry.get("ignore_fields", []))
            for file_path in glob.glob(path):
                name = os.path.splitext(os.path.basename(file_path))[0]
                streams.append(
                    GribStream(
                        tap=self,
                        name=name,
                        file_path=file_path,
                        primary_keys=["datetime", "lat", "lon", "name"],
                        ignore_fields=ignore_fields,
                    )
                )
        return streams
