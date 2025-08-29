"""Tap implementation for GRIB files (TapGrib)."""

from __future__ import annotations
import pygrib
from datetime import datetime
import json
import os
import glob
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import TapCapabilities

from tap_grib.client import GribStream


def _sample_value(msg):
    """Extract a safe sample value from a GRIB message for schema inference."""
    try:
        vals = getattr(msg, "values", None)
        if vals is not None:
            if hasattr(vals, "ravel"):  # numpy array or masked array
                if vals.size > 0:
                    return vals.ravel()[0].item()  # convert to Python scalar
            else:
                # Already a scalar (float, int)
                return vals
    except Exception:
        pass

    for attr in ("value", "data"):
        try:
            v = getattr(msg, attr, None)
            if v is not None:
                return v
        except Exception:
            continue

    return None


def _infer_type_from_sample(values) -> th.JSONTypeHelper:
    from singer_sdk import typing as th

    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return th.BooleanType()
        if isinstance(v, int):
            return th.IntegerType()
        if isinstance(v, float):
            return th.NumberType()
        if isinstance(v, datetime):
            return th.DateTimeType()
        return th.StringType()
    return th.StringType(nullable=True)


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
                        "skip_fields",
                        th.ArrayType(th.StringType),
                        required=False,
                        description="Fields to exclude from schema and records.",
                    ),
                    th.Property(
                        "primary_keys",
                        th.ArrayType(th.StringType),
                        required=False,
                        description="Fields to use as Singer primary keys.",
                    ),
                    th.Property(
                        "datasets_name",
                        th.ArrayType(th.StringType),
                        required=False,
                        description="Custom names for datasets within this file (0-based index).",
                    ),
                    th.Property(
                        "group_by",
                        th.ArrayType(th.StringType),
                        required=False,
                        description="GRIB message attributes to group by when creating streams.",
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

    def _load_paths(self) -> list[dict]:
        """Return list of file definition objects with path + settings."""
        paths_cfg = self.config.get("paths")
        external = self.config.get("paths_file")

        if external:
            self.logger.info(f"Loading path definitions from {external}")
            with open(external, "r", encoding="utf-8") as f:
                paths_cfg = json.load(f)

        if not paths_cfg:
            self.logger.error("No GRIB path definitions provided.")
            raise SystemExit(1)

        return paths_cfg

    def discover_streams(self) -> list[Stream]:
        streams: list[Stream] = []

        for entry in self._load_paths():
            path = entry["path"]
            skip_fields = set(entry.get("skip_fields", []))
            primary_keys = entry.get("primary_keys") or [
                "latitude",
                "longitude",
                "ts",
            ]

            group_by = entry.get("group_by")
            if not group_by:
                # Default: single stream for the entire file
                group_by = []

            custom_names = entry.get("datasets_name", [])

            for file_path in glob.glob(path):
                self.logger.info(
                    f"[tap-grib] Scanning {file_path} for schema and groups"
                )

                groups: dict[tuple, dict] = {}
                seen: set[str] = set()
                props: list[th.Property] = [
                    th.Property("latitude", th.NumberType()),
                    th.Property("longitude", th.NumberType()),
                    th.Property("ts", th.DateTimeType()),
                ]

                with pygrib.open(file_path) as grbs:
                    for msg in grbs:
                        # build grouping
                        key = tuple(str(getattr(msg, k, None)) for k in group_by)
                        if key not in groups:
                            groups[key] = {k: getattr(msg, k, None) for k in group_by}

                        # collect variable metadata
                        shortName = msg.shortName
                        typeOfLevel = (
                            msg.typeOfLevel if hasattr(msg, "typeOfLevel") else None
                        )
                        level = msg.level if hasattr(msg, "level") else None
                        var_name = f"{typeOfLevel}_" if typeOfLevel else ""
                        var_name += f"{level}_" if level else ""
                        var_name += shortName

                        if var_name in skip_fields or var_name in seen:
                            continue
                        val = _sample_value(msg)
                        props.append(
                            th.Property(var_name, _infer_type_from_sample([val]))
                        )
                        seen.add(var_name)

                base_schema = th.PropertiesList(*props).to_dict()

                for i, (key, filter_group) in enumerate(groups.items()):
                    base = os.path.splitext(os.path.basename(file_path))[0]
                    if i < len(custom_names):
                        name = custom_names[i]
                    else:
                        suffix = "_".join(
                            str(v) for v in filter_group.values() if v is not None
                        )
                        name = f"{base}_{suffix}" if suffix else f"{base}_{i}"

                    streams.append(
                        GribStream(
                            tap=self,
                            name=name,
                            file_path=file_path,
                            filter_group=filter_group,
                            skip_fields=skip_fields,
                            primary_keys=primary_keys,
                            cached_schema=base_schema,  # <-- pass schema
                        )
                    )
                    self.logger.info(
                        f"[tap-grib] Created stream {name} filter={filter_group}"
                    )

        return streams


if __name__ == "__main__":
    TapGrib.cli()
