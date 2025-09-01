from __future__ import annotations
import os
import json
from datetime import datetime
import pytest
from tap_grib.tap import TapGrib
import typing as t


@pytest.fixture
def repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def sample_file(repo_root: str) -> str:
    return os.path.join(repo_root, "data", "test.grib")


@pytest.fixture
def dummy_tap(sample_file: str) -> TapGrib:
    """Return a minimal TapGrib with one GRIB file configured."""
    config = {
        "paths": [
            {
                "path": sample_file,
                # demonstrate ignoring optional metadata fields
                "ignore_fields": ["centre", "grid_type"],
            }
        ]
    }
    return TapGrib(config=config, catalog={}, state={})


def test_schema_includes_core_columns(dummy_tap: TapGrib):
    """Schema must contain datetime, lat, lon, name, value at minimum."""
    streams = dummy_tap.discover_streams()
    assert streams, "No streams discovered"
    stream = streams[0]

    schema = stream.schema
    cols = set(schema["properties"].keys())
    assert {"datetime", "lat", "lon", "name", "value"}.issubset(cols)


def test_records_match_schema(dummy_tap: TapGrib, capsys: pytest.CaptureFixture):
    """Rows must match schema and include correct types for core columns."""
    streams = dummy_tap.discover_streams()
    assert streams, "No streams discovered"
    stream = streams[0]

    rows = list(stream.get_records(None))
    assert rows, "No rows were emitted – check test.grib contains data"

    schema_columns = set(stream.schema["properties"].keys())

    for raw_row in rows:
        row: dict[str, t.Any] | None = None
        if isinstance(raw_row, dict):
            row = dict(raw_row)  # already dict
        else:
            data, _ = raw_row  # unpack tuple
            row = dict(data)  # only dict(data) here

        assert row is not None

        row_keys = set(row.keys())
        assert row_keys <= schema_columns, (
            f"Row columns differ from schema.\n" f"Extra:   {row_keys - schema_columns}"
        )
        # Core column types
        assert isinstance(row["lat"], (float, int))
        assert isinstance(row["lon"], (float, int))
        assert isinstance(row["datetime"], (datetime, type(None)))
        assert isinstance(row["name"], str)
        assert isinstance(row["value"], (float, int))

    # Print first two rows for debug
    print("\n--- First two GRIB rows ------------------------------------------------")
    for i, r in enumerate(rows[:2], start=1):
        print(f"Row {i}:")
        print(json.dumps(r, indent=2, default=str))
    print("--- End of sample output ------------------------------------------------")

    captured = capsys.readouterr()
    assert "Row 1:" in captured.out


def test_ignore_core_field_raises(sample_file: str):
    """Config that ignores a core field must raise ValueError."""
    config = {
        "paths": [
            {
                "path": sample_file,
                "ignore_fields": ["lat"],  # not allowed
            }
        ]
    }
    with pytest.raises(ValueError):
        TapGrib(config=config, catalog={}, state={}).discover_streams()


def test_table_name_override(sample_file: str):
    config = {
        "paths": [
            {
                "path": sample_file,
                "table_name": "custom_table",
            }
        ]
    }
    tap = TapGrib(config=config, catalog={}, state={})
    streams = tap.discover_streams()
    assert streams[0].name == "custom_table"
