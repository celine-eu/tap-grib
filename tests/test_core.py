from __future__ import annotations

import os
import json
from datetime import datetime
import pytest

from tap_grib.tap import TapGrib


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
                "skip_fields": [],
                "primary_keys": ["latitude", "longitude", "ts"],
                # group_by left unset → pivot all params
            }
        ]
    }
    return TapGrib(config=config, catalog={}, state={})


def test_schema_includes_core_columns(dummy_tap: TapGrib):
    """Schema must contain latitude, longitude, ts at minimum."""
    streams = dummy_tap.discover_streams()
    assert streams, "No streams discovered"
    stream = streams[0]

    schema = stream.schema
    cols = set(schema["properties"].keys())
    assert {"latitude", "longitude", "ts"}.issubset(cols)


def test_records_match_schema(dummy_tap: TapGrib, capsys: pytest.CaptureFixture):
    """Rows must match schema and include correct types for core columns."""
    streams = dummy_tap.discover_streams()
    assert streams, "No streams discovered"
    stream = streams[0]

    rows = list(stream.get_records())
    assert rows, "No rows were emitted – check test.grib contains data"

    schema_columns = set(stream.schema["properties"].keys())

    for row in rows:
        row_keys = set(row.keys())
        assert row_keys <= schema_columns, (
            f"Row columns differ from schema.\n" f"Extra:   {row_keys - schema_columns}"
        )
        # Core column types
        assert isinstance(row["latitude"], (float, int))
        assert isinstance(row["longitude"], (float, int))
        assert isinstance(row["ts"], (datetime, type(None)))

    # Print first two rows
    print("\n--- First two GRIB rows ------------------------------------------------")
    for i, r in enumerate(rows[:2], start=1):
        print(f"Row {i}:")
        print(json.dumps(r, indent=2, default=str))
    print("--- End of sample output ------------------------------------------------")

    captured = capsys.readouterr()
    assert "Row 1:" in captured.out
