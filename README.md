# tap-grib

`tap-grib` is a Singer tap for Grib files extractions. It has been tested on [ERA5 datasets](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download) so far but could be extended to more use cases.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from PyPI:

```bash
uv tool install tap-grib
```

Install from GitHub:

```bash
uv tool install git+https://github.com/ORG_NAME/tap-grib.git@main
```

## Capabilities

- `catalog`
- `discover`

## Supported Python Versions

- 3.10
- 3.11
- 3.12
- 3.13
- 3.14


## Configuration

### Accepted Config Options


## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| paths | True | None | List of GRIB file path definitions. |
| stream_maps | False | None | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_maps.__else__ | False | None | Currently, only setting this to `__NULL__` is supported. This will remove all other streams. |
| stream_map_config | False | None | User-defined config values to be used within map expressions. |
| faker_config | False | None | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an additional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False | None | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False | None | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False | None | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False | None | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-grib --about
```

### Examples

```yaml
    # all values in a table
    - path: ./data/test.grib

    - path: ./data/test_1.grib
      # skip column fields
      skip_fields: ["step"]
      # select primary keys
      primary_keys: ["latitude", "longitude", "ts"]

    # creates a table per column
    - path: ./data/reanalysis-era5-single-levels*
      group_by: ["typeOfLevel", "level"]
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-grib` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-grib --version
tap-grib --help
tap-grib --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-grib` CLI interface directly using `uv run`:

```bash
uv run tap-grib --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
uv tool install meltano
# Initialize meltano within this directory
cd tap-grib
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-grib --version

# OR run a test ELT pipeline:
meltano run tap-grib target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
