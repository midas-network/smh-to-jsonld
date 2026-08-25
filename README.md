This tool processes RSV Scenario Modeling Hub model outputs and generates
consolidated JSON-LD metadata files for each scenario round.

The current beta release is
[`v0.1.0-beta.1`](https://github.com/midas-network/smh-to-jsonld/releases/tag/v0.1.0-beta.1).

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/midas-network/smh-to-jsonld.git
cd smh-to-jsonld
uv sync
```

This creates a `.venv` virtual environment and installs all dependencies from `uv.lock`. To run any script, prefix it with `uv run`:

```bash
uv run python run_pipeline.py
```

## Usage

### Quick Start: Run Complete Pipeline

The easiest way to run the entire pipeline is with a single command:

```bash
# Run all steps: update data, create JSON-LD, generate HTML
uv run python run_pipeline.py

# Skip data update and use existing data
uv run python run_pipeline.py --skip-update

# Process specific rounds only
uv run python run_pipeline.py --rounds 2024-07-28 2023-11-12

# Stop on first error
uv run python run_pipeline.py --stop-on-error
```

Options:
- `--skip-update`: Skip updating source data (use existing data)
- `--skip-jsonld`: Skip JSON-LD creation
- `--skip-html`: Skip HTML generation
- `--rounds ROUND_ID [ROUND_ID ...]`: Process only specific round IDs
- `--stop-on-error`: Stop pipeline execution on first error
- `--verbose`: Show verbose output

### Individual Pipeline Steps

You can also run each step of the pipeline individually:

### 1. Download Source Data

First, download the latest model output data:

```bash
uv run python pipeline/update_source_data.py
```

This will download the necessary model outputs and metadata from the RSV Forecasting Hub repository.

### 2. Generate JSON-LD Files

Each round is processed with the script that matches its Hubverse tasks-schema version.
Use `run_pipeline.py` to dispatch automatically:

```bash
uv run python run_pipeline.py
```

Or run a specific round manually:

```bash
# Hubverse schema v6.0.0 rounds (e.g. 2025-07-27)
uv run python pipeline/create_jsonld_v6_0_0.py --round_dir data/2025-07-27

# Hubverse schema v5.1.0 rounds (e.g. 2024-07-28, 2023-11-12)
uv run python pipeline/create_jsonld_v5_1_0.py --round_dir data/2024-07-28
uv run python pipeline/create_jsonld_v5_1_0.py --round_dir data/2023-11-12

# Process all v5.1.0 rounds at once
uv run python pipeline/create_jsonld_v5_1_0.py
```

Options:
- `--round_dir`: Path to a single round directory (e.g. `data/2025-07-27`)
- `--output`: Custom output directory (default: `output`)

The scripts will:
- Process all model outputs for the round
- Extract metadata including age groups, locations, and output types
- Generate consolidated JSON-LD files in the `output` directory. When
  `additional_metadata.round_name` is present, spaces are replaced with
  underscores (e.g. `output/Round_1_-_2025-2026_v6.0.0.jsonld`). Otherwise,
  filenames fall back to `round_<ROUND_ID>_v<SCHEMA_VERSION>.jsonld`.
- Add scenario/round definition links when `additional_metadata.internal_round_name`
  is present. These point to the hub repository markdown file, e.g.
  `https://github.com/midas-network/rsv-scenario-modeling-hub/blob/main/auxiliary-data/rounds/round3.md`.

### 3. Generate HTML Visualization

To convert JSON-LD files to HTML for easy viewing:

```bash
# Convert a specific v6.0.0 round
uv run python pipeline/jsonld_to_html.py -i output/round_2025-07-27_v6.0.0.jsonld -o output/round_2025-07-27_v6.0.0.html -r 2025-07-27

# Convert a specific v5.1.0 round
uv run python pipeline/jsonld_to_html.py -i output/round_2024-07-28_v5.1.0.jsonld -o output/round_2024-07-28_v5.1.0.html -r 2024-07-28
```

Options:
- `-i, --input`: Input JSON-LD file path (default: output/round_2024-07-28.jsonld)
- `-o, --output`: Output HTML file path (default: output/round_2024-07-28.html)
- `-r, --round-id`: Round identifier for loading sample data (default: 2024-07-28)
- `--no-sample-data`: Skip loading sample output data from parquet files

### 4. Run Complete Test Suite

To test the entire pipeline using pytest:

```bash
# Run all tests
uv run pytest test_pipeline.py

# Skip data update (use existing data)
uv run pytest test_pipeline.py --skip-update

# Run only specific tests
uv run pytest test_pipeline.py::TestCreateJsonLD

# Run with verbose output
uv run pytest test_pipeline.py -v

# Generate HTML test report
uv run pytest test_pipeline.py --html=report.html --self-contained-html

# Run tests in verbose mode with detailed output
uv run pytest test_pipeline.py --skip-update -v -s
```

Options:
- `--skip-update`: Skip updating source data (assumes data already exists)
- `-v, --verbose`: Increase verbosity
- `-s`: Show print statements (don't capture output)
- `-k EXPRESSION`: Only run tests matching the given expression
- `--html=FILE`: Generate an HTML test report
- `--self-contained-html`: Create a self-contained HTML report

## Output

The generated JSON-LD files contain comprehensive metadata about each forecasting round, including:
- Model descriptions and metadata
- Spatial coverage (US states and territories)
- Age group breakdowns
- Output types and targets
- Temporal coverage

Each round's data is saved as `round_[ROUND_ID].jsonld` in the output directory.

## Publish to Zenodo Sandbox

`publish_to_zenodo.py` creates one self-contained archive per round containing
its source Parquet/configuration data and generated JSON-LD/HTML. It also creates
a top-level dataset `README.md`, `LICENSES.json`, SHA-256 checksums, and
provenance metadata, then uploads them through the Zenodo deposition API. It
creates an unpublished draft unless `--publish` is explicitly supplied.

Zenodo metadata is derived from the consolidated JSON-LD: the Scenario Modeling
Hub Coordination Group is the creator, model authors and teams are deduplicated
contributors, projection intervals become dates, measured-variable ontology
terms become subjects, and funding statements, methods, keywords, and round
documentation links are retained. Unknown model licenses default to
`zenodo-freetoread-1.0` and are recorded in `LICENSES.json`.

Build the release locally without contacting Zenodo:

```bash
uv run publish_to_zenodo.py --dry-run
```

Create an unpublished Sandbox draft:

```bash
uv run --env-file .env publish_to_zenodo.py \
  --reuse-archives
```

Review the returned draft URL and its metadata. Model outputs have
model-specific licenses, so publication additionally requires an explicit
rights-review acknowledgement. Publish that same reviewed draft by ID:

```bash
uv run --env-file .env publish_to_zenodo.py \
  --publish-draft YOUR_DEPOSITION_ID \
  --acknowledge-rights-reviewed
```

For automation that should create, upload, and publish in one run, use
`--publish --acknowledge-rights-reviewed` instead.

Synchronize regenerated files and metadata into an existing unpublished draft:

```bash
uv run --env-file .env publish_to_zenodo.py \
  --reuse-archives \
  --update-draft YOUR_DEPOSITION_ID
```

On production Zenodo, `--community midas-network` requests inclusion in the
MIDAS Network community. Sandbox has a separate community registry, so that
production community should not be added to a Sandbox draft.

Use `--metadata-file metadata.json` for complete creator, affiliation, ORCID,
license, funding, and related-identifier metadata. Use `--help` for round,
packaging, and Sandbox API options. Tokens are sent as HTTPS bearer headers and
are never written into the release files.

## License

The `smh-to-jsonld` software in this repository is licensed under the
[Apache License 2.0](LICENSE). This software license does not relicense the
Scenario Modeling Hub model outputs, metadata, or auxiliary data. Those
research objects retain their model- and source-specific terms; Zenodo release
packages record them in `LICENSES.json`.
