This tool processes RSV Forecasting Hub model outputs and generates consolidated JSON-LD metadata files for each forecasting round.

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
- Generate consolidated JSON-LD files named `round_<ROUND_ID>_v<SCHEMA_VERSION>.jsonld`
  in the `output` directory (e.g. `output/round_2025-07-27_v6.0.0.jsonld`)

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
