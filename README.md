This tool processes RSV Forecasting Hub model outputs and generates consolidated JSON-LD metadata files for each forecasting round.

## Installation

```bash
git clone https://github.com/midas-network/smh-to-jsonld.git
cd smh-to-jsonld
pip install -r requirements.txt
```

## Usage

### Quick Start: Run Complete Pipeline

The easiest way to run the entire pipeline is with a single command:

```bash
# Run all steps: update data, create JSON-LD, generate HTML
python run_pipeline.py

# Skip data update and use existing data
python run_pipeline.py --skip-update

# Process specific rounds only
python run_pipeline.py --rounds 2024-07-28 2023-11-12

# Stop on first error
python run_pipeline.py --stop-on-error
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
python pipeline/update_source_data.py
```

This will download the necessary model outputs and metadata from the RSV Forecasting Hub repository.

### 2. Generate JSON-LD Files

To process the downloaded data and create consolidated JSON-LD files:

```bash
python pipeline/create_jsonld.py
```

Options:
- Use `--output` to specify a custom output directory

The script will:
- Process all model outputs
- Extract metadata including age groups, locations, and output types
- Generate consolidated JSON-LD files in the `output` directory

### 3. Generate HTML Visualization

To convert JSON-LD files to HTML for easy viewing:

```bash
# Convert with default settings (round_2024-07-28.jsonld)
python pipeline/jsonld_to_html.py

# Convert a specific round
python pipeline/jsonld_to_html.py -i output/round_2023-11-12.jsonld -o output/round_2023-11-12.html -r 2023-11-12

# Custom input/output paths
python pipeline/jsonld_to_html.py --input path/to/input.jsonld --output path/to/output.html --round-id 2024-07-28
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
pytest test_pipeline.py

# Skip data update (use existing data)
pytest test_pipeline.py --skip-update

# Run only specific tests
pytest test_pipeline.py::TestCreateJsonLD

# Run with verbose output
pytest test_pipeline.py -v

# Generate HTML test report
pytest test_pipeline.py --html=report.html --self-contained-html

# Run tests in verbose mode with detailed output
pytest test_pipeline.py --skip-update -v -s
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
