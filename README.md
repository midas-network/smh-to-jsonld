# RSV Forecasting Hub Parser

This tool processes RSV Forecasting Hub model outputs and generates consolidated JSON-LD metadata files for each forecasting round.

## Installation

```bash
git clone https://github.com/midas-network/smh-to-jsonld.git
cd smh-to-jsonld
pip install -r requirements.txt
```

## Usage

### 1. Download Source Data

First, download the latest model output data:

```bash
python update_source_Data.py
```

This will download the necessary model outputs and metadata from the RSV Forecasting Hub repository.

### 2. Generate JSON-LD Files

To process the downloaded data and create consolidated JSON-LD files:

```bash
python create_jsonld.py
```

Options:
- Use `--output` to specify a custom output directory

The script will:
- Process all model outputs
- Extract metadata including age groups, locations, and output types
- Generate consolidated JSON-LD files in the `output` directory

## Output

The generated JSON-LD files contain comprehensive metadata about each forecasting round, including:
- Model descriptions and metadata
- Spatial coverage (US states and territories)
- Age group breakdowns
- Output types and targets
- Temporal coverage

Each round's data is saved as `round_[ROUND_ID].jsonld` in the output directory.
