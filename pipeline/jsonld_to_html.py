import argparse
import csv
import json
import os
import sys
from pathlib import Path

# Add parent directory to path to allow imports and access to data/output directories
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import pyarrow.dataset as ds


def get_first_n_rows_of_output(n, round_id, model):
    model_dir = Path("data") / round_id / "model-output" / model
    parquet_files = sorted(model_dir.glob("*.parquet"))

    if not parquet_files:
        return ""

    pa_table = ds.dataset([str(path) for path in parquet_files], format="parquet").to_table()
    df = pa_table.to_pandas()

    # Be robust to mixed files by keeping only rows for this model when available.
    if "model_id" in df.columns:
        df = df[df["model_id"] == model]

    if df.empty:
        return ""

    # Get the first `n` rows and the last row
    if len(df) > n:
        df = pd.concat([df.head(n), pd.DataFrame([["..."] * len(df.columns)], columns=df.columns), df.tail(1)])

    html_output = df.to_html(index=False, border=0)
    return html_output


def load_geodata_mapping():
    """Load geodata CSV and create ISO code to Geonames URL mapping."""
    mapping = {}
    geodata_file = Path('geodata/geodata.csv')

    if geodata_file.exists():
        with open(geodata_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                iso_code = row['iso3166_2']
                geoname_url = row['geoname_url']
                mapping[iso_code] = geoname_url

    return mapping


def get_license_map():
    """Return mapping of license names to their URLs."""
    return {
        # Open Source / Software
        "MIT":          "https://opensource.org/license/mit/",
        "GPL-3.0":      "https://www.gnu.org/licenses/gpl-3.0.en.html",
        "APACHE-2.0":   "https://www.apache.org/licenses/LICENSE-2.0",
        "BSD SIMPLIFIED": "https://opensource.org/license/bsd-2-clause",

        # Creative Commons
        "CC0-1.0":      "https://creativecommons.org/publicdomain/zero/1.0/",
        "CC-BY-4.0":    "https://creativecommons.org/licenses/by/4.0/",
        "CC-BY-NC-4.0": "https://creativecommons.org/licenses/by-nc/4.0/",
        "CC BY-NC 4.0": "https://creativecommons.org/licenses/by-nc/4.0/",   # alternate spacing variant
        "CC-BY-SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
        "CC-BY_SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",   # legacy underscore variant

        # Open Data Commons
        "ODC-BY":       "https://opendatacommons.org/licenses/by/",
        "ODBL":         "https://opendatacommons.org/licenses/odbl/",
        "PDDL":         "https://opendatacommons.org/licenses/pddl/",

        # Government / Other
        "OGL-3.0":      "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    }
    

def generate_html_head(title):
    """Generate HTML head section with styles."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        th {{
          text-align: left;
        }}
        body {{
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 5px; }}
        h3 {{ color: #7f8c8d; }}
        .model {{
            background: #ffffff;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
            border-left: 4px solid #3498db;
        }}
        .model-separator {{
            border: 0;
            height: 2px;
            margin: 32px 0;
            width: 100%;
            background: #cfd8e3;
            border-radius: 0;
        }}
        .author {{
            margin: 0px 0;
            padding: 0px 10px;
            background: white;
            border-radius: 0px;
        }}
        .authors-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 15px;
            margin: 10px 0;
        }}
        .location {{
            display: inline-block;
            margin: 5px;
            padding: 0px 5px;
            background: #ffffff;
            border-radius: 3px;
            font-size: 0.9em;
        }}
        .metadata {{
            color: #7f8c8d;
            font-size: 0.9em;
        }}
        .variables-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 15px;
            margin: 10px 0;
        }}
        .no-wrap {{ white-space: nowrap; }}
        a {{ color: #3498db; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .age-groups {{ margin: 10px 0; }}
        .index {{
            background: #fff;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
            border: 2px solid #3498db;
        }}
        .index h2 {{ margin-top: 0; }}
        .index ul {{
            list-style: none;
            padding: 0;
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 10px;
        }}
        .index li {{
            padding: 8px;
            background: #ffffff;
            border-radius: 3px;
        }}
        .index li:hover {{ background: #e8f4f8; }}
        .back-to-top {{
            display: inline-block;
            margin-left: 15px;
            padding: 5px 10px;
            background: #3498db;
            color: white;
            border-radius: 3px;
            font-size: 0.9em;
            vertical-align: middle;
        }}
        .back-to-top:hover {{ background: #2980b9; text-decoration: none; }}
        .model-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 10px;
        }}
        .model-header h2 {{
            margin: 0;
            border-bottom: none;
            padding-bottom: 0;
        }}
        
        /* Tab styles */
        .tabs {{
            margin: 20px 0;
        }}
        .tab-buttons {{
            display: flex;
            gap: 5px;
            border-bottom: 2px solid #3498db;
            margin-bottom: 15px;
        }}
        .tab-button {{
            padding: 10px 20px;
            background: #ecf0f1;
            border: none;
            cursor: pointer;
            font-size: 1em;
            border-radius: 5px 5px 0 0;
            transition: background 0.3s;
        }}
        .tab-button:hover {{
            background: #bdc3c7;
        }}
        .tab-button.active {{
            background: #3498db;
            color: white;
        }}
        .tab-content {{
            display: none;
            padding: 5px 0;
        }}
        .tab-content.active {{
            display: block;
        }}
        table {{
            padding: 0px 10px;
        }}
    </style>
    <script>
        function switchTab(modelIdx, tabName) {{
            const modelId = 'model-' + modelIdx;
            const buttons = document.querySelectorAll('#' + modelId + ' .tab-button');
            const contents = document.querySelectorAll('#' + modelId + ' .tab-content');
            
            buttons.forEach(btn => btn.classList.remove('active'));
            contents.forEach(content => content.classList.remove('active'));
            
            document.getElementById(modelId + '-tab-' + tabName).classList.add('active');
            document.getElementById(modelId + '-content-' + tabName).classList.add('active');
        }}
    </script>
</head>
<body>
"""


def generate_header_section(data):
    """Generate the header section with dataset information."""
    return f"""    <h1>{data.get('name', 'Dataset')}</h1>
    <p class="metadata"><strong>Description:</strong> {data.get('description', 'N/A')}</p>
    <p class="metadata"><strong>Identifier:</strong> {data.get('identifier', 'N/A')}</p>
    <p class="metadata"><strong>Number of Models:</strong> {data.get('numberOfItems', 0)}</p>
"""


def generate_model_index(models):
    """Generate the clickable model index."""
    html = """    <div class="index" id="index">
        <h2>Model Index</h2>
        <ul>
"""
    for idx, model in enumerate(models):
        model_name = model.get('name', 'Unknown Model')
        model_id = f"model-{idx}"
        html += f'            <li><a href="#{model_id}">{model_name}</a></li>\n'

    html += """        </ul>
    </div>
"""
    return html


def generate_target_metadata_section(model):
    """Generate target metadata section."""
    if 'target_metadata' not in model:
        return ''

    target_meta = model['target_metadata']
    return f"""        <h3>Target Metadata</h3>
        <div class="author">
            <strong>Target Keys:</strong> {", ".join(target_meta.get("target_keys", []))}<br>
            <strong>Target Type:</strong> {target_meta.get("target_type", "N/A")}<br>
            <strong>Is Step Ahead:</strong> {target_meta.get("is_step_ahead", "N/A")}<br>
            <strong>Time Unit:</strong> {target_meta.get("time_unit", "N/A")}<br>
        </div>
"""


def generate_ensemble_section(model):
    """Generate ensemble configuration section."""
    if 'workExample' not in model or 'ensemble' not in model['workExample']:
        return ''

    ensemble = model['workExample']['ensemble']
    html = """        <h3>Ensemble Configuration</h3>
        <div class="author">
"""
    html += f'            <strong>Description:</strong> {ensemble.get("description", "N/A")}<br>\n'
    html += f'            <strong>Ensemble Type:</strong> {ensemble.get("ensemble_type", "N/A")}<br>\n'
    if 'task_ids' in ensemble:
        html += f'            <strong>Task IDs:</strong> {", ".join(ensemble["task_ids"].get("required", []))}<br>\n'
    html += '        </div>\n'
    return html


def generate_task_ids_section(model):
    """Generate task IDs section."""
    if 'workExample' not in model or 'task_ids' not in model['workExample']:
        return ''

    task_ids = model['workExample']['task_ids']
    return f"""        <h3>Task IDs</h3>
        <div class="author">
            <strong>Required:</strong> {", ".join(task_ids.get("required", []))}<br>
            <strong>Optional:</strong> {", ".join(task_ids.get("optional", []))}
        </div>
"""


def generate_authors_section(model):
    """Generate authors section in grid layout."""
    if 'author' not in model:
        return ''

    html = """        <h3>Authors</h3>
        <div class="authors-grid">
"""
    for author in model['author']:
        author_name = author.get('name', 'Unknown')
        affiliation = author.get('affiliation', {}).get('name', 'N/A')
        email = author.get('email', '')

        html += '            <div class="author">\n'
        html += f'                <strong>{author_name}</strong><br>\n'
        html += f'                <em>{affiliation}</em><br>\n'
        if email:
            html += f'                <a href="mailto:{email}">{email}</a>\n'
        html += '            </div>\n'

    html += '        </div>\n'
    return html


def generate_variables_measured_section(model):
    """Generate variables measured section in grid layout."""
    if 'workExample' not in model or 'variableMeasured' not in model['workExample']:
        return ''

    label = "Target" if len(model['workExample']['variableMeasured']) == 1 else "Targets"
    html = f"""        <h3>{label}</h3>
        <div class="variables-grid">
"""
    for variable in model['workExample']['variableMeasured']:
        html += '            <div class="author">\n'
        html += f'                <strong>{variable.get("name", "Unknown Variable")}</strong><br>\n'
        html += f'                <strong>Unit:</strong> {variable.get("unitText", "N/A")}<br>\n'
        html += f'                <strong>Target ID:</strong> {variable.get("target_id", "N/A")}<br>\n'
        html += f'                <strong>Type:</strong> {variable.get("target_type", "N/A")}<br>\n'
        html += f'                <strong>Temporal Unit:</strong> {variable.get("temporalUnit", "N/A")}<br>\n'
        if 'identifier' in variable:
            html += f'                <a href="{variable["identifier"]}" target="_blank">Ontology Reference</a><br>\n'
        html += '            </div>\n'

    html += '        </div>\n'
    return html


def format_available_output_types(variable):
    """Format target output-type availability for HTML display."""
    output_types = variable.get("available_output_types")
    if not output_types:
        return "N/A"
    if isinstance(output_types, list):
        return ", ".join(str(output_type) for output_type in output_types)
    return str(output_types)


def generate_spatial_coverage_section(model, geodata_map):
    """Generate spatial coverage section content."""
    if 'workExample' not in model or 'spatialCoverage' not in model['workExample']:
        return ''

    html = ''
    for location in model['workExample']['spatialCoverage']:
        location_name = location.get('gn:name', 'Unknown')
        location_code = location.get('iso3166-2:code', '')

        html += '                <div class="author">\n'
        if location_code and location_code in geodata_map:
            geoname_url = geodata_map[location_code]
            html += f'                    <a href="{geoname_url}" target="_blank">{location_name} ({location_code})</a><br>\n'
        elif location_code:
            search_link = f"https://www.geonames.org/search.html?q={location_name.replace(' ', '+')}"
            html += f'                    <a href="{search_link}" target="_blank">{location_name} ({location_code})</a><br>\n'
        else:
            html += f'                    {location_name}<br>\n'
        html += '                </div>\n'

    return html


def generate_output_types_section(model):
    """Generate output types section content."""
    if 'workExample' not in model or 'output_type' not in model['workExample']:
        return ''

    # output_type is stored as [list_of_types]; flatten to get individual type strings
    output_types = []
    for item in model['workExample']['output_type']:
        if isinstance(item, list):
            output_types.extend(item)
        else:
            output_types.append(item)

    html = ''
    for output_type in output_types:
        html += '                <div class="author">\n'
        html += f'                    {output_type}<br>\n'
        html += '                </div>\n'

    return html


def generate_age_groups_section(model):
    """Generate age groups section content."""
    if 'workExample' not in model or 'ageGroups' not in model['workExample']:
        return ''

    html = ''
    for age_group in model['workExample']['ageGroups']:
        html += '                <div class="author">\n'
        html += f'                    {age_group}<br>\n'
        html += '                </div>\n'

    return html


def generate_tabbed_section(model, model_idx, geodata_map, sample_output_html):
    """Generate tabbed section for sample output, output types, age groups, targets, and spatial coverage."""
    has_output_types = 'workExample' in model and 'output_type' in model['workExample']
    has_age_groups = 'workExample' in model and 'ageGroups' in model['workExample']
    has_targets = 'workExample' in model and 'variableMeasured' in model['workExample']
    has_spatial = 'workExample' in model and 'spatialCoverage' in model['workExample']
    has_sample_output = sample_output_html is not None and sample_output_html != ''

    if not (has_sample_output or has_output_types or has_age_groups or has_targets or has_spatial):
        return ''

    html = '        <div class="tabs">\n'
    html += '            <div class="tab-buttons">\n'

    # Projection Data Snippet is always the default tab if available
    first_tab = 'sample' if has_sample_output else None
    if not first_tab:
        if has_output_types:
            first_tab = 'output'
        elif has_targets:
            first_tab = 'targets'
        elif has_spatial:
            first_tab = 'spatial'
        elif has_age_groups:
            first_tab = 'age'

    # Add tab buttons
    if has_sample_output:
        html += f'                <button class="tab-button active" id="model-{model_idx}-tab-sample" onclick="switchTab({model_idx}, \'sample\')">Projection Data Snippet</button>\n'
    if has_targets:
        active_class = ' active' if first_tab == 'targets' else ''
        html += f'                <button class="tab-button{active_class}" id="model-{model_idx}-tab-targets" onclick="switchTab({model_idx}, \'targets\')">Targets</button>\n'
    if has_spatial:
        active_class = ' active' if first_tab == 'spatial' else ''
        html += f'                <button class="tab-button{active_class}" id="model-{model_idx}-tab-spatial" onclick="switchTab({model_idx}, \'spatial\')">Spatial Coverage</button>\n'
    if has_output_types:
        active_class = ' active' if first_tab == 'output' else ''
        html += f'                <button class="tab-button{active_class}" id="model-{model_idx}-tab-output" onclick="switchTab({model_idx}, \'output\')">Output Types</button>\n'
    if has_age_groups:
       active_class = ' active' if first_tab == 'age' else ''
       html += f'                <button class="tab-button{active_class}" id="model-{model_idx}-tab-age" onclick="switchTab({model_idx}, \'age\')">Age Groups</button>\n'

    html += '            </div>\n'

    # Add tab content for projection data snippet
    if has_sample_output:
        html += f'            <div class="tab-content active" id="model-{model_idx}-content-sample">\n'
        html += f'                {sample_output_html}\n'
        html += '            </div>\n'

    # Add tab content for output types
    if has_output_types:
        active_class = ' active' if first_tab == 'output' else ''
        html += f'            <div class="tab-content{active_class}" id="model-{model_idx}-content-output">\n'
        html += generate_output_types_section(model)
        html += '            </div>\n'

    # Add tab content for targets
    if has_targets:
        active_class = ' active' if first_tab == 'targets' else ''
        html += f'            <div class="tab-content{active_class}" id="model-{model_idx}-content-targets">\n'
        html += '                <div class="variables-grid">\n'
        for variable in model['workExample']['variableMeasured']:
            html += '                    <div class="author">\n'
            html += f'                        <strong>{variable.get("name", "Unknown Variable")}</strong><br>\n'
            html += f'                        <strong>Unit:</strong> {variable.get("unitText", "N/A")}<br>\n'
            html += f'                        <strong>Target ID:</strong> {variable.get("target_id", "N/A")}<br>\n'
            html += f'                        <strong>Type:</strong> {variable.get("target_type", "N/A")}<br>\n'
            html += f'                        <strong>Available Output Types:</strong> <span class="no-wrap">{format_available_output_types(variable)}</span> <br>\n'
            html += f'                        <strong>Temporal Unit:</strong> {variable.get("temporalUnit", "N/A")}<br>\n'
            if 'identifier' in variable:
                html += f'                        <a href="{variable["identifier"]}" target="_blank">Ontology Reference</a><br>\n'
            html += '                    </div>\n'
        html += '                </div>\n'
        html += '            </div>\n'

    # Add tab content for spatial coverage
    if has_spatial:
        active_class = ' active' if first_tab == 'spatial' else ''
        html += f'            <div class="tab-content{active_class}" id="model-{model_idx}-content-spatial">\n'
        html += generate_spatial_coverage_section(model, geodata_map)
        html += '            </div>\n'

    # Add tab content for age groups
    if has_age_groups:
        active_class = ' active' if first_tab == 'age' else ''
        html += f'            <div class="tab-content{active_class}" id="model-{model_idx}-content-age">\n'
        html += generate_age_groups_section(model)
        html += '            </div>\n'

    html += '        </div>\n'
    return html


def generate_temporal_coverage_section(model):
    """Generate temporal coverage section."""
    if 'workExample' not in model or 'temporalCoverage' not in model['workExample']:
        return ''

    temporal = model['workExample']['temporalCoverage']
    # Parse the date range (format: "start_date/end_date")
    if '/' in temporal:
        start_date, end_date = temporal.split('/')
        #remove time if present
        start_date = start_date.split(' ')[0]
        end_date = end_date.split(' ')[0]
        return f'        <p><strong>Temporal Coverage:</strong> <span class="location">{start_date}</span> to <span class="location">{end_date}</span></p>\n'
    else:
        return f'        <p><strong>Temporal Coverage:</strong> <span class="location">{temporal}</span></p>\n'


def parse_jsonld_to_html(jsonld_file, round_id):
    """Parse JSON-LD file and generate an HTML webpage."""

    # Load necessary data
    geodata_map = load_geodata_mapping()
    license_map = get_license_map()

    # Read the JSON-LD file
    with open(jsonld_file, 'r') as f:
        data = json.load(f)

    # Build HTML
    html = generate_html_head(data.get('name', 'Dataset'))
    html += generate_header_section(data)

    # Generate model index (sorted alphabetically by model name)
    models = sorted(data.get('hasPart', []), key=lambda m: m.get('name', '').lower())
    html += generate_model_index(models)

    # Process each model
    for idx, model in enumerate(models):
        license = model.get('license', 'N/A').upper()
        model_id = f"model-{idx}"
        parquet_html= get_first_n_rows_of_output(3, round_id, models[idx]["name"])

        # Model header
        html += f"""    <div class="model" id="{model_id}">
        <div class="model-header">
            <h2>{model.get('name', 'Unknown Model')}</h2>
            <a href="#index" class="back-to-top">↑ Back to Index</a>
        </div>
"""

        # Version, License, and Website on same line
        info_parts = []

        # Version
        version = model.get('version', 'N/A')
        info_parts.append(f'<strong>Version:</strong> {version}')

        # License with link
        if license in license_map:
            url = license_map[license]
            info_parts.append(f'<strong>License:</strong> <a href="{url}" target="_blank">{license}</a>')
        else:
            info_parts.append(f'<strong>License:</strong> {license}')

        # Website
        if 'website' in model:
            info_parts.append(f'<strong>Website:</strong> <a href="{model["website"]}" target="_blank">{model["website"]}</a>')

        html += f'        <p>{" ".join(info_parts)}</p>\n'

        # Target metadata
        html += generate_target_metadata_section(model)

        # Model tasks
        if 'modelTask' in model:
            html += f'        <p><strong>Model Tasks:</strong> {", ".join(model["modelTask"])}</p>\n'

        # Model category
        if 'modelCategory' in model:
            html += f'        <p><strong>Model Category:</strong> {", ".join(model["modelCategory"])}</p>\n'

        # Ensemble configuration
        html += generate_ensemble_section(model)

        # Task IDs
        html += generate_task_ids_section(model)


        # Description
        if 'description' in model:
            html += f'        <p><strong>Description:</strong> {model["description"]}</p>\n'

        # Data sources
        if 'isBasedOn' in model:
            html += f'        <p><strong>Data Sources:</strong> {model["isBasedOn"].get("description", "N/A")}</p>\n'

        # Producer
        if 'producer' in model:
            producer = model['producer']
            html += f'        <p><strong>Producer:</strong> {producer.get("name", "N/A")}</p>\n'
            if 'funder' in producer:
                html += f'        <p class="metadata"><em>Funding: {producer["funder"].get("description", "N/A")}</em></p>\n'

        # Authors
        html += generate_authors_section(model)

        # Temporal coverage
        html += generate_temporal_coverage_section(model)

        # Projection Data Snippet, Output types, Targets, Spatial Coverage, and Age groups in tabs
        html += generate_tabbed_section(model, idx, geodata_map, parquet_html)

        html += '    </div>\n'

        # Add a separator between models for visual structure.
        if idx < len(models) - 1:
            html += '    <hr class="model-separator">\n'

    # Close HTML
    html += """</body>
</html>
"""

    return html


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Convert JSON-LD model metadata to HTML visualization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert a specific round file
  python jsonld_to_html.py -i output/round_2024-07-28.jsonld -o output/round_2024-07-28.html -r 2024-07-28
  
  # Use default paths (reads from output/round_2024-07-28.jsonld)
  python jsonld_to_html.py
  
  # Process a different round
  python jsonld_to_html.py -i output/round_2023-11-12.jsonld -o output/round_2023-11-12.html -r 2023-11-12
        """
    )

    parser.add_argument(
        '-i', '--input',
        type=str,
        default='output/round_2024-07-28.jsonld',
        help='Input JSON-LD file path (default: output/round_2024-07-28.jsonld)'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default='output/round_2024-07-28.html',
        help='Output HTML file path (default: output/round_2024-07-28.html)'
    )

    parser.add_argument(
        '-r', '--round-id',
        type=str,
        default='2024-07-28',
        help='Round identifier for loading sample data (default: 2024-07-28)'
    )

    parser.add_argument(
        '--no-sample-data',
        action='store_true',
        help='Skip loading sample output data from parquet files'
    )

    args = parser.parse_args()

    # Validate input file exists
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Generate the HTML
    print(f"Converting {args.input} to HTML...")
    print(f"Round ID: {args.round_id}")

    try:
        html_content = parse_jsonld_to_html(args.input, args.round_id)

        # Write to file
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"✓ HTML file generated: {args.output}")

    except Exception as e:
        print(f"Error generating HTML: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

