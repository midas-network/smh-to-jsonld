import json
from pathlib import Path
import csv


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
        "CC-BY-4.0": "https://creativecommons.org/licenses/by/4.0/",
        "BSD SIMPLIFIED": "https://opensource.org/license/bsd-2-clause",
        "CC-BY_SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
        "MIT": "https://opensource.org/license/mit/",
        "GPL-3.0": "https://www.gnu.org/licenses/gpl-3.0.en.html"
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
        .author {{
            margin: 10px 0;
            padding: 10px;
            background: white;
            border-radius: 3px;
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
            margin-top: 10px;
            padding: 5px 10px;
            background: #3498db;
            color: white;
            border-radius: 3px;
            font-size: 0.9em;
        }}
        .back-to-top:hover {{ background: #2980b9; text-decoration: none; }}
    </style>
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


def generate_spatial_coverage_section(model, geodata_map):
    """Generate spatial coverage section with location links."""
    if 'workExample' not in model or 'spatialCoverage' not in model['workExample']:
        return ''

    html = """        <h3>Spatial Coverage</h3>
        <div>
"""
    for location in model['workExample']['spatialCoverage']:
        location_name = location.get('gn:name', 'Unknown')
        location_code = location.get('iso3166-2:code', '')

        # Use geodata mapping if available, otherwise fall back to search
        if location_code and location_code in geodata_map:
            geoname_url = geodata_map[location_code]
            html += f'            <span class="location"><a href="{geoname_url}" target="_blank">{location_name} ({location_code})</a></span>\n'
        elif location_code:
            search_link = f"https://www.geonames.org/search.html?q={location_name.replace(' ', '+')}"
            html += f'            <span class="location"><a href="{search_link}" target="_blank">{location_name} ({location_code})</a></span>\n'
        else:
            html += f'            <span class="location">{location_name}</span>\n'

    html += '        </div>\n'
    return html


def generate_output_types_section(model):
    """Generate output types section."""
    if 'workExample' not in model or 'output_type' not in model['workExample']:
        return ''

    html = '        <h3>Output Types</h3>\n'
    for output_type in model['workExample']['output_type']:
        html += '        <div class="author">\n'
        html += f'            <strong>Type:</strong> {output_type.get("type", "N/A")}<br>\n'

        output_type_id = output_type.get('output_type_id')
        if output_type_id and 'required' in output_type_id:
            quantiles = ', '.join(map(str, output_type_id['required']))
            html += f'            <strong>Output Type Ids:</strong> {quantiles}<br>\n'

        html += '        </div>\n'

    return html


def generate_age_groups_section(model):
    """Generate age groups section."""
    if 'workExample' not in model or 'ageGroups' not in model['workExample']:
        return ''

    html = """        <h3>Age Groups</h3>
        <div class="age-groups">
"""
    for age_group in model['workExample']['ageGroups']:
        html += f'            <span class="location">{age_group}</span>\n'

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
        return f'        <p><h3>Temporal Coverage</h3> <span class="location">{start_date}</span> to <span class="location">{end_date}</span></p>\n'
    else:
        return f'        <p><h3>Temporal Coverage</h3> <span class="location">{temporal}</span></p>\n'


def parse_jsonld_to_html(jsonld_file):
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

    # Generate model index
    models = data.get('hasPart', [])
    html += generate_model_index(models)

    # Process each model
    for idx, model in enumerate(models):
        license = model.get('license', 'N/A').upper()
        model_id = f"model-{idx}"

        # Model header
        html += f"""    <div class="model" id="{model_id}">
        <h2>{model.get('name', 'Unknown Model')}</h2>
        <a href="#index" class="back-to-top">↑ Back to Index</a>
        <p><strong>Version:</strong> {model.get('version', 'N/A')}</p>
"""

        # License with link
        if license in license_map:
            url = license_map[license]
            html += f'        <p><strong>License:</strong> <a href="{url}" target="_blank">{license}</a></p>\n'
        else:
            html += f'        <p><strong>License:</strong> {license}</p>\n'

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

        # Website
        if 'website' in model:
            html += f'        <p><strong>Website:</strong> <a href="{model["website"]}" target="_blank">{model["website"]}</a></p>\n'

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

        # Variables measured
        html += generate_variables_measured_section(model)

        # Spatial coverage
        html += generate_spatial_coverage_section(model, geodata_map)

        # Output types
        html += generate_output_types_section(model)

        # Age groups
        html += generate_age_groups_section(model)

        # Temporal coverage
        html += generate_temporal_coverage_section(model)

        html += '    </div>\n'

    # Close HTML
    html += """</body>
</html>
"""

    return html


# Generate the HTML
input_file = 'output/round_2024-07-28.jsonld'
output_file = 'output/round_2024-07-28.html'

html_content = parse_jsonld_to_html(input_file)

# Write to file
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"HTML file generated: {output_file}")
