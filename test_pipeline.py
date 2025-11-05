#!/usr/bin/env python3
"""
Testing harness for the SMH to JSON-LD pipeline using pytest.
Tests the complete workflow: update_source_data -> create_jsonld -> jsonld_to_html

Usage:
    # Run all tests
    pytest test_pipeline.py

    # Skip data update (use existing data)
    pytest test_pipeline.py --skip-update

    # Run specific test
    pytest test_pipeline.py::test_create_jsonld

    # Verbose output
    pytest test_pipeline.py -v

    # Generate HTML report
    pytest test_pipeline.py --html=report.html --self-contained-html
"""

import json
import re
import subprocess
from pathlib import Path
from typing import List, Tuple

import pytest



@pytest.fixture(scope="session")
def skip_update(request):
    """Fixture to get skip_update option."""
    return request.config.getoption("--skip-update")


@pytest.fixture(scope="session")
def output_dir():
    """Fixture for the output directory."""
    return Path('output')


@pytest.fixture(scope="session")
def data_dir():
    """Fixture for the data directory."""
    return Path('data')


def run_command(command: List[str], description: str) -> Tuple[bool, str, str]:
    """
    Run a shell command and capture output.

    Args:
        command: Command to run as list of strings
        description: Description of what the command does

    Returns:
        Tuple of (success, stdout, stderr)
    """
    print(f"\nRunning: {description}")
    print(f"Command: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            print(f"✓ {description} completed successfully")
            return True, result.stdout, result.stderr
        else:
            print(f"✗ {description} failed with return code {result.returncode}")
            if result.stderr:
                print(f"Error output: {result.stderr[:500]}")
            return False, result.stdout, result.stderr

    except subprocess.TimeoutExpired:
        print(f"✗ {description} timed out after 5 minutes")
        return False, "", "Timeout"
    except Exception as e:
        print(f"✗ {description} failed with exception: {e}")
        return False, "", str(e)


def validate_jsonld_file(filepath: Path) -> Tuple[bool, List[str]]:
    """
    Validate a JSON-LD file for basic structure and required fields.

    Args:
        filepath: Path to the JSON-LD file

    Returns:
        Tuple of (is_valid, list of issues)
    """
    issues = []

    if not filepath.exists():
        issues.append(f"File does not exist: {filepath}")
        return False, issues

    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        issues.append(f"Invalid JSON: {e}")
        return False, issues
    except Exception as e:
        issues.append(f"Error reading file: {e}")
        return False, issues

    # Check for required Schema.org fields
    required_fields = ['@context', '@type']
    for field in required_fields:
        if field not in data:
            issues.append(f"Missing required field: {field}")

    # Check context
    if '@context' in data:
        if 'schema.org' not in str(data['@context']).lower():
            issues.append("@context does not reference schema.org")

    # Check type
    if '@type' in data and data['@type'] != 'Dataset':
        issues.append(f"Expected @type 'Dataset', got '{data['@type']}'")

    # For round files, check for hasPart
    if 'round_' in str(filepath):
        if 'hasPart' not in data:
            issues.append("Round file missing 'hasPart' array")
        elif not isinstance(data['hasPart'], list):
            issues.append("'hasPart' should be an array")
        elif len(data['hasPart']) == 0:
            issues.append("'hasPart' array is empty")

    # Check for basic metadata
    recommended_fields = ['name', 'description']
    for field in recommended_fields:
        if field not in data:
            issues.append(f"Missing recommended field: {field}")

    # For individual model files (not round files), check for author
    if not filepath.name.startswith('round'):
        if 'author' not in data:
            issues.append("Missing recommended field: author")
        elif not isinstance(data['author'], list):
            issues.append("'author' should be an array")
        elif len(data['author']) == 0:
            issues.append("'author' array is empty")

    return len(issues) == 0, issues


def validate_html_file(filepath: Path) -> Tuple[bool, List[str]]:
    """
    Validate an HTML file for basic structure.

    Args:
        filepath: Path to the HTML file

    Returns:
        Tuple of (is_valid, list of issues)
    """
    issues = []

    if not filepath.exists():
        issues.append(f"File does not exist: {filepath}")
        return False, issues

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        issues.append(f"Error reading file: {e}")
        return False, issues

    # Check for basic HTML structure
    required_tags = ['<!DOCTYPE html>', '<html', '<head>', '<body>', '</html>']
    for tag in required_tags:
        if tag not in content:
            issues.append(f"Missing HTML tag: {tag}")

    # Check for title
    if '<title>' not in content:
        issues.append("Missing <title> tag")

    # Check for CSS
    if '<style>' not in content and '<link rel="stylesheet"' not in content:
        issues.append("No CSS styling found")

    # Check for content
    if len(content) < 1000:
        issues.append("HTML file seems too small, may be missing content")

    # Check for common elements
    expected_elements = ['Model Index', 'Authors', 'Spatial Coverage', 'Temporal Coverage', 'Age Groups']
    for element in expected_elements:
        if element not in content:
            issues.append(f"Missing expected content: {element}")

    return len(issues) == 0, issues


class TestUpdateSourceData:
    """Tests for the update_source_data.py script."""

    def test_update_source_data(self, skip_update, data_dir):
        """
        Test the update_source_data.py script.

        Args:
            skip_update: If True, skip the actual update and just verify structure
            data_dir: Path to the data directory
        """
        if skip_update:
            print("\n⚠ Skipping actual data update (--skip-update flag set)")
            # Just verify that data directory exists and has some content
            assert data_dir.exists(), "Data directory does not exist"

            # Check for round directories
            round_dirs = [d for d in data_dir.iterdir()
                         if d.is_dir() and re.match(r'\d{4}-\d{2}-\d{2}', d.name)]

            assert len(round_dirs) > 0, "No round directories found in data/"
            print(f"✓ Found {len(round_dirs)} round directories")
            return

        # Run the update script
        success, stdout, stderr = run_command(
            ['python3', 'pipeline/update_source_data.py'],
            'Updating source data'
        )

        assert success, f"update_source_data.py failed: {stderr}"

        # Verify output
        assert data_dir.exists(), "Data directory was not created"

        # Check for expected subdirectories
        round_dirs = [d for d in data_dir.iterdir()
                     if d.is_dir() and re.match(r'\d{4}-\d{2}-\d{2}', d.name)]

        assert len(round_dirs) > 0, "No round directories created"
        print(f"✓ Created/updated {len(round_dirs)} round directories")

        # Check structure of at least one round
        for round_dir in round_dirs[:1]:  # Check first round only
            expected_subdirs = ['hub-config', 'model-metadata', 'model-output']
            for subdir in expected_subdirs:
                subdir_path = round_dir / subdir
                if not subdir_path.exists():
                    print(f"⚠ Missing expected subdirectory: {round_dir}/{subdir}")


class TestCreateJsonLD:
    """Tests for the create_jsonld.py script."""

    def test_create_jsonld(self, output_dir):
        """
        Test the create_jsonld.py script.

        Args:
            output_dir: Path to the output directory
        """
        # Run the script
        success, stdout, stderr = run_command(
            ['python3', 'pipeline/create_jsonld.py'],
            'Creating JSON-LD files'
        )

        assert success, f"create_jsonld.py failed: {stderr}"

        # Verify output directory exists
        assert output_dir.exists(), "Output directory was not created"

        # Find all JSON-LD files
        jsonld_files = list(output_dir.rglob('*.jsonld'))

        assert len(jsonld_files) > 0, "No JSON-LD files were created"
        print(f"✓ Created {len(jsonld_files)} JSON-LD files")

    def test_validate_jsonld_files(self, output_dir):
        """
        Validate all JSON-LD files for proper structure and required fields.

        Args:
            output_dir: Path to the output directory
        """
        # Find all JSON-LD files
        jsonld_files = list(output_dir.rglob('*.jsonld'))
        assert len(jsonld_files) > 0, "No JSON-LD files found to validate"

        # Validate each JSON-LD file
        all_issues = {}
        for jsonld_file in jsonld_files:
            is_valid, issues = validate_jsonld_file(jsonld_file)

            if is_valid:
                print(f"✓ Valid: {jsonld_file.relative_to(output_dir)}")
            else:
                print(f"✗ Invalid: {jsonld_file.relative_to(output_dir)}")
                for issue in issues:
                    print(f"    - {issue}")
                all_issues[str(jsonld_file.relative_to(output_dir))] = issues

        # Assert that all files are valid
        if all_issues:
            error_msg = "\n".join([
                f"{file}:\n  " + "\n  ".join(issues)
                for file, issues in all_issues.items()
            ])
            pytest.fail(f"JSON-LD validation failed for {len(all_issues)} files:\n{error_msg}")


class TestJsonLDToHTML:
    """Tests for the jsonld_to_html.py script."""

    def test_to_html(self, output_dir):
        """
        Test the jsonld_to_html.py script.

        Args:
            output_dir: Path to the output directory
        """
        # Find available round JSON-LD files
        round_jsonld_files = list(output_dir.glob('round_*.jsonld'))

        assert len(round_jsonld_files) > 0, "No round JSON-LD files found to convert"

        # Test conversion for each round file
        failed_conversions = []
        for jsonld_file in round_jsonld_files:
            # Extract round ID from filename (e.g., round_2024-07-28.jsonld -> 2024-07-28)
            round_id = jsonld_file.stem.replace('round_', '')
            output_file = jsonld_file.with_suffix('.html')

            # Run the script
            success, stdout, stderr = run_command(
                ['python3', 'pipeline/jsonld_to_html.py',
                 '-i', str(jsonld_file),
                 '-o', str(output_file),
                 '-r', round_id],
                f'Converting {jsonld_file.name} to HTML'
            )

            if not success:
                failed_conversions.append((jsonld_file.name, stderr))

        assert len(failed_conversions) == 0, \
            f"Failed to convert {len(failed_conversions)} files: {failed_conversions}"

        # Find HTML files
        html_files = list(output_dir.glob('*.html'))
        assert len(html_files) > 0, "No HTML files were created"
        print(f"✓ Created {len(html_files)} HTML files")

    def test_validate_html_files(self, output_dir):
        """
        Validate all HTML files for proper structure.

        Args:
            output_dir: Path to the output directory
        """
        # Find HTML files
        html_files = list(output_dir.glob('*.html'))
        assert len(html_files) > 0, "No HTML files found to validate"

        # Validate each HTML file
        all_issues = {}
        for html_file in html_files:
            is_valid, issues = validate_html_file(html_file)

            if is_valid:
                print(f"✓ Valid: {html_file.relative_to(output_dir)}")
            else:
                print(f"✗ Invalid: {html_file.relative_to(output_dir)}")
                for issue in issues:
                    print(f"    - {issue}")
                all_issues[str(html_file.relative_to(output_dir))] = issues

        # Assert that all files are valid
        if all_issues:
            error_msg = "\n".join([
                f"{file}:\n  " + "\n  ".join(issues)
                for file, issues in all_issues.items()
            ])
            pytest.fail(f"HTML validation failed for {len(all_issues)} files:\n{error_msg}")



