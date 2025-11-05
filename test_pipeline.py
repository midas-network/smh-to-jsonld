#!/usr/bin/env python3
"""
Testing harness for the SMH to JSON-LD pipeline.
Tests the complete workflow: update_source_data -> create_jsonld -> to_html
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_header(message: str):
    """Print a formatted header message."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{message}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}\n")


def print_success(message: str):
    """Print a success message."""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")


def print_error(message: str):
    """Print an error message."""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")


def print_warning(message: str):
    """Print a warning message."""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")


def print_info(message: str):
    """Print an info message."""
    print(f"{Colors.OKCYAN}ℹ {message}{Colors.ENDC}")


def run_command(command: List[str], description: str) -> Tuple[bool, str, str]:
    """
    Run a shell command and capture output.

    Args:
        command: Command to run as list of strings
        description: Description of what the command does

    Returns:
        Tuple of (success, stdout, stderr)
    """
    print_info(f"Running: {description}")
    print(f"  Command: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            print_success(f"{description} completed successfully")
            return True, result.stdout, result.stderr
        else:
            print_error(f"{description} failed with return code {result.returncode}")
            if result.stderr:
                print(f"  Error output: {result.stderr[:500]}")
            return False, result.stdout, result.stderr

    except subprocess.TimeoutExpired:
        print_error(f"{description} timed out after 5 minutes")
        return False, "", "Timeout"
    except Exception as e:
        print_error(f"{description} failed with exception: {e}")
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


def test_update_source_data(skip_update: bool = False) -> bool:
    """
    Test the update_source_data.py script.

    Args:
        skip_update: If True, skip the actual update and just verify structure

    Returns:
        True if test passed, False otherwise
    """
    print_header("Testing update_source_data.py")

    if skip_update:
        print_warning("Skipping actual data update (--skip-update flag set)")
        # Just verify that data directory exists and has some content
        data_dir = Path('data')
        if not data_dir.exists():
            print_error("Data directory does not exist")
            return False

        # Check for round directories
        round_dirs = [d for d in data_dir.iterdir()
                     if d.is_dir() and re.match(r'\d{4}-\d{2}-\d{2}', d.name)]

        if not round_dirs:
            print_error("No round directories found in data/")
            return False

        print_success(f"Found {len(round_dirs)} round directories")
        return True

    # Run the update script
    success, stdout, stderr = run_command(
        ['python3', 'pipeline/update_source_data.py'],
        'Updating source data'
    )

    if not success:
        return False

    # Verify output
    data_dir = Path('data')
    if not data_dir.exists():
        print_error("Data directory was not created")
        return False

    # Check for expected subdirectories
    round_dirs = [d for d in data_dir.iterdir()
                 if d.is_dir() and re.match(r'\d{4}-\d{2}-\d{2}', d.name)]

    if not round_dirs:
        print_error("No round directories created")
        return False

    print_success(f"Created/updated {len(round_dirs)} round directories")

    # Check structure of at least one round
    for round_dir in round_dirs[:1]:  # Check first round only
        expected_subdirs = ['hub-config', 'model-metadata', 'model-output']
        for subdir in expected_subdirs:
            if not (round_dir / subdir).exists():
                print_warning(f"Missing expected subdirectory: {round_dir}/{subdir}")

    return True


def test_create_jsonld() -> bool:
    """
    Test the create_jsonld.py script.

    Returns:
        True if test passed, False otherwise
    """
    print_header("Testing create_jsonld.py")

    # Run the script
    success, stdout, stderr = run_command(
        ['python3', 'pipeline/create_jsonld.py'],
        'Creating JSON-LD files'
    )

    if not success:
        return False

    # Verify output directory exists
    output_dir = Path('output')
    if not output_dir.exists():
        print_error("Output directory was not created")
        return False

    # Find all JSON-LD files
    jsonld_files = list(output_dir.rglob('*.jsonld'))

    if not jsonld_files:
        print_error("No JSON-LD files were created")
        return False

    print_success(f"Created {len(jsonld_files)} JSON-LD files")

    # Validate each JSON-LD file
    all_valid = True
    for jsonld_file in jsonld_files:
        is_valid, issues = validate_jsonld_file(jsonld_file)

        if is_valid:
            print_success(f"Valid: {jsonld_file.relative_to(output_dir)}")
        else:
            print_error(f"Invalid: {jsonld_file.relative_to(output_dir)}")
            for issue in issues:
                print(f"    - {issue}")
            all_valid = False

    return all_valid


def test_to_html() -> bool:
    """
    Test the jsonld_to_html.py script.

    Returns:
        True if test passed, False otherwise
    """
    print_header("Testing jsonld_to_html.py")

    # Find available round JSON-LD files
    output_dir = Path('output')
    round_jsonld_files = list(output_dir.glob('round_*.jsonld'))

    if not round_jsonld_files:
        print_error("No round JSON-LD files found to convert")
        return False

    # Test conversion for each round file
    all_success = True
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
            all_success = False
            continue

    if not all_success:
        return False

    # Find HTML files
    html_files = list(output_dir.glob('*.html'))

    if not html_files:
        print_error("No HTML files were created")
        return False

    print_success(f"Created {len(html_files)} HTML files")

    # Validate each HTML file
    all_valid = True
    for html_file in html_files:
        is_valid, issues = validate_html_file(html_file)

        if is_valid:
            print_success(f"Valid: {html_file.relative_to(output_dir)}")
        else:
            print_error(f"Invalid: {html_file.relative_to(output_dir)}")
            for issue in issues:
                print(f"    - {issue}")
            all_valid = False

    return all_valid


def print_summary(results: Dict[str, bool], start_time: datetime):
    """
    Print a summary of test results.

    Args:
        results: Dictionary mapping test names to success status
        start_time: When tests started
    """
    print_header("Test Summary")

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    for test_name, success in results.items():
        if success:
            print_success(f"{test_name}: PASSED")
        else:
            print_error(f"{test_name}: FAILED")

    print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
    print(f"  Total tests: {total}")
    print(f"  {Colors.OKGREEN}Passed: {passed}{Colors.ENDC}")
    print(f"  {Colors.FAIL}Failed: {failed}{Colors.ENDC}")

    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n{Colors.BOLD}Duration: {duration:.2f} seconds{Colors.ENDC}")

    if failed == 0:
        print(f"\n{Colors.OKGREEN}{Colors.BOLD}All tests passed! ✓{Colors.ENDC}")
    else:
        print(f"\n{Colors.FAIL}{Colors.BOLD}Some tests failed ✗{Colors.ENDC}")


def main():
    """Main entry point for the test harness."""
    parser = argparse.ArgumentParser(
        description='Test the SMH to JSON-LD pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests
  python test_pipeline.py
  
  # Skip data update (use existing data)
  python test_pipeline.py --skip-update
  
  # Run only specific tests
  python test_pipeline.py --skip-update --skip-html
        """
    )

    parser.add_argument(
        '--skip-update',
        action='store_true',
        help='Skip updating source data (assumes data already exists)'
    )

    parser.add_argument(
        '--skip-jsonld',
        action='store_true',
        help='Skip JSON-LD creation test'
    )

    parser.add_argument(
        '--skip-html',
        action='store_true',
        help='Skip HTML generation test'
    )

    args = parser.parse_args()

    start_time = datetime.now()
    results = {}

    print_header("SMH to JSON-LD Pipeline Test Harness")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Test update_source_data
    if not args.skip_update or args.skip_update:  # Always run, but may skip actual update
        results['update_source_data'] = test_update_source_data(skip_update=args.skip_update)

    # Test create_jsonld
    if not args.skip_jsonld:
        results['create_jsonld'] = test_create_jsonld()

    # Test to_html
    if not args.skip_html:
        results['to_html'] = test_to_html()

    # Print summary
    print_summary(results, start_time)

    # Exit with appropriate code
    sys.exit(0 if all(results.values()) else 1)


if __name__ == '__main__':
    main()

