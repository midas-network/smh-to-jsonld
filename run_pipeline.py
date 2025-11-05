#!/usr/bin/env python3
"""
Main orchestration script for the SMH to JSON-LD pipeline.
Runs all pipeline steps in sequence: update_source_data -> create_jsonld -> jsonld_to_html
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


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


def run_command(command: List[str], description: str, show_output: bool = True) -> Tuple[bool, str, str]:
    """
    Run a shell command and optionally display output in real-time.

    Args:
        command: Command to run as list of strings
        description: Description of what the command does
        show_output: If True, print stdout/stderr in real-time

    Returns:
        Tuple of (success, stdout, stderr)
    """
    print_info(f"Running: {description}")
    print(f"  Command: {' '.join(command)}")
    print()

    try:
        if show_output:
            # Run with real-time output
            result = subprocess.run(
                command,
                text=True,
                timeout=600  # 10 minute timeout
            )
            success = result.returncode == 0
            stdout, stderr = "", ""
        else:
            # Run with captured output
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=600
            )
            success = result.returncode == 0
            stdout, stderr = result.stdout, result.stderr

            if stdout:
                print(stdout)
            if stderr:
                print(stderr, file=sys.stderr)

        if success:
            print_success(f"{description} completed successfully\n")
        else:
            print_error(f"{description} failed with return code {result.returncode}\n")

        return success, stdout, stderr

    except subprocess.TimeoutExpired:
        print_error(f"{description} timed out after 10 minutes\n")
        return False, "", "Timeout"
    except Exception as e:
        print_error(f"{description} failed with exception: {e}\n")
        return False, "", str(e)


def update_source_data(skip: bool = False) -> bool:
    """
    Run the update_source_data.py script.

    Args:
        skip: If True, skip this step

    Returns:
        True if successful, False otherwise
    """
    if skip:
        print_warning("Skipping source data update")
        return True

    print_header("Step 1: Updating Source Data")

    success, _, _ = run_command(
        ['python3', 'pipeline/update_source_data.py'],
        'Downloading and updating source data from repositories'
    )

    if not success:
        print_error("Failed to update source data")
        return False

    # Verify data directory has content
    data_dir = Path('data')
    if not data_dir.exists():
        print_error("Data directory was not created")
        return False

    round_dirs = [d for d in data_dir.iterdir()
                 if d.is_dir() and d.name[0].isdigit()]

    if not round_dirs:
        print_error("No round directories found in data/")
        return False

    print_success(f"Found {len(round_dirs)} round directories: {', '.join([d.name for d in round_dirs])}")
    return True


def create_jsonld(rounds: List[str] = None) -> bool:
    """
    Run the create_jsonld.py script.

    Args:
        rounds: List of specific round IDs to process (None = all rounds)

    Returns:
        True if successful, False otherwise
    """
    print_header("Step 2: Creating JSON-LD Files")

    success, _, _ = run_command(
        ['python3', 'pipeline/create_jsonld.py'],
        'Converting model metadata to JSON-LD format'
    )

    if not success:
        print_error("Failed to create JSON-LD files")
        return False

    # Verify output files were created
    output_dir = Path('output')
    if not output_dir.exists():
        print_error("Output directory was not created")
        return False

    jsonld_files = list(output_dir.glob('round_*.jsonld'))
    if not jsonld_files:
        print_error("No round JSON-LD files were created")
        return False

    print_success(f"Created {len(jsonld_files)} round JSON-LD files: {', '.join([f.name for f in jsonld_files])}")
    return True


def generate_html(rounds: List[str] = None) -> bool:
    """
    Run the jsonld_to_html.py script for all round files.

    Args:
        rounds: List of specific round IDs to process (None = all rounds)

    Returns:
        True if successful, False otherwise
    """
    print_header("Step 3: Generating HTML Visualizations")

    output_dir = Path('output')
    round_jsonld_files = list(output_dir.glob('round_*.jsonld'))

    if not round_jsonld_files:
        print_error("No round JSON-LD files found to convert")
        return False

    # Filter by specific rounds if provided
    if rounds:
        round_jsonld_files = [f for f in round_jsonld_files
                            if any(r in f.name for r in rounds)]
        if not round_jsonld_files:
            print_error(f"No JSON-LD files found for rounds: {rounds}")
            return False

    all_success = True
    html_files = []

    for jsonld_file in round_jsonld_files:
        # Extract round ID from filename (e.g., round_2024-07-28.jsonld -> 2024-07-28)
        round_id = jsonld_file.stem.replace('round_', '')
        output_file = jsonld_file.with_suffix('.html')

        print_info(f"Converting {jsonld_file.name} to HTML...")

        success, _, _ = run_command(
            ['python3', 'pipeline/jsonld_to_html.py',
             '-i', str(jsonld_file),
             '-o', str(output_file),
             '-r', round_id],
            f'Generating HTML for round {round_id}',
            show_output=False
        )

        if success:
            html_files.append(output_file.name)
        else:
            all_success = False
            print_error(f"Failed to convert {jsonld_file.name}")

    if all_success:
        print_success(f"Generated {len(html_files)} HTML files: {', '.join(html_files)}")

    return all_success


def main():
    """Main entry point for pipeline orchestration."""
    parser = argparse.ArgumentParser(
        description='Run the complete SMH to JSON-LD pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline
  python run_pipeline.py
  
  # Skip data update (use existing data)
  python run_pipeline.py --skip-update
  
  # Run only specific steps
  python run_pipeline.py --skip-update --skip-html
  
  # Process specific rounds only
  python run_pipeline.py --rounds 2024-07-28 2023-11-12
  
  # Stop on first error
  python run_pipeline.py --stop-on-error
        """
    )

    parser.add_argument(
        '--skip-update',
        action='store_true',
        help='Skip updating source data (use existing data)'
    )

    parser.add_argument(
        '--skip-jsonld',
        action='store_true',
        help='Skip JSON-LD creation'
    )

    parser.add_argument(
        '--skip-html',
        action='store_true',
        help='Skip HTML generation'
    )

    parser.add_argument(
        '--rounds',
        nargs='+',
        metavar='ROUND_ID',
        help='Process only specific round IDs (e.g., 2024-07-28)'
    )

    parser.add_argument(
        '--stop-on-error',
        action='store_true',
        help='Stop pipeline execution on first error'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show verbose output'
    )

    args = parser.parse_args()

    start_time = datetime.now()

    print_header("SMH to JSON-LD and HTML Pipeline")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    if args.rounds:
        print_info(f"Processing specific rounds: {', '.join(args.rounds)}")

    # Track results
    results = {}

    # Step 1: Update source data
    if not args.skip_update:
        results['update_source_data'] = update_source_data()
        if not results['update_source_data'] and args.stop_on_error:
            print_error("Pipeline stopped due to error in source data update")
            sys.exit(1)
    else:
        print_warning("Skipping source data update (--skip-update flag set)")

    # Step 2: Create JSON-LD files
    if not args.skip_jsonld:
        results['create_jsonld'] = create_jsonld(args.rounds)
        if not results['create_jsonld'] and args.stop_on_error:
            print_error("Pipeline stopped due to error in JSON-LD creation")
            sys.exit(1)
    else:
        print_warning("Skipping JSON-LD creation (--skip-jsonld flag set)")

    # Step 3: Generate HTML
    if not args.skip_html:
        results['generate_html'] = generate_html(args.rounds)
        if not results['generate_html'] and args.stop_on_error:
            print_error("Pipeline stopped due to error in HTML generation")
            sys.exit(1)
    else:
        print_warning("Skipping HTML generation (--skip-html flag set)")

    # Print summary
    print_header("Pipeline Summary")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    completed_steps = len(results)
    successful_steps = sum(1 for v in results.values() if v)
    failed_steps = completed_steps - successful_steps

    for step_name, success in results.items():
        if success:
            print_success(f"{step_name}: SUCCESS")
        else:
            print_error(f"{step_name}: FAILED")

    print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
    print(f"  Completed steps: {completed_steps}")
    print(f"  {Colors.OKGREEN}Successful: {successful_steps}{Colors.ENDC}")
    print(f"  {Colors.FAIL}Failed: {failed_steps}{Colors.ENDC}")
    print(f"\n{Colors.BOLD}Duration: {duration:.2f} seconds{Colors.ENDC}")
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    if failed_steps == 0:
        print(f"\n{Colors.OKGREEN}{Colors.BOLD}✓ Pipeline completed successfully!{Colors.ENDC}")
        sys.exit(0)
    else:
        print(f"\n{Colors.FAIL}{Colors.BOLD}✗ Pipeline completed with {failed_steps} error(s){Colors.ENDC}")
        sys.exit(1)


if __name__ == '__main__':
    main()

