#!/usr/bin/env python3
"""Clean generated output files from the output directory.

Removes consolidated round JSON-LD files, HTML files, and per-model
subdirectories produced by the JSON-LD generation pipeline.

Usage:
    # Clean everything in output/
    uv run python pipeline/clean_output.py

    # Clean only specific rounds
    uv run python pipeline/clean_output.py --rounds 2025-07-27 2024-07-28

    # Preview what would be deleted without deleting
    uv run python pipeline/clean_output.py --dry-run
"""

import argparse
import re
import shutil
import sys
from pathlib import Path


def clean_output(output_dir: str = "output", rounds: list = None, dry_run: bool = False) -> bool:
    """Remove generated pipeline output files.

    Deletes:
    - output/round_<ID>*.jsonld  — consolidated round JSON-LD files
    - output/round_<ID>*.html    — consolidated round HTML files
    - output/<YYYY-MM-DD>/       — per-round subdirectories (per-model JSON-LD)

    Args:
        output_dir: Path to the output directory (default: "output").
        rounds: List of round IDs to restrict deletion to (e.g. ["2025-07-27"]).
                When None, all rounds are cleaned.
        dry_run: If True, print what would be deleted without actually deleting.

    Returns:
        True if the operation completed without errors.
    """
    output_path = Path(output_dir)

    if not output_path.exists():
        print(f"Output directory '{output_dir}' does not exist — nothing to clean.")
        return True

    deleted = []
    errors = []

    def _matches_round(name: str) -> bool:
        """Return True if the name belongs to one of the requested rounds (or all rounds)."""
        if rounds is None:
            return True
        return any(r in name for r in rounds)

    # --- Consolidated round files (output/round_*.jsonld, output/round_*.html) ---
    for pattern in ("round_*.jsonld", "round_*.html"):
        for f in sorted(output_path.glob(pattern)):
            if not _matches_round(f.name):
                continue
            if dry_run:
                print(f"  [dry-run] would delete: {f}")
            else:
                try:
                    f.unlink()
                    print(f"  deleted: {f}")
                    deleted.append(f)
                except OSError as exc:
                    print(f"  ERROR deleting {f}: {exc}", file=sys.stderr)
                    errors.append(f)

    # --- Per-round subdirectories (output/YYYY-MM-DD/) ---
    for d in sorted(output_path.iterdir()):
        if not d.is_dir():
            continue
        if not re.match(r"\d{4}-\d{2}-\d{2}", d.name):
            continue
        if not _matches_round(d.name):
            continue
        if dry_run:
            print(f"  [dry-run] would delete directory: {d}/")
        else:
            try:
                shutil.rmtree(d)
                print(f"  deleted directory: {d}/")
                deleted.append(d)
            except OSError as exc:
                print(f"  ERROR deleting {d}/: {exc}", file=sys.stderr)
                errors.append(d)

    if dry_run:
        return True

    if deleted:
        print(f"\nRemoved {len(deleted)} item(s).")
    else:
        print("Nothing to remove.")

    return len(errors) == 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean generated output files from the pipeline output directory."
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Output directory to clean (default: output)",
    )
    parser.add_argument(
        "--rounds",
        nargs="+",
        metavar="ROUND_ID",
        help="Clean only specific round IDs (e.g. 2025-07-27). Default: all rounds.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without actually deleting.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    label = "Cleaning output" + (f" for rounds: {', '.join(args.rounds)}" if args.rounds else " (all rounds)")
    if args.dry_run:
        label += " [DRY RUN]"
    print(label)

    success = clean_output(
        output_dir=args.output,
        rounds=args.rounds,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
