"""Pytest configuration for SMH to JSON-LD pipeline tests."""

import pytest


def pytest_addoption(parser):
    """Add custom command-line options for pytest."""
    parser.addoption(
        "--skip-update",
        action="store_true",
        default=False,
        help="Skip updating source data (assumes data already exists)"
    )

