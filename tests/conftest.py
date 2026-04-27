"""Shared path setup so tests can import pipeline and utils modules."""
import sys
from pathlib import Path

# Insert repo root into sys.path
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))
