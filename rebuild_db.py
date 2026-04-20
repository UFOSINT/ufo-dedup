"""
Master rebuild script for the Unified UFO Sightings Database.

Usage:
    python rebuild_db.py                # Full rebuild
    python rebuild_db.py --skip-dedup   # Skip dedup
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

# Re-export so existing imports still work
from ufosint.fixes import apply_data_fixes, US_CA_STATES

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    from ufosint.pipeline import Pipeline
    Pipeline().run()
