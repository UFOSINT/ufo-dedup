"""
Deduplication script for the unified UFO sightings database.

Usage:
  python dedup.py              # Run all tiers
  python dedup.py --tier 1     # Run only tier 1
  python dedup.py --tier verify # Just print the verification report
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild --only dedup
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os

from ufosint.processors.dedup import main

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    main(DB_PATH)
