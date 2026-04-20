"""
Create the unified UFO sightings SQLite database schema.
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild --only schema
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os

from ufosint.schema import create_schema

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    create_schema(DB_PATH)
