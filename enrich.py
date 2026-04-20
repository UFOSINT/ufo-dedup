"""
Enrich NUFORC sightings with metadata from skipped UFOCAT records.

Run AFTER all imports are complete:
    python enrich.py
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild --only enrich_nuforc
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os

from ufosint.processors.enrich_nuforc import run_enrichment

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    run_enrichment(DB_PATH)
