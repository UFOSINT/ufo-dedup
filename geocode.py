"""
Geocode locations in the unified database using the GeoNames gazetteer.

Usage:
    python geocode.py                    # Geocode all NULL lat/lng locations
    python geocode.py --download         # Download gazetteer first, then geocode
    python geocode.py --stats-only       # Just print current geocoding stats
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild --only geocode1
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

from ufosint.processors.geocoder import (
    download_gazetteer,
    run_geocoding,
    print_stats,
)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    if "--download" in sys.argv:
        download_gazetteer()

    if "--stats-only" in sys.argv:
        print_stats(DB_PATH)
    else:
        run_geocoding(DB_PATH)
