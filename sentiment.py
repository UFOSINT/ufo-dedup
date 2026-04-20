"""
Batch sentiment/emotion analysis for UFO sighting descriptions.

Usage:
    python sentiment.py                # Analyze all unprocessed sightings
    python sentiment.py --stats-only   # Print current analysis stats
    python sentiment.py --reset        # Delete all and re-analyze
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild --only sentiment
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

from ufosint.processors.sentiment_analysis import (
    run_sentiment,
    print_stats,
    reset_sentiment,
)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    if "--stats-only" in sys.argv:
        print_stats(DB_PATH)
    elif "--reset" in sys.argv:
        reset_sentiment(DB_PATH)
        run_sentiment(DB_PATH)
    else:
        run_sentiment(DB_PATH)
