"""
LLM spot-check: grade a random sample of sightings for data quality.

Usage:
    python spot_check.py --count 500 --workers 10
    python spot_check.py --count 100 --preview
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint spot-check
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

from ufosint.llm.spot_check import run_spot_check, DB_PATH

if __name__ == "__main__":
    import argparse
    from ufosint.llm.spot_check import DEFAULT_MODEL, OPENROUTER_API_KEY

    parser = argparse.ArgumentParser(description="LLM spot-check random sample")
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    if not args.preview and not OPENROUTER_API_KEY:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    run_spot_check(
        count=args.count, batch_size=args.batch_size,
        workers=args.workers, model=args.model,
        preview=args.preview, db_path=args.db,
    )
