#!/usr/bin/env python3
"""
LLM Field Extraction — Mining structured data from descriptions.

Usage:
    python run_enrich.py                              # default: 5000 records
    python run_enrich.py --apply                      # apply cached results to DB
    python run_enrich.py --stats                      # show extraction coverage
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint enrich
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

from ufosint.llm.extractor import (
    run_extraction,
    apply_extractions,
    print_stats,
    DB_PATH,
    EXTRACT_CSV,
)

if __name__ == "__main__":
    import argparse
    from ufosint.llm.extractor import DEFAULT_MODEL, OPENROUTER_API_KEY, C

    parser = argparse.ArgumentParser(description="UFOSINT LLM Field Extraction")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--min-missing", type=int, default=2)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    if args.stats:
        print_stats()
    elif args.apply:
        apply_extractions()
    else:
        if not OPENROUTER_API_KEY:
            print(f"\n  {C.RED}ERROR: OPENROUTER_API_KEY not set.{C.RESET}")
            sys.exit(1)
        run_extraction(
            limit=args.limit, batch_size=args.batch_size,
            workers=args.workers, model=args.model,
            min_missing=args.min_missing,
        )
