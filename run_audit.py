#!/usr/bin/env python3
"""
UFO Sighting Database — LLM Audit Pipeline (dashboard runner).

Usage:
    python run_audit.py                          # run all (default 120K, 15 workers)
    python run_audit.py --limit 1000             # first 1000 only
    python run_audit.py --workers 20             # more parallel
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint audit b --workers 15
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

from ufosint.llm.run_audit import run_tier_b_with_dashboard

if __name__ == "__main__":
    import argparse
    from ufosint.llm.audit import DEFAULT_MODEL

    parser = argparse.ArgumentParser(
        description="UFOSINT Audit Pipeline — Tier B Location Normalization",
    )
    parser.add_argument("--limit", type=int, default=120000)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--workers", type=int, default=15)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    if not os.environ.get("OPENROUTER_API_KEY"):
        print("\n  ERROR: OPENROUTER_API_KEY not set.")
        sys.exit(1)

    run_tier_b_with_dashboard(
        limit=args.limit,
        batch_size=args.batch_size,
        workers=args.workers,
        model=args.model,
    )
