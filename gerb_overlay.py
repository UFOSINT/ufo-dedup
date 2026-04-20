"""
UAP Gerb overlay: crash-retrieval records, nuclear encounters, and facility
proximity computation.

Usage:
    python gerb_overlay.py                     # import + compute proximity
    python gerb_overlay.py --stats-only        # print coverage
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint rebuild --only replay
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sqlite3

from ufosint.processors.nuclear import (
    run_gerb_overlay,
    print_stats,
    denormalize_nrc,
    DB_PATH,
    BUNDLE_PATH,
)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="UAP Gerb overlay + NRC denormalization")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--bundle", default=BUNDLE_PATH)
    parser.add_argument("--stats-only", action="store_true")
    parser.add_argument("--nrc-only", action="store_true")
    args = parser.parse_args()

    if args.stats_only:
        print_stats(args.db)
    elif args.nrc_only:
        conn = sqlite3.connect(args.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        denormalize_nrc(conn)
        conn.close()
    else:
        run_gerb_overlay(args.db, args.bundle)
        print("\n[NRC] Denormalizing NRC word-counts to sighting columns...")
        conn = sqlite3.connect(args.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        denormalize_nrc(conn)
        conn.close()
