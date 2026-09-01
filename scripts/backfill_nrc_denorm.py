#!/usr/bin/env python3
"""Backfill sighting.nrc_* from sentiment_analysis.emo_*, and emit a CSV
that the sibling repo can apply to Postgres.

Why this exists
---------------
denormalize_nrc() was orphaned when its only caller (the root script
gerb_overlay.py) was deleted in 56c87c3, so the v0.16.4 rebuild left
sighting.nrc_* NULL for the whole corpus. The underlying data was never
lost — sentiment_analysis.emo_* has 461,551 scored rows, more than the
365,600 the pre-rebuild build had. Only the copy between them was missing.

v0.16.9 wires the call back into the pipeline, so a future rebuild is
correct. This script repairs the databases we already have without paying
for a full rebuild.

Postgres has no sentiment_analysis table, so it cannot do this copy
itself. Per CLAUDE.md the boundary-crossing artifact is a CSV keyed by
row id — sighting ids are copied verbatim across the SQLite/PG boundary,
so it loads cleanly.

Usage
-----
    python3 scripts/backfill_nrc_denorm.py            # dry run
    python3 scripts/backfill_nrc_denorm.py --apply    # mutate + write CSV
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ufosint.processors.nuclear import denormalize_nrc  # noqa: E402

NRC = ["joy", "fear", "anger", "sadness", "surprise", "disgust", "trust",
       "anticipation"]
NRC_COLS = [f"nrc_{e}" for e in NRC]

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNIFIED = os.path.join(HERE, "data", "output", "ufo_unified.db")
PUBLIC = os.path.join(HERE, "data", "output", "ufo_public.db")
CSV_OUT = os.path.join(HERE, "data", "output", "nrc_backfill.csv")


def _stats(conn):
    total = conn.execute("SELECT COUNT(*) FROM sighting").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM sighting WHERE nrc_fear IS NOT NULL"
    ).fetchone()[0]
    return total, filled


def backfill(path, apply_changes):
    """Unified DB: denormalize from its own sentiment_analysis table."""
    if not os.path.exists(path):
        print(f"  {path} not found — skipping")
        return None
    conn = sqlite3.connect(path)
    total, before = _stats(conn)
    src = conn.execute(
        "SELECT COUNT(*) FROM sentiment_analysis WHERE emo_joy IS NOT NULL"
    ).fetchone()[0]
    print(f"  {os.path.basename(path)}: {total:,} sightings, "
          f"{before:,} with NRC, {src:,} scored rows available")

    if not apply_changes:
        print("    dry run — no changes written")
        conn.close()
        return None

    denormalize_nrc(conn)
    _, after = _stats(conn)
    print(f"    populated: {before:,} -> {after:,}")
    conn.close()
    return after


def backfill_public(apply_changes):
    """Public DB: no sentiment_analysis table (the export strips it), so
    copy the already-denormalized values across from the unified DB.

    Ids are identical between the two files — the export preserves them —
    so this is a straight id join.
    """
    if not os.path.exists(PUBLIC):
        print(f"  {PUBLIC} not found — skipping")
        return None
    conn = sqlite3.connect(PUBLIC)
    total, before = _stats(conn)
    print(f"  {os.path.basename(PUBLIC)}: {total:,} sightings, "
          f"{before:,} with NRC")

    if not apply_changes:
        print("    dry run — no changes written")
        conn.close()
        return None

    conn.execute("ATTACH DATABASE ? AS src", (UNIFIED,))
    sets = ", ".join(
        f"{c} = (SELECT u.{c} FROM src.sighting u WHERE u.id = sighting.id)"
        for c in NRC_COLS
    )
    conn.execute(
        f"UPDATE sighting SET {sets} "
        f"WHERE id IN (SELECT id FROM src.sighting WHERE nrc_fear IS NOT NULL)"
    )
    conn.commit()
    conn.execute("DETACH DATABASE src")
    _, after = _stats(conn)
    print(f"    populated: {before:,} -> {after:,}")
    conn.close()
    return after


def write_csv(apply_changes):
    """Export the id-keyed artifact for the Postgres side."""
    if not apply_changes:
        print("  dry run — no CSV written")
        return 0
    conn = sqlite3.connect(UNIFIED)
    rows = conn.execute(
        f"SELECT id, {', '.join(NRC_COLS)} FROM sighting "
        f"WHERE nrc_fear IS NOT NULL ORDER BY id"
    )
    n = 0
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["id"] + NRC_COLS)
        for row in rows:
            w.writerow(row)
            n += 1
    conn.close()
    print(f"  wrote {n:,} rows -> {CSV_OUT}")
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="actually mutate the databases and write the CSV")
    args = ap.parse_args()

    if not args.apply:
        print("DRY RUN — pass --apply to write changes\n")

    print("Backfilling SQLite:")
    backfill(UNIFIED, args.apply)
    # The public export must be fixed too, or the next
    # reload_from_public_db.py run re-wipes production.
    backfill_public(args.apply)

    print("\nExporting CSV for Postgres:")
    write_csv(args.apply)


if __name__ == "__main__":
    main()
