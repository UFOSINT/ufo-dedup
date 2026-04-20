"""
Clean public export: build a derived-only SQLite from the private analysis DB.

This is the final stage of the v0.9 pipeline architecture:

    [Raw Vault]  →  rebuild_db.py  →  [Analysis DB]  →  analyze.py  →
        [Analysis DB + derived fields]  →  export_public.py  →
            [Public SQLite]  →  migrate_sqlite_to_pg.py  →  [Azure PG]

The private analysis DB at `data/output/ufo_unified.db` stays on this
machine forever — it's the only remaining copy of the raw narrative text
after the public cutover. The public export at `data/output/ufo_public.db`
is safe to ship anywhere: it contains only derived, non-copyrighted,
science-team-approved fields.

The export strategy is:
  1. File-copy the private DB to the target path.
  2. On the copy, DROP the raw-text columns from `sighting`.
  3. DROP tables that are private-only (sentiment_analysis, duplicate_candidate,
     reference, sighting_reference, attachment).
  4. Optionally DROP location.raw_text (preserved by default — it's short
     and useful for display).
  5. VACUUM to reclaim disk.

The script is idempotent: re-running on an existing ufo_public.db is a no-op
for already-dropped columns and rebuilds the file from source.

Usage:
    python export_public.py                              # default paths
    python export_public.py --source ... --target ...   # custom paths
    python export_public.py --drop-location-raw-text    # also strip raw_text

Defaults:
    --source ../data/output/ufo_unified.db
    --target ../data/output/ufo_public.db
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint export public
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import argparse
import os
import sqlite3
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SOURCE = os.path.join(BASE_DIR, "data", "output", "ufo_unified.db")
DEFAULT_TARGET = os.path.join(BASE_DIR, "data", "output", "ufo_public.db")


# Columns to drop from `sighting` on the public copy. Keep in sync with
# ufosint-explorer/scripts/drop_raw_text_columns.sql and with the
# RAW_COLUMNS list in ufosint-explorer/scripts/strip_raw_for_public.py.
#
# Note: per the v0.8.3 decision recorded in strip_raw_for_public.py, we do
# NOT drop date_event_raw or time_raw here — the operator chose to keep
# them as structured-adjacent display fields. The surviving 4 columns are
# the true narrative blobs.
RAW_COLUMNS_TO_DROP = [
    "description",
    "summary",
    "notes",
    "raw_json",
]

# Other free-text fields the science team may want to cleanup later
# (v0.8.4+ per the backlog). Preserved by default; enable with
# --drop-optional-free-text when the dev team signs off.
OPTIONAL_FREE_TEXT_COLUMNS = [
    "witness_names",
    "explanation",
    "characteristics",
    "weather",
    "terrain",
]

# Tables that SHOULD appear in the public export. Everything else — whether
# it's a private-only table like sentiment_analysis or a stray temp table
# an operator left behind — gets dropped. Allowlist by design: new tables
# default to private unless explicitly added here, so the public DB can't
# accidentally leak internal working state.
#
# Tables intentionally NOT in this list:
#   sentiment_analysis:    raw VADER/NRC scores. The derived fields
#                          (sighting.sentiment_score, dominant_emotion) are
#                          already denormalized onto sighting.
#   duplicate_candidate:   internal dedup metadata, not useful to consumers.
#   reference, sighting_reference: citation text. Text blobs.
#   attachment:            url/file_type/description — empty in practice,
#                          excluded in case it gets populated with text later.
#   qs_snapshot_v082:      an internal delta-report working table created
#                          during the v0.8.3 cutover. Not for consumers.
#   sqlite_sequence:       SQLite internal, kept automatically.
PUBLIC_TABLES = {
    "sighting",
    "sighting_analysis",
    "location",
    "source_collection",
    "source_database",
    "source_origin",
    # UAP Gerb overlay tables (curated crash/nuclear/facility data)
    "crash_retrieval",
    "nuclear_encounter",
    "facility",
}


# ============================================================
# Helpers
# ============================================================

def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def existing_columns(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def existing_tables(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cur.fetchall()}


def sqlite_backup(src_path, dst_path):
    """Use sqlite3.backup to copy the DB — handles WAL/locks safely."""
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()


# ============================================================
# Pre-flight
# ============================================================

def check_source(source_path):
    """Sanity-check the source DB: has the derived columns, is analyzed."""
    if not os.path.exists(source_path):
        sys.exit(f"ERROR: source DB not found at {source_path}")

    conn = sqlite3.connect(source_path)
    try:
        sighting_cols = existing_columns(conn, "sighting")
        required = {"quality_score", "standardized_shape", "has_movement_mentioned"}
        missing = required - sighting_cols
        if missing:
            sys.exit(
                f"ERROR: source DB is missing derived columns {sorted(missing)}. "
                f"Run rebuild_db.py (which runs analyze.py as the final step) "
                f"before exporting."
            )

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sighting")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sighting WHERE quality_score IS NOT NULL")
        scored = cur.fetchone()[0]
        if total == 0:
            sys.exit(f"ERROR: source DB sighting table is empty: {source_path}")
        coverage = scored / total
        if coverage < 0.95:
            print(
                f"WARNING: quality_score coverage is {coverage*100:.1f}% "
                f"({scored:,}/{total:,}). Expected ~100% after analyze.py. "
                f"Proceeding anyway — inspect the source DB if unexpected."
            )

        return total
    finally:
        conn.close()


# ============================================================
# Strip logic (runs on the target copy, not the source)
# ============================================================

def strip_sighting_columns(conn, extra_optional=False):
    """Strip raw text from `sighting` on the public copy.

    For `description` and `summary`: instead of dropping the column
    entirely, NULL out rows from legacy sources and KEEP the column
    for sources that have LLM-generated content (Reddit). This way
    the public DB popup can show a narrative for Reddit sightings
    without exposing raw user-submitted text from legacy sources.

    For `notes` and `raw_json`: dropped entirely (no safe content).

    Returns a list of columns actually dropped or scrubbed.
    """
    existing = existing_columns(conn, "sighting")
    cur = conn.cursor()

    actions = []

    # description + summary: NULL out legacy rows, keep column for LLM content
    for col in ["description", "summary"]:
        if col in existing:
            # Find source IDs that have LLM-generated descriptions (Reddit)
            cur.execute(
                f"SELECT COUNT(*) FROM sighting "
                f"WHERE {col} IS NOT NULL AND reddit_post_id IS NOT NULL"
            )
            llm_rows = cur.fetchone()[0]

            if llm_rows > 0:
                # NULL out legacy rows, keep LLM-generated rows
                cur.execute(
                    f"UPDATE sighting SET {col} = NULL "
                    f"WHERE reddit_post_id IS NULL"
                )
                actions.append(f"{col} (NULLed legacy, kept {llm_rows:,} LLM rows)")
            else:
                # No LLM content — drop entirely
                cur.execute(f"ALTER TABLE sighting DROP COLUMN {col}")
                actions.append(f"{col} (dropped)")

    # notes + raw_json: always drop entirely (never safe for public)
    for col in ["notes", "raw_json"]:
        if col in existing:
            cur.execute(f"ALTER TABLE sighting DROP COLUMN {col}")
            actions.append(f"{col} (dropped)")

    if extra_optional:
        for col in OPTIONAL_FREE_TEXT_COLUMNS:
            if col in existing:
                cur.execute(f"ALTER TABLE sighting DROP COLUMN {col}")
                actions.append(f"{col} (dropped)")

    conn.commit()
    return actions


def drop_private_tables(conn):
    """Drop every table not in the PUBLIC_TABLES allowlist.

    Allowlist approach: tables default to private unless explicitly added
    to PUBLIC_TABLES. sqlite_* internal tables are preserved automatically
    because SQLite manages them.
    """
    existing = existing_tables(conn)
    # Everything that isn't in PUBLIC_TABLES and isn't a sqlite internal
    to_drop = [
        t for t in existing
        if t not in PUBLIC_TABLES and not t.startswith("sqlite_")
    ]
    cur = conn.cursor()
    for t in to_drop:
        cur.execute(f"DROP TABLE {t}")
    conn.commit()
    return to_drop


def drop_location_raw_text(conn):
    """Optional: strip location.raw_text (long-form address lines)."""
    if "raw_text" in existing_columns(conn, "location"):
        conn.cursor().execute("ALTER TABLE location DROP COLUMN raw_text")
        conn.commit()
        return True
    return False


def vacuum(conn):
    """Reclaim disk space after the drops. Not inside a transaction."""
    # VACUUM can't run inside an explicit transaction, but auto-commit
    # mode is the default for a fresh sqlite3 connection with no BEGIN.
    conn.isolation_level = None
    conn.execute("VACUUM")


# ============================================================
# Main
# ============================================================

def main():
    """Delegate to the ufosint.export.public_db package module."""
    from ufosint.export.public_db import run_export

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default=DEFAULT_SOURCE,
                        help=f"Source analysis DB (default: {DEFAULT_SOURCE})")
    parser.add_argument("--target", default=DEFAULT_TARGET,
                        help=f"Target public DB (default: {DEFAULT_TARGET})")
    parser.add_argument("--drop-location-raw-text", action="store_true",
                        help="Also drop location.raw_text (long-form address lines)")
    parser.add_argument("--drop-optional-free-text", action="store_true",
                        help=f"Also drop {', '.join(OPTIONAL_FREE_TEXT_COLUMNS)} from sighting")
    parser.add_argument("--keep-target", action="store_true",
                        help="Refuse to run if target already exists (safety flag for scripts)")
    args = parser.parse_args()

    if args.keep_target and os.path.exists(os.path.abspath(args.target)):
        sys.exit(f"ERROR: target exists and --keep-target was passed: {args.target}")

    return run_export(
        source=args.source,
        target=args.target,
        drop_raw_text=args.drop_location_raw_text,
        drop_optional=args.drop_optional_free_text,
    )


if __name__ == "__main__":
    sys.exit(main())
