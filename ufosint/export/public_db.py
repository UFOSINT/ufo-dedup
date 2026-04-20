"""
Clean public export: build a derived-only SQLite from the private analysis DB.

Copies the analysis DB, strips raw-text columns, drops private tables,
and VACUUMs. The result is safe to ship anywhere — no copyrighted
narrative text, only derived/science-team-approved fields.

Usage (from package):
    from ufosint.export.public_db import run_export
    run_export(source_path, target_path)

Usage (CLI):
    ufosint export public
"""

import os
import sqlite3
import sys
import time

from ufosint.config import Config


# Columns to drop from `sighting` on the public copy.
RAW_COLUMNS_TO_DROP = [
    "description",
    "summary",
    "notes",
    "raw_json",
]

OPTIONAL_FREE_TEXT_COLUMNS = [
    "witness_names",
    "explanation",
    "characteristics",
    "weather",
    "terrain",
]

# Tables that SHOULD appear in the public export. Everything else gets dropped.
PUBLIC_TABLES = {
    "sighting",
    "sighting_analysis",
    "location",
    "source_collection",
    "source_database",
    "source_origin",
    # UAP Gerb overlay tables
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
    """Strip raw text from `sighting` on the public copy."""
    existing = existing_columns(conn, "sighting")
    cur = conn.cursor()

    actions = []

    # description + summary: NULL out legacy rows, keep LLM content
    for col in ["description", "summary"]:
        if col in existing:
            cur.execute(
                f"SELECT COUNT(*) FROM sighting "
                f"WHERE {col} IS NOT NULL AND reddit_post_id IS NOT NULL"
            )
            llm_rows = cur.fetchone()[0]

            if llm_rows > 0:
                cur.execute(
                    f"UPDATE sighting SET {col} = NULL "
                    f"WHERE reddit_post_id IS NULL"
                )
                actions.append(f"{col} (NULLed legacy, kept {llm_rows:,} LLM rows)")
            else:
                cur.execute(f"ALTER TABLE sighting DROP COLUMN {col}")
                actions.append(f"{col} (dropped)")

    # notes + raw_json: always drop entirely
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
    """Drop every table not in the PUBLIC_TABLES allowlist."""
    existing = existing_tables(conn)
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
    """Optional: strip location.raw_text."""
    if "raw_text" in existing_columns(conn, "location"):
        conn.cursor().execute("ALTER TABLE location DROP COLUMN raw_text")
        conn.commit()
        return True
    return False


def vacuum(conn):
    """Reclaim disk space after the drops."""
    conn.isolation_level = None
    conn.execute("VACUUM")


# ============================================================
# Main export function
# ============================================================

def run_export(source=None, target=None,
               drop_raw_text=False, drop_optional=False):
    """Run the public DB export.

    Args:
        source: path to analysis DB (default: Config.db_path())
        target: path for public DB (default: Config.public_db_path())
        drop_raw_text: also drop location.raw_text
        drop_optional: also drop optional free-text columns
    """
    source = source or Config.db_path()
    target = target or Config.public_db_path()

    source = os.path.abspath(source)
    target = os.path.abspath(target)

    if source == target:
        sys.exit("ERROR: source and target must differ.")

    print(f"Source: {source}")
    print(f"Target: {target}")
    print()

    total = check_source(source)
    src_size = os.path.getsize(source)
    print(f"Source DB is valid. {total:,} sightings, {fmt_bytes(src_size)}.")

    # 1. Copy
    if os.path.exists(target):
        print(f"Removing existing target ({fmt_bytes(os.path.getsize(target))})...")
        os.remove(target)
    print("Copying source -> target via sqlite3.backup...")
    t0 = time.perf_counter()
    sqlite_backup(source, target)
    print(f"  done in {time.perf_counter() - t0:.1f}s")

    # 2. Strip
    conn = sqlite3.connect(target)
    try:
        print()
        print("Stripping raw text columns from sighting...")
        dropped_cols = strip_sighting_columns(conn, extra_optional=drop_optional)
        if dropped_cols:
            for col in dropped_cols:
                print(f"    DROP COLUMN {col}")
        else:
            print("    (nothing to drop)")

        print()
        print("Dropping private-only tables...")
        dropped_tables = drop_private_tables(conn)
        if dropped_tables:
            for t in dropped_tables:
                print(f"    DROP TABLE {t}")
        else:
            print("    (nothing to drop)")

        if drop_raw_text:
            print()
            print("Dropping location.raw_text...")
            if drop_location_raw_text(conn):
                print("    DROP COLUMN raw_text")
            else:
                print("    (already gone)")

        print()
        print("Running VACUUM to reclaim disk...")
        t0 = time.perf_counter()
        vacuum(conn)
        print(f"  done in {time.perf_counter() - t0:.1f}s")
    finally:
        conn.close()

    # 3. Report
    tgt_size = os.path.getsize(target)
    delta = src_size - tgt_size
    pct = 100 * delta / src_size if src_size else 0
    print()
    print("=" * 60)
    print("  EXPORT COMPLETE")
    print("=" * 60)
    print(f"  Source:  {fmt_bytes(src_size):>12}")
    print(f"  Public:  {fmt_bytes(tgt_size):>12}")
    print(f"  Delta:   {fmt_bytes(delta):>12}  ({pct:.1f}% reclaimed)")
    print()
    print(f"  Public DB: {target}")
    print("  Ready for migrate_sqlite_to_pg.py -> Azure Postgres.")
    return 0
