"""
Import Reddit r/UFOs sighting reports into the unified database.

Reads the extracted CSV produced by extract_reddit.py (Pass 2) and
imports each row as a sighting + location in the standard schema,
following the v0.13 schema contract defined in:
  ufosint-explorer/docs/REDDIT_INGEST_NOTES.md

Content policy: we store LLM-derivative fields + Reddit permalink.
We do NOT store raw Reddit content (selftext, title, author, comments,
engagement metrics). See the ingest notes for the full policy.

Source: reddit_sightings_extracted.csv
Source DB: 'r/UFOs' in source_database (seeded by v0.13 migration)

Usage:
    python import_reddit.py                      # import all
    python import_reddit.py --stats-only         # print current import stats
    python import_reddit.py --db PATH            # custom DB path
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint import reddit
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import csv
import json
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
CSV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data", "raw", "reddit", "reddit_sightings_extracted.csv",
)

BATCH_SIZE = 500

# ============================================================
# Value normalization — match v0.13 CHECK constraints exactly
# ============================================================

CONFIDENCE_MAP = {
    "high": "high",
    "medium": "medium",
    "low": "low",
    "none": None,
    "": None,
}

ANOMALY_MAP = {
    "anomalous": "anomalous",
    "likely_prosaic": "prosaic",       # normalize
    "prosaic": "prosaic",
    "insufficient_data": "ambiguous",  # normalize
    "ambiguous": "ambiguous",
    "none": None,
    "": None,
}


def normalize_confidence(val):
    if val is None:
        return None
    return CONFIDENCE_MAP.get(str(val).lower().strip(), None)


def normalize_anomaly(val):
    if val is None:
        return None
    return ANOMALY_MAP.get(str(val).lower().strip(), None)


def normalize_strangeness(val):
    if val is None:
        return None
    try:
        v = int(val)
        return v if 1 <= v <= 5 else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_bool_int(val):
    """Convert True/False/'Yes'/'No'/1/0 to 1 or 0."""
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    s = str(val).lower().strip()
    if s in ("true", "yes", "1"):
        return 1
    if s in ("false", "no", "0"):
        return 0
    return None


# ============================================================
# Main import
# ============================================================

def run_import(db_path=DB_PATH, csv_path=CSV_PATH):
    """Delegate to the ufosint.importers.reddit package module."""
    from ufosint.importers.reddit import RedditImporter
    from ufosint.db import Database
    RedditImporter().run(Database(db_path))


def print_stats(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Try both possible source names
    for name in ["r/UFOs", "Reddit-UFOs"]:
        cur.execute("SELECT id FROM source_database WHERE name = ?", (name,))
        row = cur.fetchone()
        if row:
            src_id = row[0]
            break
    else:
        print("No Reddit source found in DB.")
        conn.close()
        return

    cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id = ?", (src_id,))
    total = cur.fetchone()[0]
    print(f"Reddit-UFOs sightings: {total:,}")
    if total > 0:
        cur.execute(
            "SELECT llm_anomaly_assessment, COUNT(*) FROM sighting "
            "WHERE source_db_id = ? GROUP BY 1 ORDER BY 2 DESC",
            (src_id,),
        )
        print("  Anomaly assessment:")
        for aa, n in cur.fetchall():
            print(f"    {str(aa):<12} {n:>5,}")
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import Reddit r/UFOs sightings")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--csv", default=CSV_PATH)
    parser.add_argument("--stats-only", action="store_true")
    args = parser.parse_args()

    if args.stats_only:
        print_stats(args.db)
    else:
        run_import(args.db, args.csv)
