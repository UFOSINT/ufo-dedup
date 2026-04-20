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
    """Import extracted Reddit sighting records per v0.13 schema contract."""
    if not os.path.exists(csv_path):
        print(f"ERROR: extracted CSV not found at {csv_path}")
        print("  Run extract_reddit.py --merge first.")
        return

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # Find the source_database ID by name (not hardcoded)
    cur.execute("SELECT id FROM source_database WHERE name = 'r/UFOs'")
    row = cur.fetchone()
    if not row:
        # Fall back to 'Reddit-UFOs' (our local seed name)
        cur.execute("SELECT id FROM source_database WHERE name = 'Reddit-UFOs'")
        row = cur.fetchone()
    if not row:
        print("ERROR: neither 'r/UFOs' nor 'Reddit-UFOs' found in source_database.")
        print("  Run create_schema.py or apply the v0.13 migration first.")
        return
    src_id = row[0]
    print(f"Source: source_db_id={src_id}")

    # Check for existing Reddit imports
    cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id = ?", (src_id,))
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"  {existing:,} Reddit sightings already exist.")
        print(f"  To re-import, first: DELETE FROM sighting WHERE source_db_id = {src_id};")
        return

    # Read CSV
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Records in CSV: {len(rows):,}")

    imported = 0
    skipped = 0

    for row in rows:
        post_id = (row.get("post_id") or "").strip()
        if not post_id:
            skipped += 1
            continue

        # --- Location ---
        city = row.get("city") or None
        state = row.get("state") or None
        country = row.get("country") or None
        lat = safe_float(row.get("latitude"))
        lon = safe_float(row.get("longitude"))
        raw_text = row.get("xlsx_location") or ""
        if not raw_text and city:
            parts = [p for p in [city, state, country] if p]
            raw_text = ", ".join(parts)

        cur.execute(
            "INSERT INTO location (raw_text, city, state, country, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (raw_text or None, city, state, country, lat, lon),
        )
        loc_id = cur.lastrowid

        # --- Sighting ---
        reddit_url = f"https://www.reddit.com/r/UFOs/comments/{post_id}/"

        # Normalize CHECK-constrained values
        llm_confidence = normalize_confidence(row.get("confidence"))
        llm_anomaly = normalize_anomaly(row.get("anomaly_assessment"))
        llm_strangeness = normalize_strangeness(row.get("strangeness_rating"))

        # Description is the LLM summary — NOT raw Reddit text (content policy)
        description = row.get("description") or None

        cur.execute(
            """INSERT INTO sighting (
                source_db_id, source_record_id,
                date_event, date_event_raw, time_raw,
                location_id,
                description,
                shape, color, duration, duration_seconds,
                num_objects, num_witnesses,
                sound, direction, elevation_angle,
                source_ref,
                has_photo, has_video,
                reddit_post_id, reddit_url,
                llm_confidence, llm_anomaly_assessment,
                llm_prosaic_candidate, llm_strangeness_rating,
                llm_model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                src_id,
                post_id,
                row.get("date_event") or None,
                row.get("xlsx_date") or None,
                row.get("time_of_day") or None,
                loc_id,
                description,
                row.get("shape") or None,
                row.get("color") or None,
                row.get("duration") or None,
                safe_int(row.get("duration_seconds")),
                safe_int(row.get("num_objects")),
                safe_int(row.get("num_witnesses")),
                row.get("sound") or None,
                row.get("direction") or None,
                row.get("elevation") or None,
                reddit_url,
                safe_bool_int(row.get("has_photo")),
                safe_bool_int(row.get("has_video")),
                post_id,       # reddit_post_id (UNIQUE)
                reddit_url,    # reddit_url
                llm_confidence,
                llm_anomaly,
                row.get("prosaic_candidate") or None,
                llm_strangeness,
                "google/gemini-2.0-flash-001",  # llm_model
            ),
        )

        imported += 1
        if imported % BATCH_SIZE == 0:
            conn.commit()
            sys.stdout.write(f"\r  {imported:,} imported...")
            sys.stdout.flush()

    conn.commit()

    print(f"\n\nr/UFOs import complete:")
    print(f"  Imported: {imported:,}")
    print(f"  Skipped: {skipped:,}")

    # Stats
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE source_db_id = ? AND description IS NOT NULL",
        (src_id,),
    )
    with_desc = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE source_db_id = ? AND llm_anomaly_assessment IS NOT NULL",
        (src_id,),
    )
    with_anomaly = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE source_db_id = ? AND llm_anomaly_assessment = 'anomalous'",
        (src_id,),
    )
    anomalous = cur.fetchone()[0]
    print(f"  With description: {with_desc:,}")
    print(f"  With anomaly assessment: {with_anomaly:,}")
    print(f"  Anomalous: {anomalous:,}")

    conn.close()


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
