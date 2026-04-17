"""
Import Reddit r/UFOs sighting reports into the unified database.

Reads the extracted CSV produced by extract_reddit.py (Pass 2) and
imports each row as a sighting + location in the standard schema.

Source: reddit_sightings_extracted.csv (produced by scrape_reddit.py +
extract_reddit.py from the r/UFOs community sighting spreadsheet).

This is source_db_id = 6 (Reddit-UFOs) in the source_database table.

Usage:
    python import_reddit.py                      # import all
    python import_reddit.py --stats-only         # print current import stats
"""
import csv
import json
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
DATA_DIR = os.environ.get(
    "UFOSINT_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw"),
)
CSV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data", "raw", "reddit", "reddit_sightings_extracted.csv",
)

BATCH_SIZE = 500
SOURCE_DB_ID = 6  # Reddit-UFOs — registered in create_schema.py


def run_import(db_path=DB_PATH, csv_path=CSV_PATH):
    """Import extracted Reddit sighting records into the unified database."""
    if not os.path.exists(csv_path):
        print(f"ERROR: extracted CSV not found at {csv_path}")
        print("  Run scrape_reddit.py then extract_reddit.py --merge first.")
        return

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # Verify source_db_id exists
    cur.execute("SELECT id FROM source_database WHERE name = 'Reddit-UFOs'")
    row = cur.fetchone()
    if not row:
        print("ERROR: 'Reddit-UFOs' not found in source_database table.")
        print("  Run create_schema.py first to seed the source tables.")
        return
    src_id = row[0]
    print(f"Source: Reddit-UFOs (id={src_id})")

    # Count existing Reddit imports (for idempotency check)
    cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id = ?", (src_id,))
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"  WARNING: {existing:,} Reddit sightings already imported.")
        print("  Delete them first if you want a fresh import:")
        print(f"    DELETE FROM sighting WHERE source_db_id = {src_id};")
        return

    # Read CSV
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Records in CSV: {len(rows):,}")

    imported = 0
    skipped = 0
    batch_locs = []
    batch_sightings = []

    for row in rows:
        post_id = row.get("post_id", "").strip()
        if not post_id:
            skipped += 1
            continue

        # Location
        city = row.get("city") or None
        state = row.get("state") or None
        country = row.get("country") or None
        lat = None
        lon = None
        try:
            lat = float(row["latitude"]) if row.get("latitude") else None
            lon = float(row["longitude"]) if row.get("longitude") else None
        except (ValueError, TypeError):
            pass

        raw_text = row.get("xlsx_location") or f"{city or ''}, {state or ''}, {country or ''}".strip(", ")

        cur.execute(
            "INSERT INTO location (raw_text, city, state, country, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (raw_text, city, state, country, lat, lon),
        )
        loc_id = cur.lastrowid

        # Parse duration_seconds
        dur_sec = None
        try:
            dur_sec = int(row["duration_seconds"]) if row.get("duration_seconds") else None
        except (ValueError, TypeError):
            pass

        # Parse witness/object counts
        num_witnesses = None
        num_objects = None
        try:
            num_witnesses = int(row["num_witnesses"]) if row.get("num_witnesses") else None
        except (ValueError, TypeError):
            pass
        try:
            num_objects = int(row["num_objects"]) if row.get("num_objects") else None
        except (ValueError, TypeError):
            pass

        # Media URLs go into raw_json
        media_urls = row.get("media_urls", "[]")
        raw_json = json.dumps({
            "reddit_post_id": post_id,
            "reddit_url": row.get("reddit_url", ""),
            "media_urls": json.loads(media_urls) if media_urls else [],
            "has_photo": row.get("has_photo", "").lower() == "true",
            "has_video": row.get("has_video", "").lower() == "true",
            "confidence": row.get("confidence", ""),
            "xlsx_date": row.get("xlsx_date", ""),
            "xlsx_location": row.get("xlsx_location", ""),
        })

        cur.execute(
            """INSERT INTO sighting (
                source_db_id, source_record_id,
                date_event, date_event_raw, time_raw, timezone,
                date_reported, location_id,
                summary, description,
                shape, color, duration, duration_seconds,
                num_objects, num_witnesses,
                sound, direction, elevation_angle,
                source_ref, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                src_id,
                post_id,
                row.get("date_event") or None,
                row.get("xlsx_date") or None,
                row.get("time_of_day") or None,
                row.get("timezone") or None,
                row.get("xlsx_submitted") or None,  # post submission time
                loc_id,
                None,  # summary — could use title but description covers it
                row.get("description") or None,
                row.get("shape") or None,
                row.get("color") or None,
                row.get("duration") or None,
                dur_sec,
                num_objects,
                num_witnesses,
                row.get("sound") or None,
                row.get("direction") or None,
                row.get("elevation") or None,
                row.get("reddit_url") or None,
                raw_json,
            ),
        )

        imported += 1

        if imported % BATCH_SIZE == 0:
            conn.commit()
            print(f"  ... {imported:,} rows imported", end="\r")

    conn.commit()

    print(f"\nReddit-UFOs import complete:")
    print(f"  Imported: {imported:,}")
    print(f"  Skipped: {skipped:,}")

    # Quick stats
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE source_db_id = ? AND description IS NOT NULL",
        (src_id,),
    )
    with_desc = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sighting s JOIN location l ON s.location_id = l.id "
        "WHERE s.source_db_id = ? AND l.latitude IS NOT NULL",
        (src_id,),
    )
    with_coords = cur.fetchone()[0]
    print(f"  With description: {with_desc:,}")
    print(f"  With coordinates: {with_coords:,}")

    conn.close()


if __name__ == "__main__":
    if "--stats-only" in sys.argv:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id = 6")
        print(f"Reddit-UFOs sightings: {cur.fetchone()[0]:,}")
        conn.close()
    else:
        run_import()
