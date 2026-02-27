"""
Import UPDB (phenomenAInon) CSV into the unified database.
~1.9M rows, 9 columns. Selectively skips rows whose 'name' is MUFON or NUFORC
since we already imported those from their richer original CSVs.
"""
import sqlite3
import csv
import json
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
CSV_PATH = os.path.join(os.path.dirname(__file__), "UPDB.app", "phenomenAInon_UPDB.csv")

BATCH_SIZE = 10000

# Skip these sources since we already imported them from their richer original files
SKIP_SOURCES = {'MUFON', 'NUFORC'}


def parse_updb_date(date_str):
    """Parse UPDB date like '1993-05-20 00:00:00'."""
    if not date_str or not date_str.strip():
        return None

    d = date_str.strip()
    # Already in ISO-ish format
    m = re.match(r'(\d{4}-\d{2}-\d{2})', d)
    if m:
        iso = m.group(1)
        # Add time if not 00:00:00
        time_m = re.search(r'(\d{2}:\d{2}:\d{2})', d)
        if time_m and time_m.group(1) != '00:00:00':
            iso += "T" + time_m.group(1)
        return iso
    return None


def run_import():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    cur = conn.cursor()

    cur.execute("SELECT id FROM source_database WHERE name='UPDB'")
    source_db_id = cur.fetchone()[0]

    # Build origin lookup
    cur.execute("SELECT id, name FROM source_origin")
    origin_map = {name: oid for oid, name in cur.fetchall()}

    loc_cache = {}
    cur.execute("SELECT MAX(id) FROM location")
    next_loc_id = (cur.fetchone()[0] or 0) + 1

    loc_batch = []
    sighting_batch = []
    imported = 0
    skipped = 0

    print(f"Reading {CSV_PATH}...")
    print(f"Skipping sources: {SKIP_SOURCES}")

    with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get('name', '') or '').strip()

            # Skip MUFON/NUFORC — already imported from richer originals
            if name in SKIP_SOURCES:
                skipped += 1
                if skipped % 100000 == 0:
                    print(f"  ... skipped {skipped:,} MUFON/NUFORC rows", end='\r')
                continue

            # Location
            city = (row.get('city', '') or '').strip()
            country = (row.get('country', '') or '').strip()
            raw_loc = f"{city}, {country}" if city else country

            loc_key = f"{city}|{country}"
            if loc_key not in loc_cache:
                loc_id = next_loc_id
                next_loc_id += 1
                loc_cache[loc_key] = loc_id
                loc_batch.append((
                    loc_id, raw_loc or None, city or None, None, None,
                    country or None, None, None, None, None
                ))
            else:
                loc_id = loc_cache[loc_key]

            # Date
            date_event = parse_updb_date(row.get('date', ''))
            date_raw = (row.get('date', '') or '').strip()

            # Origin
            origin_id = origin_map.get(name)

            raw = {k: v for k, v in row.items() if v and v.strip()}

            sighting_batch.append((
                source_db_id,
                (row.get('id', '') or '').strip() or None,
                origin_id,
                (row.get('source_id', '') or '').strip() or None,
                date_event,
                date_raw or None,
                None, None, None,  # date_end, time_raw, tz
                None, None,  # reported, posted
                loc_id,
                None,  # summary
                (row.get('description', '') or '').strip() or None,
                None, None, None, None, None,  # shape, color, size, angular, dist
                None, None, None, None,  # duration, dur_sec, objs, witnesses
                None, None, None, None,  # sound, direction, elev, viewed_from
                None, None, None,  # witness info
                None, None, None, None,  # classifications
                None, None,  # explanation, characteristics
                None, None,  # weather, terrain
                None, None,  # source_ref, page_vol
                None,  # notes
                json.dumps(raw, ensure_ascii=False),
            ))

            imported += 1

            if len(sighting_batch) >= BATCH_SIZE:
                if loc_batch:
                    cur.executemany(
                        "INSERT INTO location (id, raw_text, city, county, state, country, region, latitude, longitude, geoname_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        loc_batch
                    )
                    loc_batch = []
                cur.executemany("""
                    INSERT INTO sighting (
                        source_db_id, source_record_id, origin_id, origin_record_id,
                        date_event, date_event_raw, date_end, time_raw, timezone,
                        date_reported, date_posted, location_id,
                        summary, description,
                        shape, color, size_estimated, angular_size, distance,
                        duration, duration_seconds, num_objects, num_witnesses,
                        sound, direction, elevation_angle, viewed_from,
                        witness_age, witness_sex, witness_names,
                        hynek, vallee, event_type, svp_rating,
                        explanation, characteristics,
                        weather, terrain,
                        source_ref, page_volume,
                        notes, raw_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, sighting_batch)
                sighting_batch = []
                conn.commit()
                print(f"  ... {imported:,} imported, {skipped:,} skipped", end='\r')

    # Final batch
    if loc_batch:
        cur.executemany(
            "INSERT INTO location (id, raw_text, city, county, state, country, region, latitude, longitude, geoname_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            loc_batch
        )
    if sighting_batch:
        cur.executemany("""
            INSERT INTO sighting (
                source_db_id, source_record_id, origin_id, origin_record_id,
                date_event, date_event_raw, date_end, time_raw, timezone,
                date_reported, date_posted, location_id,
                summary, description,
                shape, color, size_estimated, angular_size, distance,
                duration, duration_seconds, num_objects, num_witnesses,
                sound, direction, elevation_angle, viewed_from,
                witness_age, witness_sex, witness_names,
                hynek, vallee, event_type, svp_rating,
                explanation, characteristics,
                weather, terrain,
                source_ref, page_volume,
                notes, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, sighting_batch)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id=?", (source_db_id,))
    count = cur.fetchone()[0]
    cur.execute("UPDATE source_database SET record_count=? WHERE id=?", (count, source_db_id))
    conn.commit()
    conn.close()

    print(f"\nUPDB import complete: {imported:,} sightings imported, {skipped:,} MUFON/NUFORC skipped")
    print(f"  {len(loc_cache):,} unique locations")


if __name__ == "__main__":
    run_import()
