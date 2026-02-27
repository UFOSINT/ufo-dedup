"""
Import MUFON CSV into the unified database.
~138K rows, 7 columns.
"""
import sqlite3
import csv
import json
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
CSV_PATH = os.path.join(os.path.dirname(__file__), "mufon.csv")

BATCH_SIZE = 5000


def parse_mufon_date(date_str):
    """Parse MUFON date format like '1992-08-19\\n5:45AM' into ISO."""
    if not date_str or not date_str.strip():
        return None, None

    parts = date_str.strip().split('\n')
    date_part = parts[0].strip() if parts else None
    time_part = parts[1].strip() if len(parts) > 1 else None

    # date_part should be like YYYY-MM-DD
    if date_part and re.match(r'\d{4}-\d{2}-\d{2}', date_part):
        iso = date_part
        if time_part:
            # Convert 12hr to 24hr
            t = time_part.upper().strip()
            m = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM)?', t)
            if m:
                h, mi, ampm = int(m.group(1)), m.group(2), m.group(3)
                if ampm == 'PM' and h != 12:
                    h += 12
                elif ampm == 'AM' and h == 12:
                    h = 0
                iso += f"T{h:02d}:{mi}"
        return iso, date_str.strip()

    return None, date_str.strip()


def parse_mufon_location(loc_str):
    """Parse MUFON location like 'Newscandia\\, MN\\, US'."""
    if not loc_str:
        return None, None, None
    # MUFON uses \, as escaped commas
    loc = loc_str.replace('\\,', ',').strip()
    parts = [p.strip() for p in loc.split(',')]

    city = parts[0] if len(parts) > 0 else None
    state = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None

    return city, state, country


def run_import():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    cur = conn.cursor()

    cur.execute("SELECT id FROM source_database WHERE name='MUFON'")
    source_db_id = cur.fetchone()[0]

    loc_cache = {}
    cur.execute("SELECT MAX(id) FROM location")
    next_loc_id = (cur.fetchone()[0] or 0) + 1

    loc_batch = []
    sighting_batch = []
    imported = 0

    print(f"Reading {CSV_PATH}...")
    with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse location
            raw_loc = (row.get('Location of Event', '') or '').strip()
            city, state, country = parse_mufon_location(raw_loc)

            loc_key = f"{raw_loc}|{city}|{state}|{country}"
            if loc_key not in loc_cache:
                loc_id = next_loc_id
                next_loc_id += 1
                loc_cache[loc_key] = loc_id
                loc_batch.append((
                    loc_id, raw_loc or None, city, None, state, country, None, None, None, None
                ))
            else:
                loc_id = loc_cache[loc_key]

            # Parse dates
            date_event, date_raw = parse_mufon_date(row.get('Date/Time of Event', ''))
            date_submitted = (row.get('Date Submitted', '') or '').strip() or None

            raw = {k: v for k, v in row.items() if v and v.strip()}

            sighting_batch.append((
                source_db_id,
                (row.get('No', '') or '').strip() or None,
                None, None,  # origin
                date_event,
                date_raw,
                None, None, None,  # date_end, time_raw, tz
                date_submitted,
                None,  # date_posted
                loc_id,
                (row.get('Short Description', '') or '').strip() or None,
                (row.get('Long Description', '') or '').strip() or None,
                None, None, None, None, None,  # shape, color, size, angular, distance
                None, None, None, None,  # duration, dur_sec, objects, witnesses
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
                print(f"  ... {imported:,} rows imported", end='\r')

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

    print(f"\nMUFON import complete: {imported:,} sightings, {len(loc_cache):,} unique locations")


if __name__ == "__main__":
    run_import()
