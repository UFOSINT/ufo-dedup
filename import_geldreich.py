"""
Import UFO-search (formerly Geldreich) Majestic Timeline JSON into the unified database.
~54.7K records from 19 historical source compilations.
Source: ufo-search.com
"""
import sqlite3
import json
import os
import re
import hashlib

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
JSON_PATH = os.path.join(os.path.dirname(__file__), "Geldreich", "majestic.json")

BATCH_SIZE = 5000


def parse_geldreich_date(date_str, time_str=None):
    """
    Parse Geldreich's varied date formats:
      "0's", "4/34", "4/4/34", "5/21/70", "1947", "6/24/1947", "Summer 1947"
    Returns (iso_date_or_none, raw_string)
    """
    if not date_str or not date_str.strip():
        return None, None

    raw = date_str.strip()
    d = raw

    # Remove leading/trailing whitespace
    d = d.strip()

    # Handle "Summer 1947", "Fall 1952", etc.
    season_match = re.match(r'(Spring|Summer|Fall|Winter|Early|Late|Mid|End of|Beginning of)\s+(\d{4})', d, re.I)
    if season_match:
        return f"{season_match.group(2)}-01-01", raw

    # Handle just a year like "1947" or "0's"
    year_match = re.match(r"^(\d{1,4})'?s?$", d)
    if year_match:
        y = int(year_match.group(1))
        if y > 0:
            return f"{y:04d}-01-01", raw
        return None, raw

    # Handle M/D/YYYY or M/YYYY or M/D/YY
    slash_match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{1,4})$', d)
    if slash_match:
        a, b, c = int(slash_match.group(1)), int(slash_match.group(2)), int(slash_match.group(3))
        # Determine if M/D/Y
        if c < 100:
            c = c + 1900 if c > 25 else c + 2000
        return f"{c:04d}-{a:02d}-{b:02d}", raw

    # Handle M/YYYY like "4/34" meaning April year 34
    slash2 = re.match(r'^(\d{1,2})/(\d{1,4})$', d)
    if slash2:
        m, y = int(slash2.group(1)), int(slash2.group(2))
        if y < 100:
            y = y + 1900 if y > 25 else y + 2000
        if 1 <= m <= 12:
            return f"{y:04d}-{m:02d}-01", raw

    # Handle YYYY-MM-DD already
    iso_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})', d)
    if iso_match:
        return d[:10], raw

    # Handle plain 4-digit year
    plain_year = re.match(r'^(\d{4})$', d)
    if plain_year:
        return f"{d}-01-01", raw

    return None, raw


def run_import():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    cur = conn.cursor()

    cur.execute("SELECT id FROM source_database WHERE name='UFO-search'")
    source_db_id = cur.fetchone()[0]

    # Build origin lookup
    cur.execute("SELECT id, name FROM source_origin")
    origin_map = {name: oid for oid, name in cur.fetchall()}

    loc_cache = {}
    cur.execute("SELECT MAX(id) FROM location")
    next_loc_id = (cur.fetchone()[0] or 0) + 1

    # Load JSON
    print(f"Reading {JSON_PATH}...")
    with open(JSON_PATH, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)

    items = data.get("Majestic Timeline", [])
    print(f"  {len(items):,} records found")

    loc_batch = []
    sighting_batch = []
    ref_batch = []
    imported = 0

    for item in items:
        # Location
        raw_loc = item.get('location', '')
        if isinstance(raw_loc, list):
            raw_loc = ', '.join(raw_loc)
        raw_loc = (raw_loc or '').strip()

        loc_key = raw_loc or '__EMPTY__'
        if loc_key not in loc_cache:
            loc_id = next_loc_id
            next_loc_id += 1
            loc_cache[loc_key] = loc_id
            # Try to parse country from end (e.g., "ITALY, ROME" or "China")
            loc_batch.append((
                loc_id, raw_loc or None, None, None, None, None, None, None, None, None
            ))
        else:
            loc_id = loc_cache[loc_key]

        # Date
        date_event, date_raw = parse_geldreich_date(item.get('date'), item.get('time'))

        # End date
        end_date, _ = parse_geldreich_date(item.get('end_date'))

        # Source origin
        source_name = item.get('source', '')
        origin_id = origin_map.get(source_name)

        # Event type
        event_type = item.get('type', '')
        if isinstance(event_type, list):
            event_type = ', '.join(event_type)

        # Reference
        ref = item.get('ref', '')
        if isinstance(ref, list):
            ref = '\n'.join(ref)

        # Attributes
        attrs = item.get('attributes', '')
        if isinstance(attrs, list):
            attrs = ', '.join(attrs)

        # Build raw JSON (excluding search field to save space)
        raw = {k: v for k, v in item.items() if k != 'search'}

        sighting_batch.append((
            source_db_id,
            item.get('source_id', None),
            origin_id,
            None,  # origin_record_id
            date_event,
            date_raw,
            end_date,
            item.get('time') or None,
            None,  # timezone
            None, None,  # reported, posted
            loc_id,
            None,  # summary
            item.get('desc', '') or None,
            None, None, None, None, None,  # shape, color, size, angular, dist
            None, None, None, None,  # duration, dur_sec, objs, witnesses
            None, None, None, None,  # sound, direction, elev, viewed_from
            None, None, None,  # witness info
            None, None,  # hynek, vallee
            event_type or None,
            None,  # svp
            None,  # explanation
            attrs or None,  # characteristics (storing attributes here)
            None, None,  # weather, terrain
            ref or None,  # source_ref
            None,  # page_vol
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

    print(f"\nUFO-search import complete: {imported:,} sightings, {len(loc_cache):,} unique locations")


if __name__ == "__main__":
    run_import()
