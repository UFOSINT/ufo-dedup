"""
Import NUFORC CSV into the unified database.
~159K rows, 18 columns.
"""
import sqlite3
import csv
import json
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
CSV_PATH = os.path.join(os.path.dirname(__file__), "nuforc.csv")

BATCH_SIZE = 5000


def safe_str(val):
    """Safely get a string from a value that might be a list (CSV parsing artifact)."""
    if val is None:
        return ''
    if isinstance(val, list):
        return ', '.join(str(x) for x in val if x)
    return str(val)


def parse_nuforc_date(date_str):
    """Parse NUFORC date like ' 1995-02-02 23:00 Local' into ISO."""
    if not date_str or not date_str.strip():
        return None, None

    raw = date_str.strip()
    # Extract timezone hint
    tz = None
    for tzname in ['Local', 'Pacific', 'Eastern', 'Central', 'Mountain', 'UTC', 'GMT']:
        if tzname in raw:
            tz = tzname
            raw = raw.replace(tzname, '').strip()

    m = re.match(r'(\d{4}-\d{2}-\d{2})\s*(\d{2}:\d{2})?', raw)
    if m:
        iso = m.group(1)
        if m.group(2):
            iso += "T" + m.group(2)
        return iso, date_str.strip()

    return None, date_str.strip()


def parse_nuforc_location(loc_str):
    """Parse NUFORC location like ' Shady Grove, OR, USA'."""
    if not loc_str or not loc_str.strip():
        return None, None, None

    parts = [p.strip() for p in loc_str.strip().split(',')]
    city = parts[0] if len(parts) > 0 else None
    state = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None
    return city, state, country


def safe_int(val):
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def run_import():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    cur = conn.cursor()

    cur.execute("SELECT id FROM source_database WHERE name='NUFORC'")
    source_db_id = cur.fetchone()[0]

    loc_cache = {}
    cur.execute("SELECT MAX(id) FROM location")
    next_loc_id = (cur.fetchone()[0] or 0) + 1

    loc_batch = []
    sighting_batch = []
    imported = 0

    print(f"Reading {CSV_PATH}...")
    # NUFORC CSV can have multi-line descriptions, use proper quoting
    with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse location
            raw_loc = safe_str(row.get('Location', '')).strip()
            city, state, country = parse_nuforc_location(raw_loc)

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
            date_event, date_raw = parse_nuforc_date(safe_str(row.get('Occurred', '')))
            date_reported_iso, _ = parse_nuforc_date(safe_str(row.get('Reported', '')))
            date_posted = safe_str(row.get('Posted', '')).strip() or None

            raw = {}
            for k, v in row.items():
                if v is None:
                    continue
                if isinstance(v, list):
                    v = ', '.join(str(x) for x in v if x)
                if isinstance(v, str) and v.strip():
                    raw[k] = v.strip()

            sighting_batch.append((
                source_db_id,
                safe_str(row.get('No', '')).strip() or None,
                None, None,  # origin
                date_event,
                date_raw,
                None, None, None,  # date_end, time_raw, tz
                date_reported_iso,
                date_posted,
                loc_id,
                None,  # summary
                safe_str(row.get('Description', '')).strip() or None,
                safe_str(row.get('Shape', '')).strip() or None,
                safe_str(row.get('Color', '')).strip() or None,
                safe_str(row.get('Estimated Size', '')).strip() or None,
                None,  # angular_size
                None,  # distance
                safe_str(row.get('Duration', '')).strip() or None,
                None,  # duration_seconds
                None,  # num_objects
                safe_int(safe_str(row.get('No of observers', ''))),
                None,  # sound
                safe_str(row.get(' Direction from Viewer', '') or row.get('Direction from Viewer', '')).strip() or None,
                safe_str(row.get(' Angle of Elevation', '') or row.get('Angle of Elevation', '')).strip() or None,
                safe_str(row.get(' Viewed from', '') or row.get('Viewed from', '')).strip() or None,
                None, None, None,  # witness info
                None, None, None, None,  # classifications
                safe_str(row.get('Explanation', '')).strip() or None,
                safe_str(row.get('Characteristics', '')).strip() or None,
                None, None,  # weather, terrain
                None, None,  # source_ref, page_vol
                safe_str(row.get('note', '')).strip() or None,
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

    print(f"\nNUFORC import complete: {imported:,} sightings, {len(loc_cache):,} unique locations")


if __name__ == "__main__":
    run_import()
