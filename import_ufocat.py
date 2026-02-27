"""
Import UFOCAT 2023 CSV into the unified database.
UFOCAT is the richest structured dataset with 55 columns.

Skips records whose SOURCE field matches SKIP_SOURCES (e.g. 'UFOReportCtr'
= NUFORC-origin records). Those skipped records are saved to a sidecar
JSON file so their metadata (Hynek, Vallee, shape) can be transferred
to the canonical NUFORC records by enrich.py.
"""
import sqlite3
import csv
import json
import os
import sys
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
CSV_PATH = os.path.join(os.path.dirname(__file__), "UFOCAT", "ufocat2023.csv")
ENRICHMENT_PATH = os.path.join(os.path.dirname(__file__), "ufocat_enrichment.jsonl")

BATCH_SIZE = 5000

# Skip these UFOCAT sub-sources — already imported from richer originals.
# Same pattern as import_updb.py's SKIP_SOURCES.
SKIP_SOURCES = {'UFOReportCtr'}  # NUFORC-origin records (~123K)

def parse_ufocat_date(year, mo, day, time_str):
    """Try to build an ISO date from UFOCAT's split date fields."""
    try:
        y = int(year) if year and year.strip() else None
        m = int(mo) if mo and mo.strip() else None
        d = int(day) if day and day.strip() else None
    except (ValueError, TypeError):
        return None

    if y is None or y == 0:
        return None

    parts = [f"{y:04d}"]
    if m and 1 <= m <= 12:
        parts.append(f"{m:02d}")
        if d and 1 <= d <= 31:
            parts.append(f"{d:02d}")
        else:
            parts.append("01")
    else:
        parts.extend(["01", "01"])

    date_str = "-".join(parts)

    if time_str and time_str.strip():
        t = time_str.strip()
        # Try to parse HH:MM or HHMM formats
        t = t.replace(".", ":").replace(";", ":")
        if re.match(r'^\d{3,4}$', t):
            t = t.zfill(4)
            t = t[:2] + ":" + t[2:]
        if re.match(r'^\d{1,2}:\d{2}', t):
            date_str += "T" + t

    return date_str


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


def safe_float(val):
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        f = float(val)
        if f == 0.0:
            return None  # UFOCAT uses 0 for unknown
        return f
    except (ValueError, TypeError):
        return None


def run_import():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # Speed up bulk import
    cur = conn.cursor()

    # Get source_db_id for UFOCAT
    cur.execute("SELECT id FROM source_database WHERE name='UFOCAT'")
    source_db_id = cur.fetchone()[0]

    # Location cache: raw_text -> location_id
    loc_cache = {}
    cur.execute("SELECT MAX(id) FROM location")
    row = cur.fetchone()
    next_loc_id = (row[0] or 0) + 1

    loc_batch = []
    sighting_batch = []
    imported = 0
    skipped = 0

    print(f"Reading {CSV_PATH}...")
    print(f"Skipping sub-sources: {SKIP_SOURCES} (saving to {ENRICHMENT_PATH})")

    enrich_file = open(ENRICHMENT_PATH, 'w', encoding='utf-8')

    with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader):
            # Check if this record's SOURCE is in the skip list
            source_ref = (row.get('SOURCE', '') or '').strip()
            if source_ref in SKIP_SOURCES:
                # Save enrichment data for enrich.py to transfer metadata
                date_event = parse_ufocat_date(
                    row.get('YEAR'), row.get('MO'), row.get('DAY'), row.get('TIME')
                )
                enrich_record = {
                    'date': date_event,
                    'location': (row.get('LOCATION', '') or '').strip(),
                    'state': (row.get('STATE', '') or '').strip(),
                    'hynek': (row.get('HYNEK', '') or '').strip() or None,
                    'vallee': (row.get('VALLEE', '') or '').strip() or None,
                    'shape': (row.get('SHAPE', '') or '').strip() or None,
                    'source_ref': source_ref,
                    'urn': (row.get('URN', '') or '').strip() or None,
                }
                enrich_file.write(json.dumps(enrich_record, ensure_ascii=False) + '\n')
                skipped += 1
                if skipped % 50000 == 0:
                    print(f"  ... skipped {skipped:,} {source_ref} rows", end='\r')
                continue

            # Build location
            raw_loc = (row.get('LOCATION', '') or '').strip()
            state = (row.get('STATE', '') or '').strip()
            county = (row.get('COUNTY', '') or '').strip()
            region = (row.get('REGION', '') or '').strip()
            lat = safe_float(row.get('LATITUDE'))
            lon = safe_float(row.get('LONGITUDE'))

            loc_key = f"{raw_loc}|{state}|{county}|{region}|{lat}|{lon}"

            if loc_key not in loc_cache:
                loc_id = next_loc_id
                next_loc_id += 1
                loc_cache[loc_key] = loc_id
                loc_batch.append((
                    loc_id, raw_loc or None, None, county or None,
                    state or None, None, region or None,
                    lat, lon, None
                ))
            else:
                loc_id = loc_cache[loc_key]

            # Parse date
            date_event = parse_ufocat_date(
                row.get('YEAR'), row.get('MO'), row.get('DAY'), row.get('TIME')
            )
            date_raw = f"{row.get('YEAR','')}/{row.get('MO','')}/{row.get('DAY','')} {row.get('TIME','')}"

            # Build raw_json of all original fields
            raw = {k: v for k, v in row.items() if v and v.strip()}

            sighting_batch.append((
                source_db_id,
                (row.get('URN', '') or '').strip() or (row.get('PRN', '') or '').strip() or None,
                None,  # origin_id
                None,  # origin_record_id
                date_event,
                date_raw.strip(),
                None,  # date_end
                (row.get('TIME', '') or '').strip() or None,
                (row.get('TZONE', '') or '').strip() or (row.get('TZ', '') or '').strip() or None,
                None,  # date_reported
                None,  # date_posted
                loc_id,
                None,  # summary
                (row.get('NOTES', '') or '').strip() or None,
                (row.get('SHAPE', '') or '').strip() or None,
                (row.get('COLOR', '') or '').strip() or None,
                (row.get('SIZE', '') or '').strip() or None,
                (row.get('AGLSZE', '') or '').strip() or None,
                (row.get('DIST', '') or '').strip() or None,
                (row.get('DUR', '') or '').strip() or None,
                None,  # duration_seconds
                safe_int(row.get('OBJS')),
                safe_int(row.get('WITS')),
                (row.get('SOUND', '') or '').strip() or None,
                None,  # direction
                None,  # elevation_angle
                None,  # viewed_from
                (row.get('AGE', '') or '').strip() or None,
                (row.get('SEX', '') or '').strip() or None,
                (row.get('NAMES', '') or '').strip() or None,
                (row.get('HYNEK', '') or '').strip() or None,
                (row.get('VALLEE', '') or '').strip() or None,
                (row.get('TYPE', '') or '').strip() or None,
                (row.get('SVP', '') or '').strip() or None,
                (row.get('EXPLAN', '') or '').strip() or (row.get('EXPL', '') or '').strip() or None,
                None,  # characteristics
                (row.get('WEA', '') or '').strip() or None,
                (row.get('TER', '') or '').strip() or None,
                (row.get('SOURCE', '') or '').strip() or None,
                (row.get('PAGEVOL', '') or '').strip() or None,
                (row.get('MISC', '') or '').strip() or None,
                json.dumps(raw, ensure_ascii=False),
            ))

            imported += 1

            if len(sighting_batch) >= BATCH_SIZE:
                if loc_batch:
                    cur.executemany("""
                        INSERT INTO location (id, raw_text, city, county, state, country, region, latitude, longitude, geoname_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, loc_batch)
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
                    ) VALUES (
                        ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?
                    )
                """, sighting_batch)
                sighting_batch = []
                conn.commit()
                print(f"  ... {imported:,} rows imported", end='\r')

    # Final batch
    if loc_batch:
        cur.executemany("""
            INSERT INTO location (id, raw_text, city, county, state, country, region, latitude, longitude, geoname_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, loc_batch)
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
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?
            )
        """, sighting_batch)

    conn.commit()

    # Update record count
    cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id=?", (source_db_id,))
    count = cur.fetchone()[0]
    cur.execute("UPDATE source_database SET record_count=? WHERE id=?", (count, source_db_id))
    conn.commit()

    conn.execute("PRAGMA foreign_keys=ON")
    conn.close()
    enrich_file.close()

    print(f"\nUFOCAT import complete: {imported:,} sightings, {skipped:,} skipped ({', '.join(SKIP_SOURCES)})")
    print(f"  {len(loc_cache):,} unique locations")
    print(f"  Enrichment data saved to {ENRICHMENT_PATH}")


if __name__ == "__main__":
    run_import()
