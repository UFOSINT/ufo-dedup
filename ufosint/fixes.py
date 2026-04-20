"""
Post-import data quality fixes for the unified UFO sightings database.

Contains apply_data_fixes() and copy_to_explorer(), migrated from the
root-level rebuild_db.py script.
"""
import os
import sqlite3

from ufosint.config import Config

# US states + Canadian provinces for longitude fix
US_CA_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU', 'AS', 'MP',
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
}


def apply_data_fixes(db_path=None):
    """Apply post-import data quality fixes."""
    if db_path is None:
        db_path = Config.db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fix 1a: UFOCAT longitude sign (US/CA locations with positive longitude)
    # UFOCAT stored ALL longitudes with inverted signs. US/CA should be negative.
    print("  Fixing UFOCAT longitude signs (US/CA -> negative)...")
    state_list = ','.join(f"'{s}'" for s in US_CA_STATES)
    cur.execute(f"""
        UPDATE location SET longitude = -longitude
        WHERE longitude > 0
        AND state IN ({state_list})
        AND id IN (
            SELECT location_id FROM sighting
            WHERE source_db_id = (SELECT id FROM source_database WHERE name='UFOCAT')
        )
    """)
    print(f"    Fixed {cur.rowcount:,} US/CA longitude signs")

    # Fix 1b: UFOCAT longitude sign (all OTHER locations -- rest of world)
    # Same sign inversion: Eastern Hemisphere countries have negative lons (should be
    # positive), and Western Hemisphere countries outside US/CA have positive lons
    # (should be negative). Fix: negate ALL non-US/CA UFOCAT longitudes.
    print("  Fixing UFOCAT longitude signs (rest of world)...")
    cur.execute(f"""
        UPDATE location SET longitude = -longitude
        WHERE longitude IS NOT NULL
        AND (state IS NULL OR state NOT IN ({state_list}))
        AND id IN (
            SELECT location_id FROM sighting
            WHERE source_db_id = (SELECT id FROM source_database WHERE name='UFOCAT')
        )
    """)
    print(f"    Fixed {cur.rowcount:,} non-US/CA longitude signs")

    # Fix 2: UFOCAT city field (copy from raw_text where city is NULL)
    print("  Copying UFOCAT city from raw_text...")
    cur.execute("""
        UPDATE location SET city = raw_text
        WHERE city IS NULL AND raw_text IS NOT NULL
        AND id IN (
            SELECT location_id FROM sighting
            WHERE source_db_id = (SELECT id FROM source_database WHERE name='UFOCAT')
        )
    """)
    print(f"    Copied {cur.rowcount:,} city values")

    # Fix 3: Country code normalization
    print("  Normalizing country codes...")
    country_map = {
        'USA': 'US', 'United States': 'US', 'United States of America': 'US',
        'United Kingdom': 'GB', 'UK': 'GB', 'England': 'GB',
        'Canada': 'CA', 'Australia': 'AU',
    }
    for old, new in country_map.items():
        cur.execute("UPDATE location SET country = ? WHERE country = ?", (new, old))

    # Fix 4: MUFON date normalization (strip \n artifacts from date_event_raw)
    print("  Fixing MUFON date_event_raw artifacts...")
    cur.execute(r"""
        UPDATE sighting SET date_event_raw = REPLACE(date_event_raw, '\n', ' ')
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='MUFON')
        AND date_event_raw LIKE '%\n%'
    """)

    # Fix 5: MUFON date_event literal \n (0x5C6E) -- save time to time_raw, strip
    print("  Fixing MUFON date_event literal backslash-n...")
    cur.execute(r"""
        UPDATE sighting SET
            time_raw = SUBSTR(date_event, INSTR(date_event, '\n') + 2),
            date_event = SUBSTR(date_event, 1, INSTR(date_event, '\n') - 1)
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='MUFON')
        AND date_event LIKE '%\n%'
        AND time_raw IS NULL
    """)
    print(f"    Fixed {cur.rowcount:,} MUFON date_event literal backslash-n")

    # Fix 6: Null out MUFON year-0000 dates (invalid year from empty source field)
    print("  Nulling MUFON year-0000 dates...")
    cur.execute("""
        UPDATE sighting SET date_event = NULL
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='MUFON')
        AND date_event LIKE '0000-%'
    """)
    print(f"    Nulled {cur.rowcount:,} year-0000 dates")

    # Fix 7: Null out negative-year dates (parsing artifacts)
    print("  Nulling negative-year dates...")
    cur.execute("""
        UPDATE sighting SET date_event = NULL
        WHERE date_event LIKE '-%'
    """)
    print(f"    Nulled {cur.rowcount:,} negative-year dates")

    # Fix 7b: Truncate month-00 dates to year only (e.g. 1957-00-00 -> 1957)
    print("  Truncating month-00 dates...")
    cur.execute("""
        UPDATE sighting SET date_event = SUBSTR(date_event, 1, 4)
        WHERE date_event IS NOT NULL
        AND LENGTH(date_event) >= 7
        AND SUBSTR(date_event, 6, 2) = '00'
    """)
    print(f"    Truncated {cur.rowcount:,} month-00 dates")

    # Fix 7c: Truncate day-00 dates to YYYY-MM (e.g. 1985-07-00 -> 1985-07)
    print("  Truncating day-00 dates...")
    cur.execute("""
        UPDATE sighting SET date_event = SUBSTR(date_event, 1, 7)
        WHERE date_event IS NOT NULL
        AND LENGTH(date_event) >= 10
        AND SUBSTR(date_event, 9, 2) = '00'
    """)
    print(f"    Truncated {cur.rowcount:,} day-00 dates")

    # Fix 7d: Truncate impossible calendar dates (Feb 30+, 30-day month with 31)
    print("  Truncating impossible calendar dates...")
    cur.execute("""
        UPDATE sighting SET date_event = SUBSTR(date_event, 1, 7)
        WHERE date_event IS NOT NULL
        AND LENGTH(date_event) >= 10
        AND (
            (SUBSTR(date_event, 6, 2) = '02' AND CAST(SUBSTR(date_event, 9, 2) AS INTEGER) > 29)
            OR
            (SUBSTR(date_event, 6, 2) IN ('04','06','09','11') AND SUBSTR(date_event, 9, 2) = '31')
        )
    """)
    print(f"    Truncated {cur.rowcount:,} impossible dates")

    # Fix 8: Shape normalization -- titlecase for simple words (not hyphenated)
    print("  Normalizing shape case...")
    cur.execute("""
        UPDATE sighting SET shape = UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
        WHERE shape IS NOT NULL
        AND shape != UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
        AND shape NOT LIKE '%-%'
        AND shape NOT LIKE '% %'
    """)
    print(f"    Normalized {cur.rowcount:,} shape values")

    # Fix 8b: Hyphenated shape normalization (V-shape -> V-Shape)
    cur.execute("""
        UPDATE sighting SET shape =
            UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2, INSTR(shape, '-') - 2))
            || '-'
            || UPPER(SUBSTR(shape, INSTR(shape, '-') + 1, 1))
            || LOWER(SUBSTR(shape, INSTR(shape, '-') + 2))
        WHERE shape LIKE '%-%'
        AND shape IS NOT NULL
    """)

    # Fix 9: Shape typo corrections
    print("  Fixing shape typos...")
    shape_typo_map = {
        'Ballk': 'Ball',
        'Dumbell': 'Dumbbell',
        'Frieball': 'Fireball',
        'Triange': 'Triangle',
        'Ovois': 'Ovoid',
        'Eliptic': 'Elliptic',
        'Astrix': 'Asterisk',
        'Blim': 'Blimp',
        'Done': 'Dome',
    }
    fixed_typos = 0
    for old, new in shape_typo_map.items():
        cur.execute("UPDATE sighting SET shape = ? WHERE shape = ?", (new, old))
        fixed_typos += cur.rowcount
    print(f"    Fixed {fixed_typos:,} shape typos")

    # Fix 10: Remove junk shape values
    print("  Removing junk shape values...")
    junk_shapes = ['1', '2', 'ps']
    placeholders = ','.join('?' * len(junk_shapes))
    cur.execute(
        f"UPDATE sighting SET shape = NULL WHERE shape IN ({placeholders})",
        junk_shapes
    )
    print(f"    Nulled {cur.rowcount:,} junk shapes")

    # Fix 11: Uppercase Hynek classification codes
    print("  Normalizing Hynek codes...")
    cur.execute("""
        UPDATE sighting SET hynek = UPPER(hynek)
        WHERE hynek IS NOT NULL
        AND hynek != UPPER(hynek)
    """)
    print(f"    Uppercased {cur.rowcount:,} Hynek codes")

    # Fix 12: Uppercase Vallee classification codes
    print("  Normalizing Vallee codes...")
    cur.execute("""
        UPDATE sighting SET vallee = UPPER(vallee)
        WHERE vallee IS NOT NULL
        AND vallee != UPPER(vallee)
    """)
    print(f"    Uppercased {cur.rowcount:,} Vallee codes")

    # Fix 13: Null out [MISSING DATA] placeholder descriptions
    print("  Cleaning placeholder descriptions...")
    cur.execute("""
        UPDATE sighting SET description = NULL
        WHERE description = '[MISSING DATA]'
    """)
    print(f"    Nulled {cur.rowcount:,} [MISSING DATA] descriptions")

    # Fix 14: Strip MUFON razor boilerplate from descriptions
    print("  Stripping MUFON razor boilerplate...")
    cur.execute("""
        UPDATE sighting SET description =
            TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='MUFON')
        AND description LIKE 'Submitted by razor via e-mail%Investigator Notes:%'
        AND LENGTH(TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))) > 0
    """)
    print(f"    Stripped {cur.rowcount:,} razor boilerplate descriptions")

    # Fix 14b: Null empty descriptions left over from boilerplate stripping
    cur.execute("""
        UPDATE sighting SET description = NULL
        WHERE description IS NOT NULL AND TRIM(description) = ''
    """)
    # Fix 14c: Null boilerplate-only descriptions (no Investigator Notes content)
    cur.execute("""
        UPDATE sighting SET description = NULL
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='MUFON')
        AND description LIKE 'Submitted by razor via e-mail%'
        AND (description NOT LIKE '%Investigator Notes:%'
             OR LENGTH(TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))) = 0)
    """)

    conn.commit()
    conn.close()
    print("  Data fixes applied.")
