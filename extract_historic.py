"""Extract all pre-1901 sightings into a standalone SQLite database for analysis.

Creates temp/historic_pre1901.db with:
  - Full sighting + location + source data
  - date_analysis table with parsed year components and classification flags
  - Views for common analysis queries

Usage:
    python extract_historic.py
"""
import sqlite3
import os

SRC_DB = "ufo_unified.db"
DST_DB = os.path.join("temp", "historic_pre1901.db")
CUTOFF_YEAR = 1901


def extract(src_path, dst_path):
    if os.path.exists(dst_path):
        os.remove(dst_path)

    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)
    dst.execute("PRAGMA journal_mode=WAL")
    dst.execute("PRAGMA foreign_keys=ON")
    dc = dst.cursor()

    # ── Schema ──────────────────────────────────────────────────────────
    dc.executescript("""
        CREATE TABLE source_database (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE sighting (
            id               INTEGER PRIMARY KEY,
            source_db_id     INTEGER NOT NULL REFERENCES source_database(id),
            source_record_id TEXT,
            date_event       TEXT,
            date_event_raw   TEXT,
            time_raw         TEXT,
            location_id      INTEGER,
            summary          TEXT,
            description      TEXT,
            shape            TEXT,
            hynek            TEXT,
            vallee           TEXT,
            source_ref       TEXT,
            num_witnesses    INTEGER,
            duration         TEXT,
            raw_json         TEXT
        );

        CREATE TABLE location (
            id          INTEGER PRIMARY KEY,
            raw_text    TEXT,
            city        TEXT,
            county      TEXT,
            state       TEXT,
            country     TEXT,
            latitude    REAL,
            longitude   REAL,
            geoname_id  INTEGER,
            geocode_src TEXT
        );

        CREATE TABLE date_analysis (
            sighting_id      INTEGER PRIMARY KEY REFERENCES sighting(id),
            source_name      TEXT,
            date_event       TEXT,
            date_event_raw   TEXT,
            raw_year_str     TEXT,
            raw_year_digits  INTEGER,
            parsed_year      INTEGER,
            city             TEXT,
            state            TEXT,
            country          TEXT,
            description_snip TEXT,
            -- Classification flags (to be filled during analysis)
            category         TEXT DEFAULT 'unclassified',
            corrected_year   INTEGER,
            notes            TEXT
        );

        CREATE INDEX idx_da_category   ON date_analysis(category);
        CREATE INDEX idx_da_raw_digits ON date_analysis(raw_year_digits);
        CREATE INDEX idx_da_source     ON date_analysis(source_name);
        CREATE INDEX idx_da_parsed_yr  ON date_analysis(parsed_year);
        CREATE INDEX idx_s_date        ON sighting(date_event);
        CREATE INDEX idx_s_source      ON sighting(source_db_id);
    """)

    # ── Copy source_database rows ───────────────────────────────────────
    rows = src.execute("SELECT id, name FROM source_database").fetchall()
    dc.executemany("INSERT INTO source_database VALUES (?,?)", rows)

    # ── Find pre-1901 sighting IDs ──────────────────────────────────────
    sighting_rows = src.execute("""
        SELECT s.id, s.source_db_id, s.source_record_id,
               s.date_event, s.date_event_raw, s.time_raw,
               s.location_id, s.summary, s.description,
               s.shape, s.hynek, s.vallee, s.source_ref,
               s.num_witnesses, s.duration, s.raw_json
        FROM sighting s
        WHERE s.date_event IS NOT NULL
          AND LENGTH(s.date_event) >= 4
          AND CAST(SUBSTR(s.date_event, 1, 4) AS INTEGER) BETWEEN 1 AND ?
    """, (CUTOFF_YEAR - 1,)).fetchall()

    dc.executemany("""
        INSERT INTO sighting VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, sighting_rows)
    print(f"  Extracted {len(sighting_rows)} sightings")

    # ── Copy referenced locations ───────────────────────────────────────
    loc_ids = set(r[6] for r in sighting_rows if r[6] is not None)
    if loc_ids:
        placeholders = ",".join("?" * len(loc_ids))
        loc_rows = src.execute(f"""
            SELECT id, raw_text, city, county, state, country,
                   latitude, longitude, geoname_id, geocode_src
            FROM location WHERE id IN ({placeholders})
        """, list(loc_ids)).fetchall()
        dc.executemany("""
            INSERT INTO location VALUES (?,?,?,?,?,?,?,?,?,?)
        """, loc_rows)
        print(f"  Extracted {len(loc_rows)} locations")

    # ── Build date_analysis table ───────────────────────────────────────
    dc.execute("""
        INSERT INTO date_analysis (
            sighting_id, source_name, date_event, date_event_raw,
            raw_year_str, raw_year_digits, parsed_year,
            city, state, country, description_snip
        )
        SELECT
            s.id,
            sd.name,
            s.date_event,
            s.date_event_raw,
            -- Extract the year portion from raw date (before first '/')
            CASE
                WHEN s.date_event_raw IS NOT NULL AND INSTR(s.date_event_raw, '/') > 0
                THEN SUBSTR(s.date_event_raw, 1, INSTR(s.date_event_raw, '/') - 1)
                ELSE NULL
            END,
            -- Count digits in raw year
            CASE
                WHEN s.date_event_raw IS NOT NULL AND INSTR(s.date_event_raw, '/') > 0
                THEN LENGTH(SUBSTR(s.date_event_raw, 1, INSTR(s.date_event_raw, '/') - 1))
                ELSE NULL
            END,
            CAST(SUBSTR(s.date_event, 1, 4) AS INTEGER),
            l.city,
            l.state,
            l.country,
            SUBSTR(s.description, 1, 200)
        FROM sighting s
        JOIN source_database sd ON s.source_db_id = sd.id
        LEFT JOIN location l ON s.location_id = l.id
    """)

    # ── Auto-classify obvious categories ────────────────────────────────

    # UFOCAT 2-digit year '19' = "somewhere in the 1900s, year unknown"
    dc.execute("""
        UPDATE date_analysis SET category = 'ufocat_century_only'
        WHERE source_name = 'UFOCAT'
          AND raw_year_digits = 2
          AND raw_year_str = '19'
    """)
    n = dc.rowcount
    print(f"  Classified {n} as ufocat_century_only (raw year = '19')")

    # UFOCAT 3-digit years that are legit ancient (description has ancient content)
    # For now just flag them as 'ufocat_3digit' for manual review
    dc.execute("""
        UPDATE date_analysis SET category = 'ufocat_3digit_review'
        WHERE source_name = 'UFOCAT'
          AND raw_year_digits = 3
    """)
    n = dc.rowcount
    print(f"  Classified {n} as ufocat_3digit_review")

    # UFOCAT 4-digit years pre-1901 = legitimately ancient
    dc.execute("""
        UPDATE date_analysis SET category = 'ufocat_ancient'
        WHERE source_name = 'UFOCAT'
          AND raw_year_digits = 4
          AND parsed_year < 1901
    """)
    n = dc.rowcount
    print(f"  Classified {n} as ufocat_ancient")

    # UFOCAT other 2-digit years (not 19)
    dc.execute("""
        UPDATE date_analysis SET category = 'ufocat_2digit_ancient'
        WHERE source_name = 'UFOCAT'
          AND raw_year_digits = 2
          AND raw_year_str != '19'
    """)
    n = dc.rowcount
    print(f"  Classified {n} as ufocat_2digit_ancient")

    # UFO-search / UPDB / MUFON / NUFORC pre-1901 — no raw year parsing
    # (different raw date formats)
    dc.execute("""
        UPDATE date_analysis SET category = 'other_source_review'
        WHERE category = 'unclassified'
          AND source_name != 'UFOCAT'
    """)
    n = dc.rowcount
    print(f"  Classified {n} as other_source_review")

    # ── Analysis views ──────────────────────────────────────────────────
    dc.executescript("""
        -- Summary by category
        CREATE VIEW v_category_summary AS
        SELECT category, source_name, COUNT(*) as cnt,
               MIN(parsed_year) as min_year, MAX(parsed_year) as max_year
        FROM date_analysis
        GROUP BY category, source_name
        ORDER BY category, source_name;

        -- UFOCAT 3-digit year review candidates
        CREATE VIEW v_3digit_review AS
        SELECT sighting_id, raw_year_str, parsed_year,
               city, state, country, description_snip, category, notes
        FROM date_analysis
        WHERE category = 'ufocat_3digit_review'
        ORDER BY parsed_year;

        -- Century-only records (19//) with location info
        CREATE VIEW v_century_only AS
        SELECT sighting_id, date_event, date_event_raw,
               city, state, country, description_snip
        FROM date_analysis
        WHERE category = 'ufocat_century_only'
        ORDER BY state, city;

        -- UPDB suspicious low years
        CREATE VIEW v_updb_review AS
        SELECT sighting_id, parsed_year, date_event, date_event_raw,
               city, state, country, description_snip, notes
        FROM date_analysis
        WHERE source_name = 'UPDB'
          AND parsed_year < 1000
        ORDER BY parsed_year;

        -- All records ordered by parsed year for timeline scan
        CREATE VIEW v_timeline AS
        SELECT sighting_id, source_name, parsed_year, category,
               raw_year_str, raw_year_digits,
               city, state, country,
               SUBSTR(description_snip, 1, 80) as desc_short,
               corrected_year, notes
        FROM date_analysis
        ORDER BY parsed_year, source_name;
    """)

    dst.commit()
    dst.close()
    src.close()

    size_kb = os.path.getsize(dst_path) / 1024
    print(f"\n  Output: {dst_path} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    print(f"Extracting pre-{CUTOFF_YEAR} records from {SRC_DB} ...")
    extract(SRC_DB, DST_DB)
    print("Done.")
