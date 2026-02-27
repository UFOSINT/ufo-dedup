"""
Create the unified UFO sightings SQLite database schema.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")

def create_schema(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    cur = conn.cursor()

    # ==========================================
    # REFERENCE / LOOKUP TABLES
    # ==========================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS source_collection (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL UNIQUE,
        display_name TEXT,
        description TEXT,
        url         TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS source_database (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL UNIQUE,
        collection_id INTEGER REFERENCES source_collection(id),
        description TEXT,
        url         TEXT,
        copyright   TEXT,
        record_count INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS source_origin (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL UNIQUE,
        description TEXT
    )
    """)

    # ==========================================
    # CORE TABLES
    # ==========================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS location (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_text    TEXT,
        city        TEXT,
        county      TEXT,
        state       TEXT,
        country     TEXT,
        region      TEXT,
        latitude    REAL,
        longitude   REAL,
        geoname_id  INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sighting (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,

        -- PROVENANCE
        source_db_id        INTEGER NOT NULL REFERENCES source_database(id),
        source_record_id    TEXT,
        origin_id           INTEGER REFERENCES source_origin(id),
        origin_record_id    TEXT,

        -- DATE / TIME
        date_event          TEXT,       -- ISO 8601 when parseable
        date_event_raw      TEXT,       -- Original string
        date_end            TEXT,
        time_raw            TEXT,
        timezone            TEXT,

        -- REPORTING
        date_reported       TEXT,
        date_posted         TEXT,

        -- LOCATION
        location_id         INTEGER REFERENCES location(id),

        -- DESCRIPTION
        summary             TEXT,
        description         TEXT,

        -- OBSERVATION DETAILS
        shape               TEXT,
        color               TEXT,
        size_estimated      TEXT,
        angular_size        TEXT,
        distance            TEXT,
        duration            TEXT,
        duration_seconds    INTEGER,
        num_objects         INTEGER,
        num_witnesses       INTEGER,
        sound               TEXT,
        direction           TEXT,
        elevation_angle     TEXT,
        viewed_from         TEXT,

        -- WITNESS INFO
        witness_age         TEXT,
        witness_sex         TEXT,
        witness_names       TEXT,

        -- CLASSIFICATION
        hynek               TEXT,
        vallee              TEXT,
        event_type          TEXT,
        svp_rating          TEXT,

        -- RESOLUTION
        explanation         TEXT,
        characteristics     TEXT,

        -- WEATHER / CONDITIONS
        weather             TEXT,
        terrain             TEXT,

        -- REFERENCES
        source_ref          TEXT,
        page_volume         TEXT,

        -- METADATA
        notes               TEXT,
        raw_json            TEXT,       -- JSON blob of all original fields
        created_at          TEXT DEFAULT (datetime('now'))
    )
    """)

    # ==========================================
    # SUPPORTING TABLES
    # ==========================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reference (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        text    TEXT NOT NULL,
        hash    TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sighting_reference (
        sighting_id  INTEGER NOT NULL REFERENCES sighting(id),
        reference_id INTEGER NOT NULL REFERENCES reference(id),
        PRIMARY KEY (sighting_id, reference_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS attachment (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        sighting_id INTEGER NOT NULL REFERENCES sighting(id),
        url         TEXT,
        file_type   TEXT,
        description TEXT
    )
    """)

    # ==========================================
    # DEDUPLICATION SUPPORT
    # ==========================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS duplicate_candidate (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        sighting_id_a   INTEGER NOT NULL REFERENCES sighting(id),
        sighting_id_b   INTEGER NOT NULL REFERENCES sighting(id),
        similarity_score REAL,
        match_method    TEXT,
        status          TEXT DEFAULT 'pending',
        resolved_at     TEXT,
        UNIQUE (sighting_id_a, sighting_id_b)
    )
    """)

    # ==========================================
    # INDEXES
    # ==========================================

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_sighting_date ON sighting(date_event)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_source ON sighting(source_db_id)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_origin ON sighting(origin_id)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_shape ON sighting(shape)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_hynek ON sighting(hynek)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_vallee ON sighting(vallee)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_type ON sighting(event_type)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_location ON sighting(location_id)",
        "CREATE INDEX IF NOT EXISTS idx_location_country ON location(country)",
        "CREATE INDEX IF NOT EXISTS idx_location_city ON location(city)",
        "CREATE INDEX IF NOT EXISTS idx_location_coords ON location(latitude, longitude)",
        "CREATE INDEX IF NOT EXISTS idx_duplicate_status ON duplicate_candidate(status)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_source_record ON sighting(source_db_id, source_record_id)",
    ]
    for idx_sql in indexes:
        cur.execute(idx_sql)

    conn.commit()

    # ==========================================
    # SEED SOURCE COLLECTIONS
    # ==========================================

    collections = [
        ("PUBLIUS", "PUBLIUS", "Compiled by Publius from original reporting sites and PhenomAInon downloads", None),
        ("GELDREICH", "GELDREICH", "Rich Geldreich's Majestic Timeline compilation from 19+ historical sources", "https://ufo-search.com"),
        ("UFOCAT", "UFOCAT", "CUFOS UFOCAT catalog — independent academic dataset", "https://cufos.org"),
    ]
    cur.executemany("""
        INSERT OR IGNORE INTO source_collection (name, display_name, description, url)
        VALUES (?, ?, ?, ?)
    """, collections)

    # Build collection lookup for FK assignment
    cur.execute("SELECT id, name FROM source_collection")
    coll_map = {name: cid for cid, name in cur.fetchall()}

    # ==========================================
    # SEED SOURCE DATABASES
    # ==========================================

    sources = [
        ("MUFON", coll_map["PUBLIUS"], "Mutual UFO Network case reports", "https://www.mufon.com", None),
        ("NUFORC", coll_map["PUBLIUS"], "National UFO Reporting Center", "https://nuforc.org", None),
        ("UFOCAT", coll_map["UFOCAT"], "CUFOS UFOCAT 2023 database", "https://cufos.org", None),
        ("UPDB", coll_map["PUBLIUS"], "PhenomAInon Unified Phenomena Database — compiled and parsed by Publius", None, None),
        ("UFO-search", coll_map["GELDREICH"], "Majestic Timeline compilation (ufo-search.com)", "https://ufo-search.com", None),
    ]
    cur.executemany("""
        INSERT OR IGNORE INTO source_database (name, collection_id, description, url, copyright)
        VALUES (?, ?, ?, ?, ?)
    """, sources)

    origins = [
        ("MUFON",), ("NUFORC",), ("NICAP",), ("BLUEBOOK",), ("UFODNA",),
        ("BAASS",), ("NIDS",), ("SKINWALKER",), ("PILOTS",),
        ("BRAZILGOV",), ("CANADAGOV",), ("UKTNA",),
        ("Hatch",), ("ValleeMagonia",), ("WondersInTheSky",),
        ("EberhartUFOI",), ("Overmeire",), ("rr0",), ("Johnson",),
        ("NICAP_DB",), ("Dolan",), ("Maj2",), ("Rife",),
        ("BerlinerBBUnknowns",), ("HallUFOEvidence2",),
        ("Anon2023PDF",), ("Trace",), ("Scully",),
        ("HostileFawcett",), ("MysteryHelicoptersAdams",),
        ("NukeExplosions",),
    ]
    cur.executemany("""
        INSERT OR IGNORE INTO source_origin (name) VALUES (?)
    """, origins)

    conn.commit()
    conn.close()
    print(f"Database created at: {db_path}")
    print(f"Size: {os.path.getsize(db_path):,} bytes")

if __name__ == "__main__":
    create_schema()
