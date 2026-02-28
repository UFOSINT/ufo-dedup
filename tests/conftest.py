"""Shared fixtures for UFO deduplication tests.

Provides an in-memory SQLite database with the full schema and seed data,
plus helpers for inserting synthetic sighting records.
"""
import sqlite3
import pytest


@pytest.fixture(scope="session")
def db_conn():
    """Session-scoped in-memory SQLite database with full schema and seed data."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    cur = conn.cursor()

    # -- Reference / lookup tables --
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

    # -- Core tables --
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
        geoname_id  INTEGER,
        geocode_src TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sighting (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        source_db_id        INTEGER NOT NULL REFERENCES source_database(id),
        source_record_id    TEXT,
        origin_id           INTEGER REFERENCES source_origin(id),
        origin_record_id    TEXT,
        date_event          TEXT,
        date_event_raw      TEXT,
        date_end            TEXT,
        time_raw            TEXT,
        timezone            TEXT,
        date_reported       TEXT,
        date_posted         TEXT,
        location_id         INTEGER REFERENCES location(id),
        summary             TEXT,
        description         TEXT,
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
        witness_age         TEXT,
        witness_sex         TEXT,
        witness_names       TEXT,
        hynek               TEXT,
        vallee              TEXT,
        event_type          TEXT,
        svp_rating          TEXT,
        explanation         TEXT,
        characteristics     TEXT,
        weather             TEXT,
        terrain             TEXT,
        source_ref          TEXT,
        page_volume         TEXT,
        notes               TEXT,
        raw_json            TEXT,
        created_at          TEXT DEFAULT (datetime('now'))
    )
    """)

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_analysis (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        sighting_id      INTEGER NOT NULL UNIQUE REFERENCES sighting(id),
        vader_compound   REAL,
        vader_positive   REAL,
        vader_negative   REAL,
        vader_neutral    REAL,
        emo_joy          INTEGER DEFAULT 0,
        emo_fear         INTEGER DEFAULT 0,
        emo_anger        INTEGER DEFAULT 0,
        emo_sadness      INTEGER DEFAULT 0,
        emo_surprise     INTEGER DEFAULT 0,
        emo_disgust      INTEGER DEFAULT 0,
        emo_trust        INTEGER DEFAULT 0,
        emo_anticipation INTEGER DEFAULT 0,
        text_source      TEXT,
        text_length      INTEGER,
        created_at       TEXT DEFAULT (datetime('now'))
    )
    """)

    # -- Indexes --
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_sighting_date ON sighting(date_event)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_source ON sighting(source_db_id)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_location ON sighting(location_id)",
        "CREATE INDEX IF NOT EXISTS idx_location_country ON location(country)",
        "CREATE INDEX IF NOT EXISTS idx_location_city ON location(city)",
        "CREATE INDEX IF NOT EXISTS idx_duplicate_status ON duplicate_candidate(status)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_source_date ON sighting(source_db_id, date_event)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_source_ref ON sighting(source_ref)",
        "CREATE INDEX IF NOT EXISTS idx_location_city_state ON location(city, state)",
    ]
    for sql in indexes:
        cur.execute(sql)

    # -- Seed source collections --
    collections = [
        ("PUBLIUS", "PUBLIUS", "Compiled by Publius", None),
        ("GELDREICH", "GELDREICH", "Rich Geldreich Majestic Timeline", "https://ufo-search.com"),
        ("UFOCAT", "UFOCAT", "CUFOS UFOCAT catalog", "https://cufos.org"),
    ]
    cur.executemany("""
        INSERT OR IGNORE INTO source_collection (name, display_name, description, url)
        VALUES (?, ?, ?, ?)
    """, collections)

    cur.execute("SELECT id, name FROM source_collection")
    coll_map = {name: cid for cid, name in cur.fetchall()}

    # -- Seed source databases (IDs 1-5 matching SRC_MUFON..SRC_UFOSEARCH) --
    sources = [
        ("MUFON", coll_map["PUBLIUS"], "Mutual UFO Network", "https://www.mufon.com", None),
        ("NUFORC", coll_map["PUBLIUS"], "National UFO Reporting Center", "https://nuforc.org", None),
        ("UFOCAT", coll_map["UFOCAT"], "CUFOS UFOCAT 2023", "https://cufos.org", None),
        ("UPDB", coll_map["PUBLIUS"], "PhenomAInon Unified Phenomena Database", None, None),
        ("UFO-search", coll_map["GELDREICH"], "Majestic Timeline compilation", "https://ufo-search.com", None),
    ]
    cur.executemany("""
        INSERT OR IGNORE INTO source_database (name, collection_id, description, url, copyright)
        VALUES (?, ?, ?, ?, ?)
    """, sources)

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def clean_db(db_conn):
    """Function-scoped fixture: clears data tables before each test."""
    db_conn.execute("DELETE FROM duplicate_candidate")
    db_conn.execute("DELETE FROM sentiment_analysis")
    db_conn.execute("DELETE FROM sighting")
    db_conn.execute("DELETE FROM location")
    db_conn.commit()
    yield db_conn


def insert_test_sighting(conn, source_db_id, date_event, city, state, country,
                         description, raw_text=None, source_ref=None):
    """Insert a location + sighting row and return the sighting_id.

    raw_text defaults to city if not provided (useful for UFOCAT-style records
    where city info lives in the raw_text column).
    """
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO location (raw_text, city, state, country) VALUES (?, ?, ?, ?)",
        (raw_text if raw_text is not None else city, city, state, country),
    )
    loc_id = cur.lastrowid

    cur.execute(
        """INSERT INTO sighting (source_db_id, date_event, location_id, description, source_ref)
           VALUES (?, ?, ?, ?, ?)""",
        (source_db_id, date_event, loc_id, description, source_ref),
    )
    sighting_id = cur.lastrowid
    conn.commit()
    return sighting_id
