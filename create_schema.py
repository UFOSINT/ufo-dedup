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
        geoname_id  INTEGER,
        geocode_src TEXT          -- NULL=original, 'geonames_exact', 'geonames_city_country', 'geonames_city_only'
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
        created_at          TEXT DEFAULT (datetime('now')),

        -- DERIVED ANALYSIS (populated by analyze.py)
        standardized_shape  TEXT,       -- canonical shape from fuzzy matching
        primary_color       TEXT,       -- first/dominant color from extract_colors
        sentiment_score     REAL,       -- copied from sentiment_analysis.vader_compound
        dominant_emotion    TEXT,       -- argmax of emo_* columns
        quality_score       INTEGER,    -- 0-100 richness heuristic
        richness_score      INTEGER,    -- count of filled meaningful fields
        hoax_likelihood     REAL,       -- 0.0-1.0 rule-based
        topic_id            INTEGER,    -- reserved for v0.9 topic modeling
        duration_bucket     TEXT,       -- instant|seconds|minutes|hours|days
        movement_type       TEXT,       -- hover|fast|erratic|linear|stationary|unknown
        has_movement_mentioned INTEGER, -- 0 or 1: narrative mentions structured movement
        movement_categories TEXT,       -- JSON array of movement_category names

        -- PUBLIC DATASET FIELDS (safe to expose after raw text is dropped)
        lat                 REAL,       -- denormalized from location.latitude
        lng                 REAL,       -- denormalized from location.longitude
        sighting_datetime   TEXT,       -- ISO 8601 combined date + time (fallback: date_event)
        has_description     INTEGER,    -- 0 or 1: description/summary text existed in raw
        has_media           INTEGER,    -- 0 or 1: photo/video mentioned or attachment row exists

        -- NRC LEXICON WORD COUNTS (denormalized from sentiment_analysis)
        nrc_joy             INTEGER,
        nrc_fear            INTEGER,
        nrc_anger           INTEGER,
        nrc_sadness         INTEGER,
        nrc_surprise        INTEGER,
        nrc_disgust         INTEGER,
        nrc_trust           INTEGER,
        nrc_anticipation    INTEGER,
        nrc_positive        INTEGER,
        nrc_negative        INTEGER,

        -- NUCLEAR PROXIMITY (computed by gerb_overlay.py)
        distance_to_nearest_nuclear_site_km REAL,
        nearest_nuclear_site_name TEXT,

        -- v0.11 EMOTION CLASSIFICATION (populated by emotions.py)
        emotion_28_dominant TEXT,       -- GoEmotions 28-class label
        emotion_28_group    TEXT,       -- positive|negative|ambiguous|neutral
        emotion_7_dominant  TEXT,       -- 7-class RoBERTa label
        vader_compound      REAL,       -- VADER compound score, -1.0 to +1.0
        roberta_sentiment   REAL,       -- RoBERTa-large sentiment, -1.0 to +1.0
        emotion_7_surprise  REAL,       -- 7-class softmax probability
        emotion_7_fear      REAL,
        emotion_7_neutral   REAL,
        emotion_7_anger     REAL,
        emotion_7_disgust   REAL,
        emotion_7_sadness   REAL,
        emotion_7_joy       REAL
    )
    """)

    # ==========================================
    # UAP GERB OVERLAY TABLES
    # ==========================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS crash_retrieval (
        id              TEXT PRIMARY KEY,      -- slug e.g. '1947-roswell-crash'
        page_name       TEXT NOT NULL,
        year            INTEGER,
        date_event      TEXT,
        city            TEXT,
        region          TEXT,
        country         TEXT,
        latitude        REAL,
        longitude       REAL,
        precision       TEXT,                  -- exact|regional|uncertain
        craft_type      TEXT,
        craft_size_m    REAL,
        recovery_status TEXT,
        has_biologics   INTEGER,               -- 0/1
        crew_count      TEXT,
        evidence_quality TEXT,
        source_confidence TEXT,
        short_summary   TEXT,
        raw_json        TEXT                   -- full record as JSON
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS nuclear_encounter (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        page_name       TEXT NOT NULL,
        year            INTEGER,
        date_event      TEXT,
        base            TEXT,
        city            TEXT,
        region          TEXT,
        country         TEXT,
        latitude        REAL,
        longitude       REAL,
        weapon_system   TEXT,
        incident_type   TEXT,                  -- overflight|intercept|etc.
        missiles_affected INTEGER,
        sensor_confirmation TEXT,              -- JSON array
        witness_credibility TEXT,
        evidence_quality TEXT,
        source_confidence TEXT,
        summary         TEXT,
        raw_json        TEXT                   -- full record as JSON
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS facility (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        name            TEXT,
        facility_type   TEXT,                  -- military_base|national_lab|contractor_site|test_range|etc.
        latitude        REAL NOT NULL,
        longitude       REAL NOT NULL,
        source          TEXT                   -- 'crash_storage'|'nuclear_base'|'combined_geojson'
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
    # SENTIMENT ANALYSIS
    # ==========================================

    # ==========================================
    # DERIVED ANALYSIS (richer JSON-y fields)
    # ==========================================

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sighting_analysis (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        sighting_id              INTEGER NOT NULL UNIQUE REFERENCES sighting(id),
        behavior_tags            TEXT,   -- JSON array
        color_list               TEXT,   -- JSON array
        emotion_scores           TEXT,   -- JSON object (normalized emotion floats)
        hoax_flags               TEXT,   -- JSON array of flag names
        raw_shape_matched_via    TEXT,   -- exact|substring|fuzzy|unmatched
        created_at               TEXT DEFAULT (datetime('now'))
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
        "CREATE INDEX IF NOT EXISTS idx_sentiment_sighting ON sentiment_analysis(sighting_id)",
        "CREATE INDEX IF NOT EXISTS idx_sentiment_compound ON sentiment_analysis(vader_compound)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_quality ON sighting(quality_score)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_hoax ON sighting(hoax_likelihood)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_std_shape ON sighting(standardized_shape)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_topic ON sighting(topic_id)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_dom_emotion ON sighting(dominant_emotion)",
        "CREATE INDEX IF NOT EXISTS idx_analysis_sighting ON sighting_analysis(sighting_id)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_latlng ON sighting(lat, lng)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_datetime ON sighting(sighting_datetime)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_has_desc ON sighting(has_description)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_has_media ON sighting(has_media)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_has_movement ON sighting(has_movement_mentioned)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_emo28 ON sighting(emotion_28_dominant)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_emo28g ON sighting(emotion_28_group)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_emo7 ON sighting(emotion_7_dominant)",
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
        ("Reddit-UFOs", coll_map["PUBLIUS"], "r/UFOs community sighting reports", "https://reddit.com/r/UFOs", None),
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
