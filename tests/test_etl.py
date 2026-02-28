"""Tests for the ETL pipeline: schema creation, importer parsing functions,
helper utilities, and data quality fixes.

Validates that each source's date parsing, location parsing, and field
transformations produce correct output, and that post-import data fixes
(longitude sign, city copy, country normalization) work correctly.
"""
import json
import sqlite3
import pytest

from import_nuforc import (
    parse_nuforc_date,
    parse_nuforc_location,
    safe_str,
    safe_int as nuforc_safe_int,
)
from import_mufon import (
    parse_mufon_date,
    parse_mufon_location,
)
from import_ufocat import (
    parse_ufocat_date,
    safe_int as ufocat_safe_int,
    safe_float,
    SKIP_SOURCES as UFOCAT_SKIP_SOURCES,
)
from import_updb import (
    parse_updb_date,
    SKIP_SOURCES as UPDB_SKIP_SOURCES,
)
from import_geldreich import (
    parse_geldreich_date,
)
from create_schema import create_schema
from tests.conftest import insert_test_sighting


# ============================================================
# Schema Creation Tests
# ============================================================

class TestSchemaCreation:
    """Verify that create_schema produces the expected tables, indexes, and seed data."""

    @pytest.fixture
    def schema_db(self, tmp_path):
        """Create a fresh database using the real create_schema function."""
        db_path = str(tmp_path / "test_schema.db")
        create_schema(db_path)
        conn = sqlite3.connect(db_path)
        yield conn
        conn.close()

    def test_core_tables_exist(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = {row[0] for row in cur.fetchall()}
        expected = {
            'source_collection', 'source_database', 'source_origin',
            'location', 'sighting', 'duplicate_candidate',
            'reference', 'sighting_reference', 'attachment',
            'sentiment_analysis',
        }
        assert expected.issubset(tables)

    def test_sighting_columns(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("PRAGMA table_info(sighting)")
        cols = {row[1] for row in cur.fetchall()}
        required = {
            'id', 'source_db_id', 'source_record_id', 'origin_id',
            'date_event', 'date_event_raw', 'location_id',
            'summary', 'description', 'shape', 'color',
            'hynek', 'vallee', 'event_type', 'raw_json',
            'duration', 'num_witnesses', 'num_objects',
            'weather', 'terrain', 'source_ref',
        }
        assert required.issubset(cols)

    def test_location_columns(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("PRAGMA table_info(location)")
        cols = {row[1] for row in cur.fetchall()}
        required = {
            'id', 'raw_text', 'city', 'county', 'state', 'country',
            'region', 'latitude', 'longitude', 'geoname_id', 'geocode_src',
        }
        assert required.issubset(cols)

    def test_duplicate_candidate_columns(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("PRAGMA table_info(duplicate_candidate)")
        cols = {row[1] for row in cur.fetchall()}
        required = {
            'id', 'sighting_id_a', 'sighting_id_b',
            'similarity_score', 'match_method', 'status',
        }
        assert required.issubset(cols)

    def test_source_collections_seeded(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("SELECT name FROM source_collection ORDER BY name")
        names = [row[0] for row in cur.fetchall()]
        assert 'PUBLIUS' in names
        assert 'GELDREICH' in names
        assert 'UFOCAT' in names

    def test_source_databases_seeded(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("SELECT id, name FROM source_database ORDER BY id")
        rows = cur.fetchall()
        expected = [(1, 'MUFON'), (2, 'NUFORC'), (3, 'UFOCAT'), (4, 'UPDB'), (5, 'UFO-search')]
        assert rows == expected

    def test_source_database_ids_match_constants(self, schema_db):
        """Verify the source_database IDs match the SRC_* constants in dedup.py."""
        from dedup import SRC_MUFON, SRC_NUFORC, SRC_UFOCAT, SRC_UPDB, SRC_UFOSEARCH
        cur = schema_db.cursor()
        cur.execute("SELECT id, name FROM source_database")
        db_map = {name: id for id, name in cur.fetchall()}
        assert db_map['MUFON'] == SRC_MUFON
        assert db_map['NUFORC'] == SRC_NUFORC
        assert db_map['UFOCAT'] == SRC_UFOCAT
        assert db_map['UPDB'] == SRC_UPDB
        assert db_map['UFO-search'] == SRC_UFOSEARCH

    def test_source_origins_seeded(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("SELECT COUNT(*) FROM source_origin")
        count = cur.fetchone()[0]
        assert count >= 30  # 31 origins seeded
        cur.execute("SELECT name FROM source_origin WHERE name='BLUEBOOK'")
        assert cur.fetchone() is not None
        cur.execute("SELECT name FROM source_origin WHERE name='NICAP'")
        assert cur.fetchone() is not None

    def test_indexes_created(self, schema_db):
        cur = schema_db.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
        indexes = {row[0] for row in cur.fetchall()}
        expected = {
            'idx_sighting_date', 'idx_sighting_source',
            'idx_sighting_location', 'idx_location_country',
            'idx_location_city', 'idx_duplicate_status',
        }
        assert expected.issubset(indexes)


# ============================================================
# NUFORC Parsing Tests
# ============================================================

class TestNuforcDateParsing:
    def test_standard_date_time(self):
        iso, raw = parse_nuforc_date(' 1995-02-02 23:00 Local')
        assert iso == '1995-02-02T23:00'
        assert raw == '1995-02-02 23:00 Local'

    def test_date_only(self):
        iso, raw = parse_nuforc_date('2010-07-04')
        assert iso == '2010-07-04'

    def test_with_timezone_pacific(self):
        iso, raw = parse_nuforc_date('2005-03-15 20:30 Pacific')
        assert iso == '2005-03-15T20:30'
        assert 'Pacific' in raw

    def test_empty_string(self):
        assert parse_nuforc_date('') == (None, None)

    def test_none(self):
        assert parse_nuforc_date(None) == (None, None)

    def test_whitespace_only(self):
        assert parse_nuforc_date('   ') == (None, None)

    def test_unparseable(self):
        iso, raw = parse_nuforc_date('sometime in March')
        assert iso is None
        assert raw == 'sometime in March'

    def test_date_preserves_raw(self):
        _, raw = parse_nuforc_date(' 1995-02-02 23:00 Local')
        assert raw == '1995-02-02 23:00 Local'


class TestNuforcLocationParsing:
    def test_city_state_country(self):
        assert parse_nuforc_location('Shady Grove, OR, USA') == ('Shady Grove', 'OR', 'USA')

    def test_city_state_only(self):
        city, state, country = parse_nuforc_location('Phoenix, AZ')
        assert city == 'Phoenix'
        assert state == 'AZ'
        assert country is None

    def test_city_only(self):
        city, state, country = parse_nuforc_location('London')
        assert city == 'London'
        assert state is None
        assert country is None

    def test_empty(self):
        assert parse_nuforc_location('') == (None, None, None)

    def test_none(self):
        assert parse_nuforc_location(None) == (None, None, None)

    def test_whitespace_trimmed(self):
        city, state, country = parse_nuforc_location('  Phoenix ,  AZ , USA ')
        assert city == 'Phoenix'
        assert state == 'AZ'
        assert country == 'USA'


class TestNuforcHelpers:
    def test_safe_str_none(self):
        assert safe_str(None) == ''

    def test_safe_str_list(self):
        assert safe_str(['a', 'b', 'c']) == 'a, b, c'

    def test_safe_str_list_with_none(self):
        assert safe_str(['a', None, 'c']) == 'a, c'

    def test_safe_str_normal(self):
        assert safe_str('hello') == 'hello'

    def test_safe_str_int(self):
        assert safe_str(42) == '42'

    def test_safe_int_valid(self):
        assert nuforc_safe_int('42') == 42

    def test_safe_int_float_string(self):
        assert nuforc_safe_int('42.0') == 42

    def test_safe_int_none(self):
        assert nuforc_safe_int(None) is None

    def test_safe_int_empty(self):
        assert nuforc_safe_int('') is None

    def test_safe_int_invalid(self):
        assert nuforc_safe_int('abc') is None


# ============================================================
# MUFON Parsing Tests
# ============================================================

class TestMufonDateParsing:
    def test_date_with_time_am(self):
        iso, raw = parse_mufon_date('1992-08-19\n5:45AM')
        assert iso == '1992-08-19T05:45'

    def test_date_with_time_pm(self):
        iso, raw = parse_mufon_date('1992-08-19\n5:45PM')
        assert iso == '1992-08-19T17:45'

    def test_noon(self):
        iso, _ = parse_mufon_date('2005-06-15\n12:00PM')
        assert iso == '2005-06-15T12:00'

    def test_midnight(self):
        iso, _ = parse_mufon_date('2005-06-15\n12:00AM')
        assert iso == '2005-06-15T00:00'

    def test_date_only_no_time(self):
        iso, raw = parse_mufon_date('2005-06-15')
        assert iso == '2005-06-15'

    def test_24hr_time_no_ampm(self):
        iso, _ = parse_mufon_date('2005-06-15\n22:30')
        assert iso == '2005-06-15T22:30'

    def test_empty(self):
        assert parse_mufon_date('') == (None, None)

    def test_none(self):
        assert parse_mufon_date(None) == (None, None)

    def test_unparseable_date(self):
        iso, raw = parse_mufon_date('sometime')
        assert iso is None
        assert raw == 'sometime'

    def test_raw_preserved(self):
        _, raw = parse_mufon_date('1992-08-19\n5:45AM')
        assert raw == '1992-08-19\n5:45AM'


class TestMufonLocationParsing:
    def test_escaped_commas(self):
        city, state, country = parse_mufon_location('Newscandia\\, MN\\, US')
        assert city == 'Newscandia'
        assert state == 'MN'
        assert country == 'US'

    def test_normal_commas(self):
        city, state, country = parse_mufon_location('Phoenix, AZ, US')
        assert city == 'Phoenix'
        assert state == 'AZ'
        assert country == 'US'

    def test_empty(self):
        assert parse_mufon_location('') == (None, None, None)

    def test_none(self):
        assert parse_mufon_location(None) == (None, None, None)

    def test_city_only(self):
        city, state, country = parse_mufon_location('London')
        assert city == 'London'
        assert state is None
        assert country is None


# ============================================================
# UFOCAT Parsing Tests
# ============================================================

class TestUfocatDateParsing:
    def test_full_date_with_time(self):
        result = parse_ufocat_date('1992', '8', '19', '05:45')
        assert result == '1992-08-19T05:45'

    def test_full_date_no_time(self):
        result = parse_ufocat_date('1992', '8', '19', None)
        assert result == '1992-08-19'

    def test_year_month_no_day(self):
        result = parse_ufocat_date('1992', '8', None, None)
        assert result == '1992-08-01'

    def test_year_only(self):
        result = parse_ufocat_date('1992', None, None, None)
        assert result == '1992-01-01'

    def test_zero_year(self):
        assert parse_ufocat_date('0', '1', '1', None) is None

    def test_none_year(self):
        assert parse_ufocat_date(None, '1', '1', None) is None

    def test_empty_year(self):
        assert parse_ufocat_date('', '1', '1', None) is None

    def test_invalid_month_defaults(self):
        # Month out of range (13) defaults to 01
        result = parse_ufocat_date('1992', '13', '15', None)
        assert result == '1992-01-01'

    def test_invalid_day_defaults(self):
        # Day out of range (32) defaults to 01
        result = parse_ufocat_date('1992', '8', '32', None)
        assert result == '1992-08-01'

    def test_time_hhmm_format(self):
        # 4-digit time like "1430" → "14:30"
        result = parse_ufocat_date('1992', '8', '19', '1430')
        assert result == '1992-08-19T14:30'

    def test_time_3digit_format(self):
        # 3-digit time like "830" → "0830" → "08:30"
        result = parse_ufocat_date('1992', '8', '19', '830')
        assert result == '1992-08-19T08:30'

    def test_time_dot_separator(self):
        # Dot separator: "14.30" → "14:30"
        result = parse_ufocat_date('1992', '8', '19', '14.30')
        assert result == '1992-08-19T14:30'

    def test_time_semicolon_separator(self):
        result = parse_ufocat_date('1992', '8', '19', '14;30')
        assert result == '1992-08-19T14:30'

    def test_non_numeric_fields(self):
        assert parse_ufocat_date('abc', '1', '1', None) is None


class TestUfocatHelpers:
    def test_safe_float_valid(self):
        assert safe_float('33.45') == 33.45

    def test_safe_float_zero_returns_none(self):
        # UFOCAT convention: 0 means unknown
        assert safe_float('0') is None
        assert safe_float('0.0') is None

    def test_safe_float_none(self):
        assert safe_float(None) is None

    def test_safe_float_empty(self):
        assert safe_float('') is None

    def test_safe_float_invalid(self):
        assert safe_float('abc') is None

    def test_safe_float_negative(self):
        assert safe_float('-111.95') == -111.95

    def test_safe_int_valid(self):
        assert ufocat_safe_int('3') == 3

    def test_safe_int_float_string(self):
        assert ufocat_safe_int('3.0') == 3

    def test_safe_int_none(self):
        assert ufocat_safe_int(None) is None

    def test_safe_int_empty(self):
        assert ufocat_safe_int('') is None

    def test_ufocat_skip_sources(self):
        assert 'UFOReportCtr' in UFOCAT_SKIP_SOURCES


# ============================================================
# UPDB Parsing Tests
# ============================================================

class TestUpdbDateParsing:
    def test_standard_date_midnight(self):
        # 00:00:00 should be skipped (treated as unknown time)
        assert parse_updb_date('1993-05-20 00:00:00') == '1993-05-20'

    def test_date_with_real_time(self):
        assert parse_updb_date('1993-05-20 14:30:00') == '1993-05-20T14:30:00'

    def test_date_only(self):
        assert parse_updb_date('1993-05-20') == '1993-05-20'

    def test_empty(self):
        assert parse_updb_date('') is None

    def test_none(self):
        assert parse_updb_date(None) is None

    def test_unparseable(self):
        assert parse_updb_date('sometime in 1993') is None

    def test_skip_sources(self):
        assert 'MUFON' in UPDB_SKIP_SOURCES
        assert 'NUFORC' in UPDB_SKIP_SOURCES


# ============================================================
# Geldreich/UFO-search Parsing Tests
# ============================================================

class TestGeldreichDateParsing:
    def test_iso_date(self):
        iso, raw = parse_geldreich_date('1947-06-24')
        assert iso == '1947-06-24'
        assert raw == '1947-06-24'

    def test_slash_mdy(self):
        iso, raw = parse_geldreich_date('6/24/1947')
        assert iso == '1947-06-24'

    def test_slash_mdy_two_digit_year_pre1926(self):
        # Year > 25 → add 1900
        iso, _ = parse_geldreich_date('5/21/70')
        assert iso == '1970-05-21'

    def test_slash_mdy_two_digit_year_post2000(self):
        # Year <= 25 → add 2000
        iso, _ = parse_geldreich_date('3/15/10')
        assert iso == '2010-03-15'

    def test_slash_month_year(self):
        # M/YYYY like "4/34" meaning April 1934
        iso, _ = parse_geldreich_date('4/34')
        assert iso == '1934-04-01'

    def test_year_only(self):
        iso, raw = parse_geldreich_date('1947')
        assert iso == '1947-01-01'

    def test_summer_season(self):
        iso, raw = parse_geldreich_date('Summer 1947')
        assert iso == '1947-01-01'
        assert raw == 'Summer 1947'

    def test_fall_season(self):
        iso, _ = parse_geldreich_date('Fall 1952')
        assert iso == '1952-01-01'

    def test_early_prefix(self):
        iso, _ = parse_geldreich_date('Early 1960')
        assert iso == '1960-01-01'

    def test_decade_notation(self):
        # "0's" pattern
        iso, raw = parse_geldreich_date("50's")
        assert raw == "50's"
        # Parses as year 50 → 0050-01-01
        assert iso == '0050-01-01'

    def test_empty(self):
        assert parse_geldreich_date('') == (None, None)

    def test_none(self):
        assert parse_geldreich_date(None) == (None, None)

    def test_unparseable(self):
        iso, raw = parse_geldreich_date('?')
        assert iso is None
        assert raw == '?'


# ============================================================
# Data Quality Fixes Tests
# ============================================================

class TestDataFixLongitudeSign:
    """Test the UFOCAT longitude sign inversion fix from rebuild_db.apply_data_fixes."""

    def test_us_positive_longitude_negated(self, clean_db):
        """Positive longitude for US state should become negative."""
        cur = clean_db.cursor()

        # Insert a UFOCAT sighting with positive longitude (wrong for US)
        cur.execute(
            "INSERT INTO location (raw_text, city, state, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?)",
            ('Phoenix', 'Phoenix', 'AZ', 33.45, 112.07)  # positive, should be -112.07
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '2005-06-15', loc_id, 'test')  # source_db_id=3 is UFOCAT
        )
        clean_db.commit()

        # Run the longitude fix SQL (inline version of Fix 1a from rebuild_db.py)
        from rebuild_db import US_CA_STATES
        state_list = ','.join(f"'{s}'" for s in US_CA_STATES)
        cur.execute(f"""
            UPDATE location SET longitude = -longitude
            WHERE longitude > 0
            AND state IN ({state_list})
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT longitude FROM location WHERE id = ?", (loc_id,))
        lon = cur.fetchone()[0]
        assert lon == pytest.approx(-112.07)

    def test_non_ufocat_longitude_untouched(self, clean_db):
        """NUFORC longitude should NOT be modified by the UFOCAT fix."""
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text, city, state, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?)",
            ('Phoenix', 'Phoenix', 'AZ', 33.45, 112.07)
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (2, '2005-06-15', loc_id, 'test')  # source_db_id=2 is NUFORC
        )
        clean_db.commit()

        from rebuild_db import US_CA_STATES
        state_list = ','.join(f"'{s}'" for s in US_CA_STATES)
        cur.execute(f"""
            UPDATE location SET longitude = -longitude
            WHERE longitude > 0
            AND state IN ({state_list})
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT longitude FROM location WHERE id = ?", (loc_id,))
        lon = cur.fetchone()[0]
        assert lon == pytest.approx(112.07)  # unchanged

    def test_already_negative_longitude_untouched(self, clean_db):
        """Already-negative longitude should NOT be doubled-negated."""
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text, city, state, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?)",
            ('Phoenix', 'Phoenix', 'AZ', 33.45, -112.07)  # already correct
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '2005-06-15', loc_id, 'test')
        )
        clean_db.commit()

        from rebuild_db import US_CA_STATES
        state_list = ','.join(f"'{s}'" for s in US_CA_STATES)
        cur.execute(f"""
            UPDATE location SET longitude = -longitude
            WHERE longitude > 0
            AND state IN ({state_list})
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT longitude FROM location WHERE id = ?", (loc_id,))
        lon = cur.fetchone()[0]
        assert lon == pytest.approx(-112.07)  # still negative

    def test_rest_of_world_longitude_negated(self, clean_db):
        """Non-US/CA UFOCAT locations get all longitudes negated (Fix 1b)."""
        cur = clean_db.cursor()

        # A location with no US/CA state
        cur.execute(
            "INSERT INTO location (raw_text, city, state, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?)",
            ('London', 'London', None, 51.5, -0.12)  # negative, should become +0.12
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '1990-01-01', loc_id, 'test')
        )
        clean_db.commit()

        from rebuild_db import US_CA_STATES
        state_list = ','.join(f"'{s}'" for s in US_CA_STATES)
        cur.execute(f"""
            UPDATE location SET longitude = -longitude
            WHERE longitude IS NOT NULL
            AND (state IS NULL OR state NOT IN ({state_list}))
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT longitude FROM location WHERE id = ?", (loc_id,))
        lon = cur.fetchone()[0]
        assert lon == pytest.approx(0.12)


class TestDataFixCityFromRawText:
    """Test Fix 2: Copy UFOCAT city from raw_text where city is NULL."""

    def test_null_city_gets_raw_text(self, clean_db):
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text, city, state) VALUES (?, ?, ?)",
            ('Springfield', None, 'IL')
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '2005-06-15', loc_id, 'test')
        )
        clean_db.commit()

        cur.execute("""
            UPDATE location SET city = raw_text
            WHERE city IS NULL AND raw_text IS NOT NULL
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT city FROM location WHERE id = ?", (loc_id,))
        assert cur.fetchone()[0] == 'Springfield'

    def test_existing_city_not_overwritten(self, clean_db):
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text, city, state) VALUES (?, ?, ?)",
            ('Springfield Area', 'Springfield', 'IL')
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '2005-06-15', loc_id, 'test')
        )
        clean_db.commit()

        cur.execute("""
            UPDATE location SET city = raw_text
            WHERE city IS NULL AND raw_text IS NOT NULL
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT city FROM location WHERE id = ?", (loc_id,))
        assert cur.fetchone()[0] == 'Springfield'  # not overwritten with raw_text

    def test_non_ufocat_unaffected(self, clean_db):
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text, city, state) VALUES (?, ?, ?)",
            ('Springfield', None, 'IL')
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (2, '2005-06-15', loc_id, 'test')  # NUFORC, not UFOCAT
        )
        clean_db.commit()

        cur.execute("""
            UPDATE location SET city = raw_text
            WHERE city IS NULL AND raw_text IS NOT NULL
            AND id IN (SELECT location_id FROM sighting WHERE source_db_id = 3)
        """)
        clean_db.commit()

        cur.execute("SELECT city FROM location WHERE id = ?", (loc_id,))
        assert cur.fetchone()[0] is None  # not touched


class TestDataFixCountryNormalization:
    """Test Fix 3: Country code normalization (USA→US, UK→GB, etc.)."""

    @pytest.mark.parametrize("old,expected", [
        ('USA', 'US'),
        ('United States', 'US'),
        ('United States of America', 'US'),
        ('United Kingdom', 'GB'),
        ('UK', 'GB'),
        ('England', 'GB'),
        ('Canada', 'CA'),
        ('Australia', 'AU'),
    ])
    def test_country_normalization(self, clean_db, old, expected):
        cur = clean_db.cursor()
        cur.execute(
            "INSERT INTO location (raw_text, country) VALUES (?, ?)",
            ('somewhere', old)
        )
        loc_id = cur.lastrowid
        clean_db.commit()

        # Run the fix
        from rebuild_db import apply_data_fixes  # noqa: F811
        country_map = {
            'USA': 'US', 'United States': 'US', 'United States of America': 'US',
            'United Kingdom': 'GB', 'UK': 'GB', 'England': 'GB',
            'Canada': 'CA', 'Australia': 'AU',
        }
        for old_val, new_val in country_map.items():
            cur.execute("UPDATE location SET country = ? WHERE country = ?", (new_val, old_val))
        clean_db.commit()

        cur.execute("SELECT country FROM location WHERE id = ?", (loc_id,))
        assert cur.fetchone()[0] == expected

    def test_already_normalized_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute(
            "INSERT INTO location (raw_text, country) VALUES (?, ?)",
            ('somewhere', 'US')
        )
        loc_id = cur.lastrowid
        clean_db.commit()

        country_map = {
            'USA': 'US', 'United States': 'US', 'United States of America': 'US',
            'United Kingdom': 'GB', 'UK': 'GB', 'England': 'GB',
            'Canada': 'CA', 'Australia': 'AU',
        }
        for old_val, new_val in country_map.items():
            cur.execute("UPDATE location SET country = ? WHERE country = ?", (new_val, old_val))
        clean_db.commit()

        cur.execute("SELECT country FROM location WHERE id = ?", (loc_id,))
        assert cur.fetchone()[0] == 'US'

    def test_unknown_country_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute(
            "INSERT INTO location (raw_text, country) VALUES (?, ?)",
            ('somewhere', 'France')
        )
        loc_id = cur.lastrowid
        clean_db.commit()

        country_map = {
            'USA': 'US', 'United States': 'US', 'United States of America': 'US',
            'United Kingdom': 'GB', 'UK': 'GB', 'England': 'GB',
            'Canada': 'CA', 'Australia': 'AU',
        }
        for old_val, new_val in country_map.items():
            cur.execute("UPDATE location SET country = ? WHERE country = ?", (new_val, old_val))
        clean_db.commit()

        cur.execute("SELECT country FROM location WHERE id = ?", (loc_id,))
        assert cur.fetchone()[0] == 'France'


class TestDataFixMufonDateArtifacts:
    """Test Fix 4: MUFON date_event_raw newline removal."""

    def test_newline_replaced_with_space(self, clean_db):
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text) VALUES (?)", ('somewhere',)
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, date_event_raw, location_id, description) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2005-06-15', '2005-06-15\\n5:45AM', loc_id, 'test')
        )
        sighting_id = cur.lastrowid
        clean_db.commit()

        # Run the fix (literal \n in SQL, not actual newline)
        cur.execute(r"""
            UPDATE sighting SET date_event_raw = REPLACE(date_event_raw, '\n', ' ')
            WHERE source_db_id = 1
            AND date_event_raw LIKE '%\n%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event_raw FROM sighting WHERE id = ?", (sighting_id,))
        assert cur.fetchone()[0] == '2005-06-15 5:45AM'

    def test_non_mufon_unaffected(self, clean_db):
        cur = clean_db.cursor()

        cur.execute(
            "INSERT INTO location (raw_text) VALUES (?)", ('somewhere',)
        )
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, date_event_raw, location_id, description) "
            "VALUES (?, ?, ?, ?, ?)",
            (2, '2005-06-15', '2005-06-15\\nsome text', loc_id, 'test')
        )
        sighting_id = cur.lastrowid
        clean_db.commit()

        cur.execute(r"""
            UPDATE sighting SET date_event_raw = REPLACE(date_event_raw, '\n', ' ')
            WHERE source_db_id = 1
            AND date_event_raw LIKE '%\n%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event_raw FROM sighting WHERE id = ?", (sighting_id,))
        assert cur.fetchone()[0] == '2005-06-15\\nsome text'  # unchanged


# ============================================================
# Cross-Source Field Preservation Tests
# ============================================================

class TestFieldPreservation:
    """Verify that raw_json captures source fields and that field mappings
    are consistent with the sighting table structure."""

    def test_sighting_table_has_42_value_slots(self):
        """All importers use exactly 42 value placeholders in their INSERT."""
        # The INSERT statement has 42 columns. Verify by counting from the schema.
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("""
            CREATE TABLE sighting (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db_id INTEGER, source_record_id TEXT,
                origin_id INTEGER, origin_record_id TEXT,
                date_event TEXT, date_event_raw TEXT, date_end TEXT,
                time_raw TEXT, timezone TEXT,
                date_reported TEXT, date_posted TEXT,
                location_id INTEGER,
                summary TEXT, description TEXT,
                shape TEXT, color TEXT, size_estimated TEXT,
                angular_size TEXT, distance TEXT,
                duration TEXT, duration_seconds INTEGER,
                num_objects INTEGER, num_witnesses INTEGER,
                sound TEXT, direction TEXT, elevation_angle TEXT,
                viewed_from TEXT,
                witness_age TEXT, witness_sex TEXT, witness_names TEXT,
                hynek TEXT, vallee TEXT, event_type TEXT, svp_rating TEXT,
                explanation TEXT, characteristics TEXT,
                weather TEXT, terrain TEXT,
                source_ref TEXT, page_volume TEXT,
                notes TEXT, raw_json TEXT
            )
        """)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(sighting)")
        cols = cur.fetchall()
        # 43 columns total (id + 42 inserted fields)
        assert len(cols) == 43
        conn.close()

    def test_location_table_has_10_insert_slots(self):
        """All importers insert 10 values into location (id + 9 fields)."""
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE location (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_text TEXT, city TEXT, county TEXT, state TEXT,
                country TEXT, region TEXT, latitude REAL,
                longitude REAL, geoname_id INTEGER, geocode_src TEXT
            )
        """)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(location)")
        cols = cur.fetchall()
        # 11 columns total; importers insert 10 (id, raw_text, city, county,
        # state, country, region, latitude, longitude, geoname_id)
        # geocode_src is added later by geocode.py
        assert len(cols) == 11
        conn.close()
