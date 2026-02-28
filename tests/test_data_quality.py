"""Tests for data quality fixes discovered during live database inspection.

Each test class sets up dirty data in the in-memory DB, runs the fix SQL,
and asserts the data is clean afterward. These tests define the expected
behavior of new data-fix functions to be added to rebuild_db.py.

Issues found:
  1. Shape: 24 case-duplicate groups (fireball/Fireball/FireBall), typos (Ballk, Dumbell),
     junk values (ps, 1, 2), lowercase variants matching titlecase (light→Light)
  2. Date: MUFON \n in date_event (not just date_event_raw), year 0000, negative years
  3. Hynek: case duplicates (nl→NL, No→NO, ph→PH)
  4. Vallee: case duplicates (fb1→FB1, ma1→MA1)
  5. Description: [MISSING DATA] placeholders, residual MUFON razor boilerplate
"""
import sqlite3
import pytest

from tests.conftest import insert_test_sighting


# ============================================================
# Shape Normalization
# ============================================================

class TestShapeNormalization:
    """Test shape field normalization: case folding to Titlecase."""

    # --- Lowercase → Titlecase ---

    @pytest.mark.parametrize("dirty,expected", [
        ("changing", "Changing"),
        ("cigar", "Cigar"),
        ("circle", "Circle"),
        ("cylinder", "Cylinder"),
        ("diamond", "Diamond"),
        ("egg", "Egg"),
        ("fireball", "Fireball"),
        ("flash", "Flash"),
        ("irregulr", "Irregulr"),
        ("light", "Light"),
        ("linear", "Linear"),
        ("other", "Other"),
        ("oval", "Oval"),
        ("rectangle", "Rectangle"),
        ("sphere", "Sphere"),
        ("triangle", "Triangle"),
        ("unknown", "Unknown"),
    ])
    def test_lowercase_to_titlecase(self, clean_db, dirty, expected):
        """All-lowercase shapes should be normalized to Titlecase."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', dirty)
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Fix: normalize shape to titlecase for simple words
        cur.execute("""
            UPDATE sighting SET shape = UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            WHERE shape IS NOT NULL
            AND shape != UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            AND shape NOT LIKE '%-%'
            AND shape NOT LIKE '% %'
        """)
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == expected

    # --- CamelCase → Titlecase ---

    @pytest.mark.parametrize("dirty,expected", [
        ("BatWing", "Batwing"),
        ("BeeHive", "Beehive"),
        ("BowTie", "Bowtie"),
        ("DomeDisc", "Domedisc"),
        ("FireBall", "Fireball"),
        ("LightS", "Lights"),
        ("LiteBulb", "Litebulb"),
    ])
    def test_camelcase_to_titlecase(self, clean_db, dirty, expected):
        """CamelCase shapes should be normalized to Titlecase."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', dirty)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET shape = UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            WHERE shape IS NOT NULL
            AND shape != UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            AND shape NOT LIKE '%-%'
            AND shape NOT LIKE '% %'
        """)
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == expected

    # --- Hyphenated shapes preserved ---

    @pytest.mark.parametrize("shape", [
        "V-Shape", "C-Shape", "6-Shape", "8-Shape", "8-Form",
        "A-Shape", "H-Shape", "I-Shape", "J-Shape", "L-Shape",
    ])
    def test_hyphenated_shapes_preserved(self, clean_db, shape):
        """Hyphenated shapes should not be altered by simple titlecase fix."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', shape)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET shape = UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            WHERE shape IS NOT NULL
            AND shape != UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            AND shape NOT LIKE '%-%'
            AND shape NOT LIKE '% %'
        """)
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == shape  # unchanged

    # --- V-shape → V-Shape (lowercase after hyphen) ---

    def test_v_shape_lowercase_normalized(self, clean_db):
        """V-shape should become V-Shape."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', 'V-shape')
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Hyphenated fix: uppercase both parts
        cur.execute("""
            UPDATE sighting SET shape =
                UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2, INSTR(shape, '-') - 2))
                || '-'
                || UPPER(SUBSTR(shape, INSTR(shape, '-') + 1, 1))
                || LOWER(SUBSTR(shape, INSTR(shape, '-') + 2))
            WHERE shape LIKE '%-%'
            AND shape IS NOT NULL
        """)
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == 'V-Shape'


class TestShapeTypoFixes:
    """Test shape typo corrections: known misspellings → correct forms."""

    @pytest.mark.parametrize("typo,correct", [
        ("Ballk", "Ball"),
        ("Dumbell", "Dumbbell"),
        ("Frieball", "Fireball"),
        ("Triange", "Triangle"),
        ("Ovois", "Ovoid"),
        ("Eliptic", "Elliptic"),
        ("Astrix", "Asterisk"),
        ("Blim", "Blimp"),
        ("Done", "Dome"),
    ])
    def test_typo_correction(self, clean_db, typo, correct):
        """Known shape typos should be corrected to their canonical form."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', typo)
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Fix: explicit typo map
        typo_map = {
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
        for old, new in typo_map.items():
            cur.execute("UPDATE sighting SET shape = ? WHERE shape = ?", (new, old))
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == correct

    def test_correct_spelling_unaffected(self, clean_db):
        """Correctly-spelled shapes should not be modified."""
        cur = clean_db.cursor()
        correct_shapes = ["Ball", "Dumbbell", "Fireball", "Triangle", "Ovoid", "Dome"]
        sids = []
        for shape in correct_shapes:
            cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
            loc_id = cur.lastrowid
            cur.execute(
                "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
                "VALUES (?, ?, ?, ?, ?)",
                (1, '2020-01-01', loc_id, 'test', shape)
            )
            sids.append((cur.lastrowid, shape))
        clean_db.commit()

        typo_map = {
            'Ballk': 'Ball', 'Dumbell': 'Dumbbell', 'Frieball': 'Fireball',
            'Triange': 'Triangle', 'Ovois': 'Ovoid', 'Eliptic': 'Elliptic',
            'Astrix': 'Asterisk', 'Blim': 'Blimp', 'Done': 'Dome',
        }
        for old, new in typo_map.items():
            cur.execute("UPDATE sighting SET shape = ? WHERE shape = ?", (new, old))
        clean_db.commit()

        for sid, expected in sids:
            cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
            assert cur.fetchone()[0] == expected


class TestShapeJunkRemoval:
    """Test removal of junk/meaningless shape values."""

    @pytest.mark.parametrize("junk", ["1", "2", "ps"])
    def test_junk_shapes_nulled(self, clean_db, junk):
        """Numeric and meaningless shape values should be set to NULL."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', junk)
        )
        sid = cur.lastrowid
        clean_db.commit()

        junk_shapes = {'1', '2', 'ps'}
        placeholders = ','.join('?' * len(junk_shapes))
        cur.execute(
            f"UPDATE sighting SET shape = NULL WHERE shape IN ({placeholders})",
            list(junk_shapes)
        )
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None

    def test_valid_shape_not_nulled(self, clean_db):
        """Valid shapes should not be affected by junk removal."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', 'Triangle')
        )
        sid = cur.lastrowid
        clean_db.commit()

        junk_shapes = {'1', '2', 'ps'}
        placeholders = ','.join('?' * len(junk_shapes))
        cur.execute(
            f"UPDATE sighting SET shape = NULL WHERE shape IN ({placeholders})",
            list(junk_shapes)
        )
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == 'Triangle'


# ============================================================
# MUFON Date Fixes
# ============================================================

class TestMufonDateEventNewline:
    r"""Test Fix: Strip \n from MUFON date_event (not just date_event_raw)."""

    def test_newline_stripped_from_date_event(self, clean_db):
        r"""date_event '2020-01-15\n3:00PM' should become '2020-01-15'."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, date_event_raw, location_id, description) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-15\n3:00PM', '2020-01-15\n3:00PM', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Fix: strip everything from \n onward in date_event for MUFON
        cur.execute("""
            UPDATE sighting SET date_event = SUBSTR(date_event, 1, INSTR(date_event, CHAR(10)) - 1)
            WHERE source_db_id = 1
            AND INSTR(date_event, CHAR(10)) > 0
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == '2020-01-15'

    def test_date_event_without_newline_untouched(self, clean_db):
        """MUFON records with clean date_event should not be modified."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2020-01-15', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = SUBSTR(date_event, 1, INSTR(date_event, CHAR(10)) - 1)
            WHERE source_db_id = 1
            AND INSTR(date_event, CHAR(10)) > 0
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == '2020-01-15'

    def test_non_mufon_newline_unaffected(self, clean_db):
        """Non-MUFON records with newlines should not be modified."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (2, '2020-01-15\nsome text', loc_id, 'test')  # NUFORC
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = SUBSTR(date_event, 1, INSTR(date_event, CHAR(10)) - 1)
            WHERE source_db_id = 1
            AND INSTR(date_event, CHAR(10)) > 0
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == '2020-01-15\nsome text'

    def test_time_preserved_in_time_raw(self, clean_db):
        r"""The time portion after \n should be saved to time_raw."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2020-01-15\n3:00PM', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Fix: save time part to time_raw before stripping
        cur.execute("""
            UPDATE sighting SET
                time_raw = SUBSTR(date_event, INSTR(date_event, CHAR(10)) + 1),
                date_event = SUBSTR(date_event, 1, INSTR(date_event, CHAR(10)) - 1)
            WHERE source_db_id = 1
            AND INSTR(date_event, CHAR(10)) > 0
            AND time_raw IS NULL
        """)
        clean_db.commit()

        cur.execute("SELECT date_event, time_raw FROM sighting WHERE id = ?", (sid,))
        row = cur.fetchone()
        assert row[0] == '2020-01-15'
        assert row[1] == '3:00PM'


class TestMufonDateYear0000:
    """Test Fix: MUFON records with year 0000 should have date_event set to NULL."""

    def test_year_0000_nulled(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '0000-12-29\n4:20AM', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE source_db_id = 1
            AND date_event LIKE '0000-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None

    def test_year_0000_raw_preserved(self, clean_db):
        """date_event_raw should still contain the original value."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, date_event_raw, location_id, description) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '0000-12-29\n4:20AM', '0000-12-29\n4:20AM', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE source_db_id = 1
            AND date_event LIKE '0000-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event, date_event_raw FROM sighting WHERE id = ?", (sid,))
        row = cur.fetchone()
        assert row[0] is None
        assert row[1] is not None  # raw preserved

    def test_valid_mufon_date_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2020-06-15', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE source_db_id = 1
            AND date_event LIKE '0000-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == '2020-06-15'

    def test_non_mufon_0000_untouched(self, clean_db):
        """Year 0000 from other sources (unlikely but possible) should not be nulled."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (5, '0000-01-01', loc_id, 'test')  # UFO-search
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE source_db_id = 1
            AND date_event LIKE '0000-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == '0000-01-01'  # unchanged


class TestNegativeYearDates:
    """Test Fix: Negative year dates (e.g., -009-02-10) should be set to NULL."""

    def test_negative_year_nulled(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '-009-02-10', loc_id, 'test')  # UFOCAT
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE date_event LIKE '-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None

    def test_positive_date_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (3, '0881-09-03', loc_id, 'test')  # legitimate ancient date
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE date_event LIKE '-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == '0881-09-03'  # preserved


# ============================================================
# Hynek Classification Normalization
# ============================================================

class TestHynekNormalization:
    """Test Fix: Hynek codes should be uppercased (nl→NL, No→NO, ph→PH)."""

    @pytest.mark.parametrize("dirty,expected", [
        ("nl", "NL"),
        ("No", "NO"),
        ("ph", "PH"),
    ])
    def test_lowercase_hynek_uppercased(self, clean_db, dirty, expected):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, hynek) "
            "VALUES (?, ?, ?, ?, ?)",
            (3, '1980-01-01', loc_id, 'test', dirty)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET hynek = UPPER(hynek)
            WHERE hynek IS NOT NULL
            AND hynek != UPPER(hynek)
        """)
        clean_db.commit()

        cur.execute("SELECT hynek FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == expected

    def test_already_uppercase_untouched(self, clean_db):
        cur = clean_db.cursor()
        codes = ["CE1", "CE2", "CE3", "NL", "DD", "FB"]
        sids = []
        for code in codes:
            cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
            loc_id = cur.lastrowid
            cur.execute(
                "INSERT INTO sighting (source_db_id, date_event, location_id, description, hynek) "
                "VALUES (?, ?, ?, ?, ?)",
                (3, '1980-01-01', loc_id, 'test', code)
            )
            sids.append((cur.lastrowid, code))
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET hynek = UPPER(hynek)
            WHERE hynek IS NOT NULL
            AND hynek != UPPER(hynek)
        """)
        clean_db.commit()

        for sid, expected in sids:
            cur.execute("SELECT hynek FROM sighting WHERE id = ?", (sid,))
            assert cur.fetchone()[0] == expected

    def test_null_hynek_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, hynek) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', None)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET hynek = UPPER(hynek)
            WHERE hynek IS NOT NULL
            AND hynek != UPPER(hynek)
        """)
        clean_db.commit()

        cur.execute("SELECT hynek FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None


# ============================================================
# Vallee Classification Normalization
# ============================================================

class TestValleeNormalization:
    """Test Fix: Vallee codes should be uppercased (fb1→FB1, ma1→MA1)."""

    @pytest.mark.parametrize("dirty,expected", [
        ("fb1", "FB1"),
        ("ma1", "MA1"),
    ])
    def test_lowercase_vallee_uppercased(self, clean_db, dirty, expected):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, vallee) "
            "VALUES (?, ?, ?, ?, ?)",
            (3, '1980-01-01', loc_id, 'test', dirty)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET vallee = UPPER(vallee)
            WHERE vallee IS NOT NULL
            AND vallee != UPPER(vallee)
        """)
        clean_db.commit()

        cur.execute("SELECT vallee FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == expected

    def test_already_uppercase_untouched(self, clean_db):
        cur = clean_db.cursor()
        codes = ["CE1", "FB1", "MA1", "AN3", "FB2"]
        sids = []
        for code in codes:
            cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
            loc_id = cur.lastrowid
            cur.execute(
                "INSERT INTO sighting (source_db_id, date_event, location_id, description, vallee) "
                "VALUES (?, ?, ?, ?, ?)",
                (3, '1980-01-01', loc_id, 'test', code)
            )
            sids.append((cur.lastrowid, code))
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET vallee = UPPER(vallee)
            WHERE vallee IS NOT NULL
            AND vallee != UPPER(vallee)
        """)
        clean_db.commit()

        for sid, expected in sids:
            cur.execute("SELECT vallee FROM sighting WHERE id = ?", (sid,))
            assert cur.fetchone()[0] == expected

    def test_null_vallee_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, vallee) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', None)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET vallee = UPPER(vallee)
            WHERE vallee IS NOT NULL
            AND vallee != UPPER(vallee)
        """)
        clean_db.commit()

        cur.execute("SELECT vallee FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None


# ============================================================
# Description Cleanup
# ============================================================

class TestMissingDataPlaceholder:
    """Test Fix: [MISSING DATA] descriptions should be set to NULL."""

    def test_missing_data_nulled(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, '[MISSING DATA]')
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET description = NULL
            WHERE description = '[MISSING DATA]'
        """)
        clean_db.commit()

        cur.execute("SELECT description FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None

    def test_real_description_untouched(self, clean_db):
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'Bright light seen over the lake')
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET description = NULL
            WHERE description = '[MISSING DATA]'
        """)
        clean_db.commit()

        cur.execute("SELECT description FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == 'Bright light seen over the lake'

    def test_partial_missing_data_untouched(self, clean_db):
        """Descriptions containing [MISSING DATA] but with other text should not be nulled."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        desc = 'Saw something. [MISSING DATA] for duration.'
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, desc)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET description = NULL
            WHERE description = '[MISSING DATA]'
        """)
        clean_db.commit()

        cur.execute("SELECT description FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == desc  # not exact match, so preserved


class TestMufonBoilerplateInDescription:
    """Test Fix: Residual MUFON razor boilerplate should be stripped from descriptions."""

    def test_razor_boilerplate_stripped(self, clean_db):
        """Description starting with 'Submitted by razor via e-mail' should be cleaned."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        desc = 'Submitted by razor via e-mail: Investigator Notes: Large triangular craft hovering silently.'
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2015-03-15', loc_id, desc)
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Fix: strip razor boilerplate preamble
        cur.execute("""
            UPDATE sighting SET description =
                TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))
            WHERE source_db_id = 1
            AND description LIKE 'Submitted by razor via e-mail%Investigator Notes:%'
            AND LENGTH(TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))) > 0
        """)
        clean_db.commit()

        cur.execute("SELECT description FROM sighting WHERE id = ?", (sid,))
        result = cur.fetchone()[0]
        assert 'Submitted by razor' not in result
        assert 'Large triangular craft' in result

    def test_non_boilerplate_untouched(self, clean_db):
        """Normal MUFON descriptions should not be modified."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        desc = 'Bright orange orb hovering silently above the treeline for 10 minutes.'
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2015-03-15', loc_id, desc)
        )
        sid = cur.lastrowid
        clean_db.commit()

        cur.execute("""
            UPDATE sighting SET description =
                TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))
            WHERE source_db_id = 1
            AND description LIKE 'Submitted by razor via e-mail%Investigator Notes:%'
            AND LENGTH(TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))) > 0
        """)
        clean_db.commit()

        cur.execute("SELECT description FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == desc

    def test_boilerplate_only_nulled(self, clean_db):
        """If boilerplate has no content after 'Investigator Notes:', null it."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        desc = 'Submitted by razor via e-mail: Investigator Notes: '
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '2015-03-15', loc_id, desc)
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Step 1: strip boilerplate
        cur.execute("""
            UPDATE sighting SET description =
                TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))
            WHERE source_db_id = 1
            AND description LIKE 'Submitted by razor via e-mail%Investigator Notes:%'
            AND LENGTH(TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))) > 0
        """)
        # Step 2: null empty descriptions
        cur.execute("""
            UPDATE sighting SET description = NULL
            WHERE description IS NOT NULL AND TRIM(description) = ''
        """)
        # Step 3: also null if still has boilerplate-only (not caught by step 1)
        cur.execute("""
            UPDATE sighting SET description = NULL
            WHERE source_db_id = 1
            AND description LIKE 'Submitted by razor via e-mail%'
            AND (description NOT LIKE '%Investigator Notes:%'
                 OR LENGTH(TRIM(SUBSTR(description, INSTR(description, 'Investigator Notes:') + 19))) = 0)
        """)
        clean_db.commit()

        cur.execute("SELECT description FROM sighting WHERE id = ?", (sid,))
        result = cur.fetchone()[0]
        # Should be either NULL or empty (the boilerplate-only was not cleaned by step 1
        # because the content after Investigator Notes: was empty/whitespace)
        assert result is None or result.strip() == ''


# ============================================================
# Combined Fix Order Tests
# ============================================================

class TestFixOrdering:
    """Test that fixes apply correctly in sequence without interfering."""

    def test_shape_normalization_then_typo_fix(self, clean_db):
        """Shape normalization should run before typo fixes so 'frieball' → 'Fireball'."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        # 'frieball' is lowercase typo — needs BOTH titlecase + typo fix
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, shape) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, '2020-01-01', loc_id, 'test', 'frieball')
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Step 1: titlecase normalization
        cur.execute("""
            UPDATE sighting SET shape = UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            WHERE shape IS NOT NULL
            AND shape != UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            AND shape NOT LIKE '%-%'
            AND shape NOT LIKE '% %'
        """)
        # Step 2: typo fixes
        typo_map = {'Frieball': 'Fireball', 'Ballk': 'Ball', 'Dumbell': 'Dumbbell',
                     'Triange': 'Triangle', 'Ovois': 'Ovoid', 'Eliptic': 'Elliptic',
                     'Astrix': 'Asterisk', 'Blim': 'Blimp', 'Done': 'Dome'}
        for old, new in typo_map.items():
            cur.execute("UPDATE sighting SET shape = ? WHERE shape = ?", (new, old))
        clean_db.commit()

        cur.execute("SELECT shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == 'Fireball'

    def test_mufon_date_newline_then_year0000(self, clean_db):
        r"""Date \n strip should run before year 0000 nullification."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description) "
            "VALUES (?, ?, ?, ?)",
            (1, '0000-12-29\n4:20AM', loc_id, 'test')
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Step 1: save time, strip newline
        cur.execute("""
            UPDATE sighting SET
                time_raw = SUBSTR(date_event, INSTR(date_event, CHAR(10)) + 1),
                date_event = SUBSTR(date_event, 1, INSTR(date_event, CHAR(10)) - 1)
            WHERE source_db_id = 1
            AND INSTR(date_event, CHAR(10)) > 0
            AND time_raw IS NULL
        """)
        # Step 2: null year 0000
        cur.execute("""
            UPDATE sighting SET date_event = NULL
            WHERE source_db_id = 1
            AND date_event LIKE '0000-%'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event, time_raw FROM sighting WHERE id = ?", (sid,))
        row = cur.fetchone()
        assert row[0] is None  # date nulled
        assert row[1] == '4:20AM'  # time preserved

    def test_all_fixes_on_single_record(self, clean_db):
        """A record with multiple issues gets all fixes applied."""
        cur = clean_db.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc_id = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, location_id, description, "
            "shape, hynek, vallee) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, '2020-05-15\n8:00PM', loc_id, '[MISSING DATA]', 'fireball', None, None)
        )
        sid = cur.lastrowid
        clean_db.commit()

        # Apply all fixes in order
        # 1. MUFON date newline
        cur.execute("""
            UPDATE sighting SET
                time_raw = SUBSTR(date_event, INSTR(date_event, CHAR(10)) + 1),
                date_event = SUBSTR(date_event, 1, INSTR(date_event, CHAR(10)) - 1)
            WHERE source_db_id = 1
            AND INSTR(date_event, CHAR(10)) > 0
            AND time_raw IS NULL
        """)
        # 2. Shape normalization
        cur.execute("""
            UPDATE sighting SET shape = UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            WHERE shape IS NOT NULL
            AND shape != UPPER(SUBSTR(shape, 1, 1)) || LOWER(SUBSTR(shape, 2))
            AND shape NOT LIKE '%-%'
            AND shape NOT LIKE '% %'
        """)
        # 3. [MISSING DATA] nullification
        cur.execute("""
            UPDATE sighting SET description = NULL
            WHERE description = '[MISSING DATA]'
        """)
        clean_db.commit()

        cur.execute("SELECT date_event, time_raw, shape, description FROM sighting WHERE id = ?", (sid,))
        row = cur.fetchone()
        assert row[0] == '2020-05-15'
        assert row[1] == '8:00PM'
        assert row[2] == 'Fireball'
        assert row[3] is None
