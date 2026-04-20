"""Tests for analyze.py — derived-insight pipeline.

Uses a fresh on-disk SQLite built from the real create_schema() so that the
new columns (standardized_shape, quality_score, etc.) and the new
sighting_analysis table are present. Each test class exercises one function.
"""
import json
import sqlite3
import pytest

from ufosint.schema import create_schema
import analyze


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def analysis_db(tmp_path):
    """Fresh DB with the real production schema, connection yielded."""
    db_path = str(tmp_path / "analyze_test.db")
    create_schema(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    yield conn, db_path
    conn.close()


def _insert_sighting(conn, **fields):
    """Insert a minimal sighting row plus its location. Returns sighting_id.

    Accepts any column on sighting as a kwarg, plus latitude/longitude which
    are routed to the location row.
    """
    cur = conn.cursor()

    lat = fields.pop("latitude", None)
    lon = fields.pop("longitude", None)
    cur.execute(
        "INSERT INTO location (raw_text, latitude, longitude) VALUES (?, ?, ?)",
        ("test location", lat, lon),
    )
    loc_id = cur.lastrowid

    fields.setdefault("source_db_id", 1)
    fields.setdefault("date_event", "2020-01-01")
    fields["location_id"] = loc_id

    cols = ", ".join(fields.keys())
    placeholders = ", ".join("?" * len(fields))
    cur.execute(
        f"INSERT INTO sighting ({cols}) VALUES ({placeholders})",
        list(fields.values()),
    )
    sid = cur.lastrowid
    conn.commit()
    return sid


def _insert_sentiment(conn, sighting_id, compound=0.0, **emos):
    """Insert a sentiment_analysis row. Any emo_* kwargs get set."""
    cur = conn.cursor()
    defaults = {f"emo_{k}": 0 for k in analyze.EMOTION_KEYS}
    for k, v in emos.items():
        defaults[k] = v
    cur.execute(
        """INSERT INTO sentiment_analysis
           (sighting_id, vader_compound, vader_positive, vader_negative, vader_neutral,
            emo_joy, emo_fear, emo_anger, emo_sadness,
            emo_surprise, emo_disgust, emo_trust, emo_anticipation,
            text_source, text_length)
           VALUES (?, ?, 0.0, 0.0, 0.0, ?, ?, ?, ?, ?, ?, ?, ?, 'description', 100)""",
        (sighting_id, compound,
         defaults["emo_joy"], defaults["emo_fear"], defaults["emo_anger"],
         defaults["emo_sadness"], defaults["emo_surprise"], defaults["emo_disgust"],
         defaults["emo_trust"], defaults["emo_anticipation"]),
    )
    conn.commit()


# ============================================================
# 1. Shape normalization
# ============================================================

class TestShapeNormalization:
    def test_exact_match(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, shape="Triangle", description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.normalize_and_cluster_shapes(conn)

        cur = conn.cursor()
        cur.execute("SELECT standardized_shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "Triangle"
        cur.execute("SELECT raw_shape_matched_via FROM sighting_analysis WHERE sighting_id = ?", (sid,))
        assert cur.fetchone()[0] == "exact"

    def test_lowercase_and_plural(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, shape="DISCS", description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.normalize_and_cluster_shapes(conn)

        cur = conn.cursor()
        cur.execute("SELECT standardized_shape FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "Disc"

    def test_substring_match(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, shape="cigar-shaped", description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.normalize_and_cluster_shapes(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT s.standardized_shape, a.raw_shape_matched_via
            FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
            WHERE s.id = ?
        """, (sid,))
        shape, method = cur.fetchone()
        assert shape == "Cigar"
        assert method == "substring"

    def test_unmatched_falls_to_other(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, shape="xyz-garbage-thing", description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.normalize_and_cluster_shapes(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT s.standardized_shape, a.raw_shape_matched_via
            FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
            WHERE s.id = ?
        """, (sid,))
        shape, method = cur.fetchone()
        assert shape == "Other"
        assert method == "unmatched"


# ============================================================
# 2. Movement classification
# ============================================================

class TestMovementClassification:
    def test_hovering_and_silent(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn,
            description="Object hovered above the trees, completely silent.",
        )
        analyze.ensure_analysis_rows(conn)
        analyze.classify_movement(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT s.movement_type, a.behavior_tags
            FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
            WHERE s.id = ?
        """, (sid,))
        movement, tags_json = cur.fetchone()
        tags = json.loads(tags_json)
        assert "hovering" in tags
        assert "silent" in tags
        assert movement == "hover"

    def test_erratic_zigzag(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="Zigzag pattern, very erratic flight path.")
        analyze.ensure_analysis_rows(conn)
        analyze.classify_movement(conn)

        cur = conn.cursor()
        cur.execute("SELECT movement_type FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "erratic"


# ============================================================
# 3. Color extraction
# ============================================================

class TestColorExtraction:
    def test_multiple_colors(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn,
            description="Bright red and white lights with a hint of blue.",
        )
        analyze.ensure_analysis_rows(conn)
        analyze.extract_colors(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT s.primary_color, a.color_list
            FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
            WHERE s.id = ?
        """, (sid,))
        primary, colors_json = cur.fetchone()
        colors = json.loads(colors_json)
        # "bright red" is longer and matches first
        assert primary == "bright red"
        assert "white" in colors
        assert "blue" in colors

    def test_no_colors(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="Object seen in sky.")
        analyze.ensure_analysis_rows(conn)
        analyze.extract_colors(conn)

        cur = conn.cursor()
        cur.execute("SELECT primary_color FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None


# ============================================================
# 4. Sentiment derivation
# ============================================================

class TestSentimentDerivation:
    def test_dominant_emotion_argmax(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="scary thing")
        _insert_sentiment(conn, sid, compound=-0.8, emo_fear=10, emo_surprise=3)
        analyze.ensure_analysis_rows(conn)
        analyze.derive_sentiment_summary(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT s.sentiment_score, s.dominant_emotion, a.emotion_scores
            FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
            WHERE s.id = ?
        """, (sid,))
        score, dominant, emo_json = cur.fetchone()
        assert score == pytest.approx(-0.8)
        assert dominant == "fear"
        normalized = json.loads(emo_json)
        assert normalized["fear"] == pytest.approx(10 / 13, abs=0.01)
        assert normalized["surprise"] == pytest.approx(3 / 13, abs=0.01)

    def test_tie_yields_null(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="neutral")
        _insert_sentiment(conn, sid, compound=0.0, emo_joy=1, emo_fear=1)
        analyze.ensure_analysis_rows(conn)
        analyze.derive_sentiment_summary(conn)

        cur = conn.cursor()
        cur.execute("SELECT dominant_emotion FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None


# ============================================================
# Movement categories (v0.8.3)
# ============================================================

class TestMovementCategories:
    def test_hovering_classified(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="The craft was stationary and motionless.")
        analyze.ensure_analysis_rows(conn)
        analyze.classify_movement(conn)

        cur = conn.cursor()
        cur.execute(
            "SELECT has_movement_mentioned, movement_categories FROM sighting WHERE id = ?",
            (sid,),
        )
        has, cats_json = cur.fetchone()
        cats = json.loads(cats_json)
        assert has == 1
        assert "hovering" in cats

    def test_multiple_categories(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn,
            description="Object hovered briefly then accelerated away and vanished into the distance.",
        )
        analyze.ensure_analysis_rows(conn)
        analyze.classify_movement(conn)

        cur = conn.cursor()
        cur.execute("SELECT movement_categories FROM sighting WHERE id = ?", (sid,))
        cats = json.loads(cur.fetchone()[0])
        assert "hovering" in cats
        assert "accelerating" in cats
        assert "vanished" in cats

    def test_ascending_and_descending(self, analysis_db):
        conn, _ = analysis_db
        sid_up = _insert_sighting(conn, description="The object climbed straight up and away.")
        sid_down = _insert_sighting(conn, description="It descended toward the trees and dropped out of sight.")
        analyze.ensure_analysis_rows(conn)
        analyze.classify_movement(conn)

        cur = conn.cursor()
        cur.execute("SELECT movement_categories FROM sighting WHERE id = ?", (sid_up,))
        assert "ascending" in json.loads(cur.fetchone()[0])
        cur.execute("SELECT movement_categories FROM sighting WHERE id = ?", (sid_down,))
        assert "descending" in json.loads(cur.fetchone()[0])

    def test_no_movement_mentioned(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="A bright red glow in the sky for about ten minutes.")
        analyze.ensure_analysis_rows(conn)
        analyze.classify_movement(conn)

        cur = conn.cursor()
        cur.execute(
            "SELECT has_movement_mentioned, movement_categories FROM sighting WHERE id = ?",
            (sid,),
        )
        has, cats_json = cur.fetchone()
        assert has == 0
        assert json.loads(cats_json) == []


# ============================================================
# 5. Quality score
# ============================================================

class TestQualityScore:
    def test_minimal_row_low_score(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)

        cur = conn.cursor()
        cur.execute("SELECT quality_score FROM sighting WHERE id = ?", (sid,))
        score = cur.fetchone()[0]
        assert score is not None
        assert score < 15  # tiny desc only, no other signals

    def test_rich_row_high_score(self, analysis_db):
        conn, _ = analysis_db
        rich_desc = (
            "At around 9pm on a clear summer night I saw a bright object "
            "hovering silently to the northeast at roughly 500 feet altitude. "
            "It remained motionless for several minutes then accelerated away "
            "in a flash. I took a photo on my cell phone camera."
        ) * 2
        sid = _insert_sighting(
            conn,
            description=rich_desc,
            time_raw="21:00",
            shape="Sphere",
            color="White",
            duration="several minutes",
            num_witnesses=2,
            sound="silent",
            direction="NE",
            elevation_angle="30",
            hynek="NL",
            vallee="FB1",
            latitude=40.7,
            longitude=-74.0,
        )
        analyze.ensure_analysis_rows(conn)
        # New ordering: movement + public_fields must run before quality_score
        # so has_movement_mentioned / has_media / lat/lng are populated.
        analyze.classify_movement(conn)
        analyze.derive_public_fields(conn)
        analyze.calculate_quality_score(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT quality_score, richness_score, has_media, has_movement_mentioned
            FROM sighting WHERE id = ?
        """, (sid,))
        score, features, has_media, has_mov = cur.fetchone()
        # Rich row w/ photo + hovering + accelerating + 2 witnesses should land
        # solidly in the high-quality bucket under the v0.8.3 weighting.
        assert has_media == 1
        assert has_mov == 1
        assert score >= 75, f"expected >=75, got {score}"
        assert features >= 10


# ============================================================
# Unknown-date quality cap (v0.8.3)
# ============================================================

class TestUnknownDateQualityCap:
    def test_null_date_caps_rich_row(self, analysis_db):
        """Even a very rich row with NULL date_event is capped at 15."""
        conn, _ = analysis_db
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO location (raw_text, latitude, longitude) VALUES ('x', 40.0, -100.0)"
        )
        loc = cur.lastrowid
        # Craft a rich row with date_event explicitly NULL
        cur.execute(
            """
            INSERT INTO sighting (
                source_db_id, date_event, location_id, description,
                shape, color, num_witnesses,
                has_media, has_movement_mentioned, movement_categories,
                lat, lng
            ) VALUES (1, NULL, ?, ?, 'Sphere', 'White', 3, 1, 1,
                     '["hovering","accelerating"]', 40.0, -100.0)
            """,
            (
                loc,
                "Bright hovering craft observed for several minutes at 9pm "
                "in the northeast at 500 feet altitude. I took a photo." * 3,
            ),
        )
        sid = cur.lastrowid
        conn.commit()

        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)

        cur.execute("SELECT quality_score FROM sighting WHERE id = ?", (sid,))
        score = cur.fetchone()[0]
        assert score <= analyze.UNKNOWN_DATE_CAP

    def test_known_date_not_capped(self, analysis_db):
        """Same row with a real date_event is NOT capped."""
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn,
            date_event="2020-05-15",
            description="Bright hovering craft observed in the northeast at 500 feet altitude." * 3,
            num_witnesses=3,
        )
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE sighting
            SET has_media = 1,
                has_movement_mentioned = 1,
                movement_categories = '["hovering","accelerating"]',
                lat = 40.0, lng = -100.0
            WHERE id = ?
            """,
            (sid,),
        )
        conn.commit()

        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)

        cur.execute("SELECT quality_score FROM sighting WHERE id = ?", (sid,))
        score = cur.fetchone()[0]
        assert score > analyze.UNKNOWN_DATE_CAP

    def test_null_date_rich_row_relaxed_cap(self, analysis_db):
        """Text-rich NULL-date row (features>=8, has_description) is
        capped at UNKNOWN_DATE_CAP_RICH (35), not UNKNOWN_DATE_CAP (15).

        This is the NICAP/Blue Book carve-out: if the narrative is rich
        enough, a missing date shouldn't floor an otherwise high-quality
        record.
        """
        conn, _ = analysis_db
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO location (raw_text, latitude, longitude) VALUES ('x', 40.0, -100.0)"
        )
        loc = cur.lastrowid
        # Features: desc (>=200) + has_media + witnesses tier + movement +
        # 6 structured fields + coords = 11 features. Well above the
        # threshold of 8.
        cur.execute(
            """
            INSERT INTO sighting (
                source_db_id, date_event, location_id, description,
                shape, color, duration, sound, direction, hynek,
                num_witnesses,
                has_media, has_movement_mentioned, movement_categories,
                lat, lng
            ) VALUES (1, NULL, ?, ?, 'Disc', 'Silver', 'minutes', 'silent',
                      'NE', 'NL', 3, 1, 1,
                      '["hovering","accelerating"]', 40.0, -100.0)
            """,
            (
                loc,
                "Detailed NICAP-style report of a disc hovering silently over "
                "the reservoir at 9pm approximately 2000 feet altitude northeast "
                "of town, observed by three witnesses for several minutes before "
                "accelerating away." * 2,
            ),
        )
        sid = cur.lastrowid
        conn.commit()

        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)

        cur.execute(
            "SELECT quality_score, richness_score FROM sighting WHERE id = ?",
            (sid,),
        )
        score, richness = cur.fetchone()
        assert richness >= analyze.UNKNOWN_DATE_RICH_MIN_FEATURES, f"expected richness>=8, got {richness}"
        assert analyze.UNKNOWN_DATE_CAP < score <= analyze.UNKNOWN_DATE_CAP_RICH, (
            f"expected {analyze.UNKNOWN_DATE_CAP} < score <= {analyze.UNKNOWN_DATE_CAP_RICH}, got {score}"
        )


# ============================================================
# 6. Hoax flags
# ============================================================

class TestHoaxFlags:
    def test_short_and_generic(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="Saw a UFO.")
        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)  # need features_count
        analyze.flag_potential_hoaxes(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT s.hoax_likelihood, a.hoax_flags
            FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
            WHERE s.id = ?
        """, (sid,))
        weight, flags_json = cur.fetchone()
        flags = json.loads(flags_json)
        assert "very_short_text" in flags
        assert "generic_phrasing" in flags
        assert weight >= 0.4

    def test_dramatic_without_specifics(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn,
            description="Aliens abducted me and probed me for hours and hours.",
        )
        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)
        analyze.flag_potential_hoaxes(conn)

        cur = conn.cursor()
        cur.execute("SELECT hoax_flags FROM sighting_analysis WHERE sighting_id = ?", (sid,))
        flags = json.loads(cur.fetchone()[0])
        assert "dramatic_no_specifics" in flags

    def test_rich_normal_row_clean(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn,
            description=(
                "At approximately 9:30 pm on a cloudless evening I observed "
                "a slow-moving silver object crossing from the north toward "
                "the east at an estimated 2000 feet altitude."
            ),
            shape="Disc",
            color="Silver",
            direction="NE",
            num_witnesses=1,
            latitude=40.0,
            longitude=-100.0,
        )
        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)
        analyze.flag_potential_hoaxes(conn)

        cur = conn.cursor()
        cur.execute("SELECT hoax_likelihood FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == 0.0

    def test_duplicate_phrasing(self, analysis_db):
        conn, _ = analysis_db
        shared = (
            "Dear sir, on the night in question I was returning home from "
            "work when I happened to observe an unusual object overhead "
            "which I cannot explain by conventional means at all."
        )
        for _ in range(11):
            _insert_sighting(conn, description=shared)
        analyze.ensure_analysis_rows(conn)
        analyze.calculate_quality_score(conn)
        analyze.flag_potential_hoaxes(conn)

        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM sighting_analysis
            WHERE hoax_flags LIKE '%duplicate_phrasing%'
        """)
        flagged = cur.fetchone()[0]
        assert flagged == 11


# ============================================================
# 8. Duration bucketing
# ============================================================

class TestDurationBucket:
    @pytest.mark.parametrize("seconds,expected", [
        (3, "instant"),
        (45, "seconds"),
        (600, "minutes"),
        (7200, "hours"),
        (100000, "days"),
    ])
    def test_buckets(self, analysis_db, seconds, expected):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, duration_seconds=seconds, description="x")
        analyze.clean_duration(conn)

        cur = conn.cursor()
        cur.execute("SELECT duration_bucket FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == expected

    def test_null_and_zero(self, analysis_db):
        conn, _ = analysis_db
        sid_null = _insert_sighting(conn, description="x")
        sid_zero = _insert_sighting(conn, duration_seconds=0, description="y")
        analyze.clean_duration(conn)

        cur = conn.cursor()
        cur.execute("SELECT duration_bucket FROM sighting WHERE id IN (?, ?)",
                    (sid_null, sid_zero))
        for (bucket,) in cur.fetchall():
            assert bucket is None


# ============================================================
# 9. Public-field derivations
# ============================================================

class TestDerivePublicFields:
    def test_lat_lng_denormalized(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(
            conn, description="x", latitude=40.7, longitude=-74.0,
        )
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur = conn.cursor()
        cur.execute("SELECT lat, lng FROM sighting WHERE id = ?", (sid,))
        lat, lng = cur.fetchone()
        assert lat == pytest.approx(40.7)
        assert lng == pytest.approx(-74.0)

    def test_null_coords_stay_null(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur = conn.cursor()
        cur.execute("SELECT lat, lng FROM sighting WHERE id = ?", (sid,))
        lat, lng = cur.fetchone()
        assert lat is None
        assert lng is None

    def test_has_description_truthy(self, analysis_db):
        conn, _ = analysis_db
        sid_desc = _insert_sighting(conn, description="something happened")
        sid_summ = _insert_sighting(conn, description=None, summary="a summary")
        sid_none = _insert_sighting(conn, description=None)
        sid_blank = _insert_sighting(conn, description="   ")
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur = conn.cursor()
        cur.execute("SELECT id, has_description FROM sighting ORDER BY id")
        got = dict(cur.fetchall())
        assert got[sid_desc] == 1
        assert got[sid_summ] == 1
        assert got[sid_none] == 0
        assert got[sid_blank] == 0

    def test_has_media_from_text(self, analysis_db):
        conn, _ = analysis_db
        sid_photo = _insert_sighting(conn, description="I took a photo of the object.")
        sid_video = _insert_sighting(conn, description="My friend recorded video footage.")
        sid_none = _insert_sighting(conn, description="Just saw it with my eyes.")
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur = conn.cursor()
        cur.execute("SELECT id, has_media FROM sighting ORDER BY id")
        got = dict(cur.fetchall())
        assert got[sid_photo] == 1
        assert got[sid_video] == 1
        assert got[sid_none] == 0

    def test_has_media_from_attachment(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="No media mention here.")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO attachment (sighting_id, url, file_type) VALUES (?, ?, ?)",
            (sid, "http://example.com/x.jpg", "image"),
        )
        conn.commit()
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur.execute("SELECT has_media FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == 1

    def test_sighting_datetime_date_only(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, date_event="2020-05-15", description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur = conn.cursor()
        cur.execute("SELECT sighting_datetime FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "2020-05-15"

    def test_sighting_datetime_with_time_raw(self, analysis_db):
        conn, _ = analysis_db
        cur = conn.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, time_raw, location_id, description) "
            "VALUES (1, '2020-05-15', '9:30 PM', ?, 'x')",
            (loc,),
        )
        sid = cur.lastrowid
        conn.commit()
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur.execute("SELECT sighting_datetime FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "2020-05-15T21:30:00"

    def test_sighting_datetime_year_only_passthrough(self, analysis_db):
        conn, _ = analysis_db
        cur = conn.cursor()
        cur.execute("INSERT INTO location (raw_text) VALUES ('x')")
        loc = cur.lastrowid
        cur.execute(
            "INSERT INTO sighting (source_db_id, date_event, time_raw, location_id, description) "
            "VALUES (1, '1957', '9:30 PM', ?, 'x')",
            (loc,),
        )
        sid = cur.lastrowid
        conn.commit()
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur.execute("SELECT sighting_datetime FROM sighting WHERE id = ?", (sid,))
        # Year-only date: time not appended (only full YYYY-MM-DD gets a time suffix)
        assert cur.fetchone()[0] == "1957"

    def test_sighting_datetime_null_when_no_date(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, date_event=None, description="x")
        analyze.ensure_analysis_rows(conn)
        analyze.derive_public_fields(conn)

        cur = conn.cursor()
        cur.execute("SELECT sighting_datetime FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] is None


# ============================================================
# Idempotency
# ============================================================

class TestIdempotency:
    def test_run_analysis_twice(self, analysis_db):
        conn, db_path = analysis_db
        _insert_sighting(
            conn,
            description="Bright red object hovered silently at 9pm to the north.",
            shape="sphere",
            duration_seconds=120,
            num_witnesses=2,
            latitude=40.0,
            longitude=-100.0,
        )
        conn.close()  # run_analysis opens its own connection

        analyze.run_analysis(db_path)
        analyze.run_analysis(db_path)

        conn2 = sqlite3.connect(db_path)
        cur = conn2.cursor()
        cur.execute("SELECT COUNT(*) FROM sighting_analysis")
        assert cur.fetchone()[0] == 1  # no duplicates
        cur.execute("""
            SELECT standardized_shape, quality_score, richness_score, duration_bucket,
                   primary_color, lat, lng, has_description, has_media,
                   has_movement_mentioned, movement_categories
            FROM sighting
        """)
        (shape, q, rich, bucket, color, lat, lng, has_desc, has_media,
         has_mov, mov_cats_json) = cur.fetchone()
        assert shape == "Sphere"
        assert q is not None
        assert rich is not None and rich > 0
        assert bucket == "minutes"
        assert color == "bright red"
        assert lat == pytest.approx(40.0)
        assert lng == pytest.approx(-100.0)
        assert has_desc == 1
        assert has_media == 0
        assert has_mov == 1  # "hovered silently" in the description
        mov_cats = json.loads(mov_cats_json)
        assert "hovering" in mov_cats
        conn2.close()
