"""Tests for the UFO deduplication pipeline (dedup.py).

Covers pure-function unit tests, DB integration tests for data loading and
candidate insertion, and end-to-end tier integration tests using an in-memory
SQLite database with synthetic sighting data.
"""
import pytest

from dedup import (
    strip_nuforc_prefix,
    strip_mufon_boilerplate,
    token_jaccard,
    compute_similarity,
    normalize_city,
    parse_ufosearch_city_state,
    load_source_sightings,
    load_source_sightings_city_only,
    insert_candidates,
    create_indexes,
    tier_1a,
    tier_2,
    tier_3,
    SRC_MUFON,
    SRC_NUFORC,
    SRC_UFOCAT,
    SRC_UPDB,
    SRC_UFOSEARCH,
)
from tests.conftest import insert_test_sighting


# ============================================================
# Synthetic test descriptions
# ============================================================

DESC_ORB = (
    "Bright orange orb hovering silently above the treeline for approximately "
    "ten minutes before accelerating rapidly to the northeast and disappearing."
)
DESC_ORB_NUFORC = f"NUFORC UFO Sighting 78432 {DESC_ORB}"

DESC_TRIANGLE = (
    "Triangular formation of three white lights observed moving south at "
    "high altitude over the residential area."
)
DESC_MUFON_BOILERPLATE = (
    f"Submitted by razor via e-mail. Investigator Notes: {DESC_TRIANGLE}"
)

DESC_MODERATE_A = (
    "Large triangular craft with red and white lights seen hovering over downtown area."
)
DESC_MODERATE_B = (
    "Triangle-shaped object with red lights spotted near the city center at low altitude."
)

DESC_UNRELATED = "Strange humming noise coming from underground during midnight thunderstorm."

DESC_SHORT = "A bright light."


# ============================================================
# Group A: normalize_city
# ============================================================

class TestNormalizeCity:
    def test_basic_uppercase(self):
        assert normalize_city("phoenix") == "PHOENIX"

    def test_already_upper(self):
        assert normalize_city("PHOENIX") == "PHOENIX"

    def test_strip_parenthetical(self):
        assert normalize_city("Springfield (North)") == "SPRINGFIELD"

    def test_strip_question_mark(self):
        assert normalize_city("Phoenix?") == "PHOENIX"

    def test_strip_period(self):
        assert normalize_city("Phoenix.") == "PHOENIX"

    def test_strip_exclamation(self):
        assert normalize_city("Phoenix!") == "PHOENIX"

    def test_collapse_whitespace(self):
        assert normalize_city("San  Francisco") == "SAN FRANCISCO"

    def test_leading_trailing_whitespace(self):
        assert normalize_city("  Phoenix  ") == "PHOENIX"

    def test_empty_string(self):
        assert normalize_city("") == ""

    def test_none(self):
        assert normalize_city(None) == ""

    def test_paren_and_question(self):
        # Regex removes parens first but ? after ) prevents \s*$ from matching,
        # so only the trailing ? is removed by the second regex.
        assert normalize_city("Springfield (IL)?") == "SPRINGFIELD (IL)"

    def test_unicode_city(self):
        result = normalize_city("S\u00e3o Paulo")
        assert result == "S\u00c3O PAULO"

    def test_only_whitespace(self):
        assert normalize_city("   ") == ""

    def test_mid_string_parens_preserved(self):
        # Parens not at end should be preserved by the regex (uses $)
        # Actually the regex r'\s*\(.*\)\s*$' with greedy .* will match from
        # first ( to last ) if ) is at end. For mid-string parens with text
        # after, the $ anchor prevents matching.
        result = normalize_city("(North) Springfield")
        assert "SPRINGFIELD" in result


# ============================================================
# Group B: parse_ufosearch_city_state
# ============================================================

class TestParseUfosearchCityState:
    def test_basic_city_state(self):
        assert parse_ufosearch_city_state("Phoenix, AZ") == ("PHOENIX", "AZ")

    def test_city_state_with_question_mark(self):
        assert parse_ufosearch_city_state("Phoenix, AZ?") == ("PHOENIX", "AZ")

    def test_multi_word_city(self):
        assert parse_ufosearch_city_state("San Francisco, CA") == ("SAN FRANCISCO", "CA")

    def test_canadian_province(self):
        assert parse_ufosearch_city_state("Toronto, ON") == ("TORONTO", "ON")

    def test_invalid_state_code(self):
        assert parse_ufosearch_city_state("London, XX") == (None, None)

    def test_no_comma(self):
        assert parse_ufosearch_city_state("Phoenix AZ") == (None, None)

    def test_three_letter_state(self):
        assert parse_ufosearch_city_state("Phoenix, AZZ") == (None, None)

    def test_empty_string(self):
        assert parse_ufosearch_city_state("") == (None, None)

    def test_none(self):
        assert parse_ufosearch_city_state(None) == (None, None)

    def test_lowercase_input(self):
        # Regex uses re.I flag so lowercase should match
        assert parse_ufosearch_city_state("phoenix, az") == ("PHOENIX", "AZ")

    def test_extra_whitespace(self):
        assert parse_ufosearch_city_state("  Phoenix ,  AZ  ") == ("PHOENIX", "AZ")

    def test_city_with_period(self):
        assert parse_ufosearch_city_state("St. Louis, MO") == ("ST. LOUIS", "MO")

    def test_no_city(self):
        # Regex .+? requires at least one char before comma
        assert parse_ufosearch_city_state(", AZ") == (None, None)


# ============================================================
# Group C: strip_nuforc_prefix
# ============================================================

class TestStripNuforcPrefix:
    def test_with_prefix(self):
        result = strip_nuforc_prefix("NUFORC UFO Sighting 12345 Bright light seen")
        assert result == "Bright light seen"

    def test_no_prefix(self):
        assert strip_nuforc_prefix("Bright light seen") == "Bright light seen"

    def test_prefix_no_number(self):
        # startswith check passes but regex won't match \d+, so re.sub returns original
        original = "NUFORC UFO Sighting description here"
        assert strip_nuforc_prefix(original) == original

    def test_empty(self):
        assert strip_nuforc_prefix("") == ""

    def test_none(self):
        assert strip_nuforc_prefix(None) is None

    def test_prefix_only(self):
        result = strip_nuforc_prefix("NUFORC UFO Sighting 99999")
        assert result == ""


# ============================================================
# Group D: strip_mufon_boilerplate
# ============================================================

class TestStripMufonBoilerplate:
    def test_with_boilerplate_investigator_notes(self):
        text = "Submitted by razor via e-mail foo bar Investigator Notes: The witness saw a light."
        assert strip_mufon_boilerplate(text) == "The witness saw a light."

    def test_with_boilerplate_investigators_note(self):
        text = "Submitted by razor via e-mail stuff Investigators Note: Actual content here."
        assert strip_mufon_boilerplate(text) == "Actual content here."

    def test_no_boilerplate(self):
        assert strip_mufon_boilerplate("Regular MUFON description") == "Regular MUFON description"

    def test_boilerplate_no_notes_section(self):
        text = "Submitted by razor via e-mail with nothing useful"
        # Has prefix but no "Investigator Notes" match -> returns original
        assert strip_mufon_boilerplate(text) == text

    def test_empty(self):
        assert strip_mufon_boilerplate("") == ""

    def test_none(self):
        assert strip_mufon_boilerplate(None) is None

    def test_boilerplate_beyond_60_chars(self):
        # Push "Submitted by razor via e-mail" past the 60-char check window
        text = "X" * 61 + "Submitted by razor via e-mail Investigator Notes: Content"
        assert strip_mufon_boilerplate(text) == text

    def test_boilerplate_within_60_chars(self):
        text = "Submitted by razor via e-mail. Investigator Notes: Found this."
        assert strip_mufon_boilerplate(text) == "Found this."


# ============================================================
# Group E: token_jaccard
# ============================================================

class TestTokenJaccard:
    def test_identical(self):
        assert token_jaccard("bright light in the sky", "bright light in the sky") == 1.0

    def test_no_overlap(self):
        assert token_jaccard("bright light sky", "dark object ground") == 0.0

    def test_partial_overlap(self):
        # shared: bright, light, the (3); union: bright, light, in, the, sky, over, ground (7)
        result = token_jaccard("bright light in the sky", "bright light over the ground")
        assert abs(result - 3.0 / 7.0) < 1e-9

    def test_empty_a(self):
        assert token_jaccard("", "something") == 0.0

    def test_empty_b(self):
        assert token_jaccard("something", "") == 0.0

    def test_both_empty(self):
        assert token_jaccard("", "") == 0.0

    def test_none_a(self):
        assert token_jaccard(None, "text") == 0.0

    def test_none_b(self):
        assert token_jaccard("text", None) == 0.0

    def test_case_insensitive(self):
        assert token_jaccard("BRIGHT LIGHT", "bright light") == 1.0

    def test_punctuation_ignored(self):
        # \w+ regex strips punctuation
        assert token_jaccard("bright, light!", "bright light") == 1.0

    def test_numbers_as_tokens(self):
        # shared: ufo, 123 (2); union: ufo, 123, sighting, report (4)
        result = token_jaccard("ufo 123 sighting", "ufo 123 report")
        assert abs(result - 0.5) < 1e-9

    def test_whitespace_only(self):
        assert token_jaccard("   ", "text") == 0.0


# ============================================================
# Group F: compute_similarity
# ============================================================

class TestComputeSimilarity:
    def test_none_desc_a(self):
        assert compute_similarity(None, "text", SRC_MUFON, SRC_NUFORC) == 0.0

    def test_none_desc_b(self):
        assert compute_similarity("text", None, SRC_MUFON, SRC_NUFORC) == 0.0

    def test_empty_desc_a(self):
        assert compute_similarity("", "text") == 0.0

    def test_nuforc_prefix_stripped(self):
        # After stripping NUFORC prefix, the descriptions should be nearly identical
        score = compute_similarity(DESC_ORB, DESC_ORB_NUFORC, None, SRC_NUFORC)
        assert score >= 0.9

    def test_mufon_boilerplate_stripped(self):
        score = compute_similarity(DESC_MUFON_BOILERPLATE, DESC_TRIANGLE, SRC_MUFON, None)
        assert score >= 0.9

    def test_starts_with_shortcut(self):
        # Identical text >= 20 chars triggers the starts-with shortcut -> 0.95
        long_text = "A" * 50
        assert compute_similarity(long_text, long_text) == 0.95

    def test_starts_with_too_short(self):
        # 19 chars: below the 20-char threshold for starts-with
        short_text = "A" * 19
        score = compute_similarity(short_text, short_text)
        # Should NOT be 0.95; falls through to Jaccard -> SequenceMatcher
        # For identical strings, SequenceMatcher returns 1.0
        # But first: Jaccard of "AAAAAAAAAAAAAAAAAAA" vs itself = 1.0 (>= 0.03)
        # Then SequenceMatcher("AAA...", "AAA...") = 1.0
        assert score != 0.95
        assert score > 0.0

    def test_identical_long_descriptions(self):
        # Identical 50-char+ strings trigger starts-with shortcut
        assert compute_similarity(DESC_ORB, DESC_ORB) == 0.95

    def test_completely_different(self):
        score = compute_similarity(DESC_ORB, DESC_UNRELATED)
        assert score < 0.15

    def test_moderate_similarity(self):
        score = compute_similarity(DESC_MODERATE_A, DESC_MODERATE_B)
        assert 0.1 < score < 0.8

    def test_no_source_specified(self):
        score = compute_similarity(DESC_ORB, DESC_ORB)
        assert score == 0.95

    def test_preprocessing_makes_empty(self):
        # NUFORC prefix that IS the entire description
        score = compute_similarity("NUFORC UFO Sighting 12345", "Some text", SRC_NUFORC, None)
        assert score == 0.0

    def test_truncation_at_1000_chars(self):
        # Build descriptions with many tokens sharing the first 1000 chars
        shared_words = " ".join(f"word{i}" for i in range(150))  # ~1000 chars of shared words
        a = shared_words + " " + " ".join(f"alpha{i}" for i in range(50))
        b = shared_words + " " + " ".join(f"bravo{i}" for i in range(50))
        score = compute_similarity(a, b)
        # Many shared tokens → high Jaccard → proceeds to SequenceMatcher
        # SequenceMatcher on first 1000 chars (mostly shared) → high score
        assert score > 0.7

    def test_symmetry(self):
        score_ab = compute_similarity(DESC_MODERATE_A, DESC_MODERATE_B, SRC_MUFON, SRC_NUFORC)
        score_ba = compute_similarity(DESC_MODERATE_B, DESC_MODERATE_A, SRC_NUFORC, SRC_MUFON)
        assert abs(score_ab - score_ba) < 0.01


# ============================================================
# Group G: insert_candidates (DB integration)
# ============================================================

class TestInsertCandidates:
    def test_basic_insert(self, clean_db):
        # Create sightings so FK constraints are satisfied
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "desc2")
        s3 = insert_test_sighting(clean_db, SRC_UFOCAT, "2005-06-15", "Phoenix", "AZ", "US", "desc3")

        candidates = [
            (s1, s2, 0.8, "test", "pending"),
            (s1, s3, 0.5, "test", "pending"),
            (s2, s3, 0.3, "test", "pending"),
        ]
        inserted = insert_candidates(clean_db, candidates)
        assert inserted == 3

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 3

    def test_a_less_than_b_normalization(self, clean_db):
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "desc2")

        # Pass (larger, smaller) — should be normalized to (smaller, larger)
        insert_candidates(clean_db, [(s2, s1, 0.8, "test", "pending")])

        cur = clean_db.cursor()
        cur.execute("SELECT sighting_id_a, sighting_id_b FROM duplicate_candidate")
        row = cur.fetchone()
        assert row[0] < row[1]

    def test_self_pair_skipped(self, clean_db):
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        inserted = insert_candidates(clean_db, [(s1, s1, 1.0, "test", "pending")])
        assert inserted == 0

    def test_duplicate_pair_ignored(self, clean_db):
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "desc2")

        insert_candidates(clean_db, [(s1, s2, 0.8, "test", "pending")])
        insert_candidates(clean_db, [(s1, s2, 0.9, "test2", "pending")])

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 1

    def test_empty_list(self, clean_db):
        assert insert_candidates(clean_db, []) == 0

    def test_reversed_duplicate_ignored(self, clean_db):
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "desc2")

        insert_candidates(clean_db, [(s1, s2, 0.8, "test", "pending")])
        insert_candidates(clean_db, [(s2, s1, 0.9, "test", "pending")])

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 1

    def test_batch_insert(self, clean_db):
        # Create 101 sightings
        ids = []
        for i in range(101):
            sid = insert_test_sighting(
                clean_db, SRC_MUFON, f"2005-01-{(i % 28) + 1:02d}",
                "Phoenix", "AZ", "US", f"desc {i}"
            )
            ids.append(sid)

        candidates = [
            (ids[0], ids[i], 0.5, "test", "pending") for i in range(1, 101)
        ]
        inserted = insert_candidates(clean_db, candidates)
        assert inserted == 100

    def test_mixed_valid_and_self_pairs(self, clean_db):
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "d1")
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "d2")
        s3 = insert_test_sighting(clean_db, SRC_UFOCAT, "2005-06-15", "Phoenix", "AZ", "US", "d3")

        candidates = [
            (s1, s2, 0.8, "test", "pending"),
            (s2, s2, 1.0, "test", "pending"),  # self-pair
            (s1, s3, 0.5, "test", "pending"),
        ]
        inserted = insert_candidates(clean_db, candidates)
        assert inserted == 2


# ============================================================
# Group H: load_source_sightings (DB integration)
# ============================================================

class TestLoadSourceSightings:
    def test_basic_loading(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-16", "Tucson", "AZ", "US", "desc2")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-17", "Mesa", "AZ", "US", "desc3")

        groups, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 3
        assert len(groups) == 3

    def test_grouping_by_date_city_state(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc2")

        groups, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 2
        assert len(groups) == 1  # same group
        key = list(groups.keys())[0]
        assert len(groups[key]) == 2

    def test_city_normalization(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "phoenix", "AZ", "US", "desc1")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "PHOENIX", "AZ", "US", "desc2")

        groups, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 2
        assert len(groups) == 1  # normalized to same key

    def test_null_date_excluded(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, None, "Phoenix", "AZ", "US", "desc1")

        groups, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 0

    def test_empty_city_excluded(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "", "AZ", "US", "desc1")

        groups, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 0

    def test_use_raw_text_as_city(self, clean_db):
        # UFOCAT style: city column may be empty but raw_text has the city
        insert_test_sighting(
            clean_db, SRC_UFOCAT, "2005-06-15", "", "AZ", "US", "desc1",
            raw_text="Springfield"
        )

        groups, count = load_source_sightings(
            clean_db, SRC_UFOCAT, use_raw_text_as_city=True
        )
        assert count == 1
        key = list(groups.keys())[0]
        assert key[1] == "SPRINGFIELD"  # normalized from raw_text

    def test_source_filter(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "desc2")

        groups, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 1

    def test_returns_count(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "d1")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "d2")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-16", "Tucson", "AZ", "US", "d3")

        _, count = load_source_sightings(clean_db, SRC_MUFON)
        assert count == 3


# ============================================================
# Group I: load_source_sightings_city_only (DB integration)
# ============================================================

class TestLoadSourceSightingsCityOnly:
    def test_basic_loading_ignores_state(self, clean_db):
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "Phoenix", "CA", "US", "desc2")

        groups, count = load_source_sightings_city_only(clean_db, SRC_UPDB)
        assert count == 2
        # Both should be in the same group since state is not part of the key
        assert len(groups) == 1

    def test_country_filter(self, clean_db):
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "Phoenix", "AZ", "US", "desc1")
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "London", "", "UK", "desc2")

        groups, count = load_source_sightings_city_only(clean_db, SRC_UPDB, country_filter="US")
        assert count == 1

    def test_empty_city_excluded(self, clean_db):
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "", "AZ", "US", "desc1")

        groups, count = load_source_sightings_city_only(clean_db, SRC_UPDB)
        assert count == 0

    def test_returns_3_tuple(self, clean_db):
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "Phoenix", "AZ", "US", "desc1")

        groups, _ = load_source_sightings_city_only(clean_db, SRC_UPDB)
        key = list(groups.keys())[0]
        item = groups[key][0]
        assert len(item) == 3  # (sighting_id, description, source_db_id)
        assert item[2] == SRC_UPDB


# ============================================================
# Group J: tier_1a (end-to-end integration)
# ============================================================

class TestTier1a:
    def test_matching_mufon_nuforc_pair(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB_NUFORC)
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT match_method, similarity_score FROM duplicate_candidate")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "tier1a_mufon_nuforc"

    def test_high_similarity_score(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB_NUFORC)
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT similarity_score FROM duplicate_candidate")
        score = cur.fetchone()[0]
        assert score >= 0.8

    def test_no_match_different_city(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Tucson", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 0

    def test_no_match_different_date(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2010-01-01", "Phoenix", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 0

    def test_no_match_same_source(self, clean_db):
        # Two MUFON sightings — tier_1a only crosses MUFON<->NUFORC
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 0

    def test_multiple_pairs_cartesian(self, clean_db):
        # 2 MUFON + 2 NUFORC on same date/city/state = 4 pairs
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "mufon desc 1")
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", "mufon desc 2")
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "nuforc desc 1")
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", "nuforc desc 2")
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
        assert cur.fetchone()[0] == 4

    def test_a_less_than_b_ordering(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB_NUFORC)
        create_indexes(clean_db)

        tier_1a(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT sighting_id_a, sighting_id_b FROM duplicate_candidate")
        for row in cur.fetchall():
            assert row[0] < row[1]


# ============================================================
# Group K: tier_2 (end-to-end integration)
# ============================================================

class TestTier2:
    def test_tier2a_mufon_ufocat(self, clean_db):
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        # UFOCAT: city in raw_text, city column can be empty
        insert_test_sighting(
            clean_db, SRC_UFOCAT, "2005-06-15", "", "AZ", "US", DESC_ORB,
            raw_text="Phoenix"
        )
        create_indexes(clean_db)

        tier_2(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT match_method FROM duplicate_candidate WHERE match_method = 'tier2a_mufon_ufocat'")
        rows = cur.fetchall()
        assert len(rows) >= 1

    def test_tier2b_nuforc_ufocat(self, clean_db):
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(
            clean_db, SRC_UFOCAT, "2005-06-15", "", "AZ", "US", DESC_ORB,
            raw_text="Phoenix"
        )
        create_indexes(clean_db)

        tier_2(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT match_method FROM duplicate_candidate WHERE match_method = 'tier2b_nuforc_ufocat'")
        rows = cur.fetchall()
        assert len(rows) >= 1

    def test_tier2c_updb_mufon_city_only(self, clean_db):
        # UPDB and MUFON on same date/city, different states — city-only matching
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "CA", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_2(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT match_method FROM duplicate_candidate WHERE match_method = 'tier2c_updb_mufon'")
        rows = cur.fetchall()
        assert len(rows) >= 1

    def test_tier2c_updb_non_us_excluded(self, clean_db):
        insert_test_sighting(clean_db, SRC_UPDB, "2005-06-15", "London", "", "UK", DESC_ORB)
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "London", "", "UK", DESC_ORB)
        create_indexes(clean_db)

        tier_2(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method LIKE 'tier2c%'")
        assert cur.fetchone()[0] == 0

    def test_tier2d_ufosearch_nuforc(self, clean_db):
        # UFO-search with parseable "City, ST" in raw_text
        insert_test_sighting(
            clean_db, SRC_UFOSEARCH, "2005-06-15", "", "", "US", DESC_ORB,
            raw_text="Phoenix, AZ"
        )
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_2(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT match_method FROM duplicate_candidate WHERE match_method = 'tier2d_ufosearch_nuforc'")
        rows = cur.fetchall()
        assert len(rows) >= 1

    def test_tier2d_ufosearch_unparseable_location(self, clean_db):
        insert_test_sighting(
            clean_db, SRC_UFOSEARCH, "2005-06-15", "", "", "US", DESC_ORB,
            raw_text="Unknown location somewhere"
        )
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_2(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method LIKE 'tier2d%'")
        assert cur.fetchone()[0] == 0

    def test_insert_or_ignore_no_duplicates_with_tier1(self, clean_db):
        # Run tier_1a first, then tier_2. Same pair should not be duplicated.
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_1a(clean_db)
        tier_2(clean_db)

        cur = clean_db.cursor()
        lo, hi = min(s1, s2), max(s1, s2)
        cur.execute(
            "SELECT COUNT(*) FROM duplicate_candidate WHERE sighting_id_a=? AND sighting_id_b=?",
            (lo, hi)
        )
        assert cur.fetchone()[0] == 1  # exactly once, not duplicated


# ============================================================
# Group L: tier_3 (end-to-end integration)
# ============================================================

class TestTier3:
    def test_finds_description_match(self, clean_db):
        # Same date, different cities (so tier 1/2 won't find them), similar descriptions
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Tucson", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT match_method, similarity_score FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        rows = cur.fetchall()
        assert len(rows) >= 1
        assert rows[0][1] >= 0.5

    def test_skips_existing_pairs(self, clean_db):
        s1 = insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        s2 = insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Tucson", "AZ", "US", DESC_ORB)

        # Pre-insert the pair
        insert_candidates(clean_db, [(s1, s2, 0.9, "tier1a_mufon_nuforc", "pending")])
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        assert cur.fetchone()[0] == 0  # skipped because it was already found

    def test_skips_dates_over_20_records(self, clean_db):
        # Insert 21 records on the same date from 2 sources — exceeds the <=20 filter
        for i in range(11):
            insert_test_sighting(
                clean_db, SRC_MUFON, "2005-06-15",
                f"City{i}", "AZ", "US", DESC_ORB
            )
        for i in range(11):
            insert_test_sighting(
                clean_db, SRC_NUFORC, "2005-06-15",
                f"Town{i}", "AZ", "US", DESC_ORB
            )
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        # 22 records on this date > 20, so tier 3 skips it
        assert cur.fetchone()[0] == 0

    def test_requires_multi_source_date(self, clean_db):
        # All from the same source — tier 3 requires src_cnt >= 2
        for i in range(5):
            insert_test_sighting(
                clean_db, SRC_MUFON, "2005-06-15",
                f"City{i}", "AZ", "US", DESC_ORB
            )
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        assert cur.fetchone()[0] == 0

    def test_jaccard_prefilter(self, clean_db):
        # Two descriptions with very low overlap (Jaccard < 0.25)
        insert_test_sighting(
            clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US",
            "Bright orange orb hovering above treeline northeast accelerating"
        )
        insert_test_sighting(
            clean_db, SRC_NUFORC, "2005-06-15", "Tucson", "AZ", "US",
            DESC_UNRELATED
        )
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        assert cur.fetchone()[0] == 0

    def test_score_threshold(self, clean_db):
        # Descriptions that share some words but are different enough to score < 0.5
        insert_test_sighting(
            clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US",
            "Bright light seen in the northern sky moving slowly eastward at dusk"
        )
        insert_test_sighting(
            clean_db, SRC_NUFORC, "2005-06-15", "Tucson", "AZ", "US",
            "Bright light appeared over the western horizon at dawn rapidly descending south"
        )
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        # Even if a pair gets past the Jaccard pre-filter, score < 0.5 means no candidate
        count = cur.fetchone()[0]
        # We accept 0 candidates (filtered by either Jaccard or score threshold)
        assert count == 0

    def test_cross_source_only(self, clean_db):
        # 2 MUFON + 1 NUFORC — tier 3 should only compare cross-source pairs
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_MUFON, "2005-06-15", "Tucson", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005-06-15", "Mesa", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT sighting_id_a, sighting_id_b FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        rows = cur.fetchall()
        # Should only have MUFON<->NUFORC pairs, never MUFON<->MUFON
        # Get source IDs for each sighting
        for a_id, b_id in rows:
            cur.execute("SELECT source_db_id FROM sighting WHERE id IN (?, ?)", (a_id, b_id))
            sources = {r[0] for r in cur.fetchall()}
            assert len(sources) == 2  # cross-source

    def test_short_date_excluded(self, clean_db):
        # date_event with fewer than 10 chars should be excluded by LENGTH filter
        insert_test_sighting(clean_db, SRC_MUFON, "2005", "Phoenix", "AZ", "US", DESC_ORB)
        insert_test_sighting(clean_db, SRC_NUFORC, "2005", "Tucson", "AZ", "US", DESC_ORB)
        create_indexes(clean_db)

        tier_3(clean_db)

        cur = clean_db.cursor()
        cur.execute("SELECT COUNT(*) FROM duplicate_candidate WHERE match_method = 'tier3_desc_fuzzy'")
        assert cur.fetchone()[0] == 0


# ============================================================
# Group M: Edge cases
# ============================================================

class TestEdgeCases:
    def test_unicode_descriptions(self):
        desc = "UFO observ\u00e9 au-dessus de la rivi\u00e8re \u2014 tr\u00e8s brillant et silencieux"
        score = compute_similarity(desc, desc)
        assert score == 0.95  # starts-with shortcut for identical >= 20 chars

    def test_very_long_description(self):
        desc = "x " * 5000  # 10000 chars
        score = compute_similarity(desc, desc)
        # Identical, >= 20 chars, starts-with shortcut
        assert score == 0.95

    def test_punctuation_only_description(self):
        score = token_jaccard("!!??...", "!!??...")
        # No \w+ tokens found
        assert score == 0.0

    def test_newlines_in_description(self):
        desc_a = "Bright light\nobserved in the\nsky above"
        desc_b = "Bright light observed in the sky above"
        score = token_jaccard(desc_a, desc_b)
        # \w+ matches across newlines; tokens should be the same
        assert score == 1.0

    def test_multiple_parentheticals_in_city(self):
        # Greedy .* in regex matches from first ( to last )
        result = normalize_city("Springfield (North) (IL)")
        assert result == "SPRINGFIELD"

    def test_compute_similarity_symmetry(self):
        score_ab = compute_similarity(DESC_MODERATE_A, DESC_MODERATE_B, SRC_MUFON, SRC_NUFORC)
        score_ba = compute_similarity(DESC_MODERATE_B, DESC_MODERATE_A, SRC_NUFORC, SRC_MUFON)
        assert abs(score_ab - score_ba) < 0.01
