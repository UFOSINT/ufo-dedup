"""
Derived insight analysis for UFO sighting records.

Produces legally-safe, non-copyrighted features from the raw imported data
so the public site can filter and visualize without exposing source narratives.

The pipeline is defined in ANALYSIS_STEPS as an ordered list of
(name, function, label) tuples. `run_analysis` iterates the list; new
steps (e.g. a future offline LLM enrichment) plug in by appending to
the list — no edit to run_analysis required.

Steps (in current order; later ones may depend on earlier ones):
  1. normalize_and_cluster_shapes  -> standardized_shape, raw_shape_matched_via
  2. classify_movement             -> behavior_tags, movement_type,
                                      has_movement_mentioned, movement_categories (JSON)
  3. extract_colors                -> primary_color, color_list (JSON)
  4. derive_sentiment_summary      -> sentiment_score, dominant_emotion, emotion_scores (JSON)
  5. clean_duration                -> duration_bucket
  6. derive_public_fields          -> lat, lng, sighting_datetime, has_description, has_media
  7. calculate_quality_score       -> quality_score, richness_score
                                      (reads has_media / has_movement_mentioned /
                                       num_witnesses / date_event from step 2 & 6)
  8. flag_potential_hoaxes         -> hoax_likelihood, hoax_flags (JSON)
                                      (reads richness_score from step 7)
  9. run_topic_modeling            -> STUB (topic_id stays NULL until v0.9)

All functions write through sqlite batched UPDATEs. No pandas/torch/sklearn.

Usage:
    python analyze.py              # Run full derived analysis
    python analyze.py --stats-only # Print current derived stats
    python analyze.py --reset      # Null derived columns and re-run
"""
import sqlite3
import os
import sys
import re
import json
import time
from collections import Counter

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
BATCH_SIZE = 5000


# ============================================================
# Canonical vocabularies
# ============================================================

# Canonical shapes — used for fuzzy matching raw shape strings.
CANONICAL_SHAPES = [
    "Sphere", "Disc", "Triangle", "Cigar", "Oval", "Circle", "Light",
    "Fireball", "Cylinder", "Diamond", "Rectangle", "Chevron", "Cross",
    "Teardrop", "Star", "Egg", "Cone", "Cube", "Saucer", "Boomerang",
    "Flash", "Formation", "Changing", "Unknown", "Other",
]

# Substring hints for shapes that won't fuzzy-match the canonical token directly
# (e.g. "triangular" -> "Triangle", "disc-shaped" -> "Disc").
SHAPE_SUBSTRING_HINTS = {
    "triangular": "Triangle",
    "triangle": "Triangle",
    "disc": "Disc",
    "disk": "Disc",
    "saucer": "Saucer",
    "cigar": "Cigar",
    "sphere": "Sphere",
    "spherical": "Sphere",
    "orb": "Sphere",
    "ball": "Sphere",
    "round": "Circle",
    "circular": "Circle",
    "oval": "Oval",
    "egg": "Egg",
    "cylinder": "Cylinder",
    "cylindrical": "Cylinder",
    "tube": "Cylinder",
    "cone": "Cone",
    "chevron": "Chevron",
    "boomerang": "Boomerang",
    "rectangle": "Rectangle",
    "rectangular": "Rectangle",
    "diamond": "Diamond",
    "teardrop": "Teardrop",
    "fireball": "Fireball",
    "flash": "Flash",
    "light": "Light",
    "star": "Star",
    "cube": "Cube",
    "cross": "Cross",
    "formation": "Formation",
    "changing": "Changing",
    "unknown": "Unknown",
}

# Behavior keyword -> tag name
BEHAVIOR_KEYWORDS = {
    "hovering": [r"\bhover(?:ed|ing|s)?\b", r"\bstationary\b", r"\bsuspended\b"],
    "silent": [r"\bsilent\b", r"\bno sound\b", r"\bmade no noise\b", r"\bsoundless\b"],
    "bright": [r"\bbright\b", r"\bbrilliant\b", r"\bglowing\b", r"\billuminated\b"],
    "pulsing": [r"\bpuls(?:ed|ing|ates?)\b", r"\bflash(?:ed|ing)\b", r"\bblink(?:ed|ing)?\b"],
    "rotating": [r"\brotat(?:ed|ing|es)\b", r"\bspin(?:ning)?\b", r"\brevolv(?:ed|ing)\b"],
    "zigzag": [r"\bzig.?zag\b", r"\berratic\b", r"\bzipped\b"],
    "vanished": [r"\bvanish(?:ed|es|ing)?\b", r"\bdisappear(?:ed)?\b", r"\bgone in\b"],
    "accelerated": [r"\baccelerat(?:ed|ing|ion)\b", r"\bhigh speed\b", r"\bshot (?:off|up|away)\b"],
    "split": [r"\bsplit\b", r"\bdivided\b", r"\bsepar(?:ated|ating)\b"],
    "merged": [r"\bmerged\b", r"\bjoined\b", r"\bcombined\b"],
    "formation": [r"\bformation\b", r"\bin a line\b", r"\bv-shape\b"],
    "chased": [r"\bchased\b", r"\bpursued\b"],
    "followed": [r"\bfollowed\b", r"\btrail(?:ed|ing)\b"],
    "landed": [r"\blanded\b", r"\btouched down\b", r"\bon the ground\b"],
}

# Compiled once for speed
_BEHAVIOR_PATTERNS = {
    tag: [re.compile(p, re.IGNORECASE) for p in pats]
    for tag, pats in BEHAVIOR_KEYWORDS.items()
}

# Movement-only category taxonomy (v0.8.3, per science team brief).
# Narrower than BEHAVIOR_KEYWORDS — each category describes HOW the object
# moved, not how it looked or sounded. `has_movement_mentioned` is True if
# ANY of these fire. `movement_categories` is the JSON array of every
# category that fired. New categories can be appended without touching the
# classifier — the regex iteration below picks them up automatically.
MOVEMENT_CATEGORY_PATTERNS = {
    "hovering":     [r"\bhover(?:ed|ing|s)?\b", r"\bstationary\b", r"\bsuspended\b", r"\bmotionless\b"],
    "linear":       [r"\bstraight line\b", r"\bstraight path\b", r"\bin a line\b",
                     r"\bheaded (?:north|south|east|west)\b"],
    "erratic":      [r"\bzig.?zag\b", r"\berratic\b", r"\bdarted\b", r"\bjerky\b"],
    "accelerating": [r"\baccelerat\w*\b", r"\bshot (?:off|up|away|out)\b",
                     r"\bhigh speed\b", r"\bsped (?:off|away)\b", r"\bzipped\b"],
    "rotating":     [r"\brotat\w*\b", r"\bspin(?:ning|ned)?\b", r"\brevolv\w*\b", r"\bwobbl\w*\b"],
    "ascending":    [r"\bascend\w*\b", r"\bclimb(?:ed|ing)?\b", r"\bshot up\b",
                     r"\bstraight up\b", r"\bupward\b"],
    "descending":   [r"\bdescend\w*\b", r"\bdropp\w*\b", r"\bfell\b", r"\bdownward\b",
                     r"\bplummet\w*\b"],
    "vanished":     [r"\bvanish\w*\b", r"\bdisappear\w*\b", r"\bgone in\b", r"\bfaded\b"],
    "followed":     [r"\bfollow(?:ed|ing)?\b", r"\btrail(?:ed|ing)?\b",
                     r"\bchased\b", r"\bpursued\b"],
    "landed":       [r"\bland(?:ed|ing)?\b", r"\btouched down\b", r"\bon the ground\b"],
}

_MOVEMENT_CATEGORY_RE = {
    cat: [re.compile(p, re.IGNORECASE) for p in pats]
    for cat, pats in MOVEMENT_CATEGORY_PATTERNS.items()
}

# Color whitelist — compound forms first so longer matches win
COLOR_WORDS = [
    "metallic silver", "bright white", "bright red", "bright green",
    "red", "orange", "yellow", "green", "blue", "purple", "violet",
    "white", "black", "silver", "gold", "gray", "grey", "pink",
    "amber", "brown", "turquoise",
]
_COLOR_PATTERNS = [
    (c, re.compile(r"\b" + re.escape(c) + r"\b", re.IGNORECASE))
    for c in COLOR_WORDS
]

EMOTION_KEYS = [
    "joy", "fear", "anger", "sadness",
    "surprise", "disgust", "trust", "anticipation",
]

# Hoax detection — canned phrases that show up copy-pasted across bad reports
GENERIC_PHRASE_PATTERNS = [
    re.compile(r"^saw a ufo\.?$", re.IGNORECASE),
    re.compile(r"^ufo sighting\.?$", re.IGNORECASE),
    re.compile(r"^strange lights?\.?$", re.IGNORECASE),
    re.compile(r"^unknown object\.?$", re.IGNORECASE),
    re.compile(r"^i saw something\.?$", re.IGNORECASE),
]

DRAMATIC_KEYWORDS = re.compile(
    r"\b(alien|abduct(?:ed|ion)|probed|reptilian|grey|illuminati)\b",
    re.IGNORECASE,
)

TIME_OF_DAY_RE = re.compile(
    r"\b(dawn|dusk|morning|afternoon|evening|night|midnight|noon|\d{1,2}\s*(?:am|pm))\b",
    re.IGNORECASE,
)
DIRECTION_RE = re.compile(
    r"\b(north|south|east|west|northeast|northwest|southeast|southwest|"
    r"n\.?e\.?|n\.?w\.?|s\.?e\.?|s\.?w\.?)\b",
    re.IGNORECASE,
)
ALTITUDE_RE = re.compile(
    r"\b(altitude|overhead|treetop|low in the sky|high in the sky|\d+\s*(?:feet|ft|meters?))\b",
    re.IGNORECASE,
)

# Media mentions in descriptions — feeds has_media
MEDIA_RE = re.compile(
    r"\b(photo(?:graph(?:ed|s)?)?|picture(?:s|d)?|\bpic(?:s)?\b|video(?:s|ed)?|"
    r"footage|film(?:ed|ing)?|recording|recorded|camera|camcorder|snapshot|"
    r"cellphone picture|cell phone picture|phone (?:photo|video|pic))\b",
    re.IGNORECASE,
)

# Time-of-day in a raw time_raw string
TIME_HHMM_RE = re.compile(r"\b(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm)?\b", re.IGNORECASE)


# ============================================================
# Helpers
# ============================================================

def _pragmas(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


def _executemany_batched(conn, sql, rows):
    """Commit in BATCH_SIZE chunks for large updates."""
    cur = conn.cursor()
    for i in range(0, len(rows), BATCH_SIZE):
        cur.executemany(sql, rows[i:i + BATCH_SIZE])
        conn.commit()


def ensure_analysis_rows(conn):
    """Create a sighting_analysis row for every sighting that doesn't have one."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sighting_analysis (sighting_id)
        SELECT s.id FROM sighting s
        LEFT JOIN sighting_analysis a ON s.id = a.sighting_id
        WHERE a.id IS NULL
    """)
    conn.commit()
    return cur.rowcount


# ============================================================
# 1. Shape normalization
# ============================================================

def _normalize_shape_token(raw):
    """Lowercase, strip punctuation and trailing plural 's'."""
    if raw is None:
        return ""
    s = raw.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", " ", s)
    # Strip a lone trailing 's' (discs -> disc) but leave words like "cross".
    if len(s) > 3 and s.endswith("s") and not s.endswith("ss"):
        s = s[:-1]
    return s


def _match_shape(token, fuzzy_fn):
    """Return (canonical_shape, match_method)."""
    if not token:
        return None, None

    canonical_lower = {c.lower(): c for c in CANONICAL_SHAPES}

    # Exact
    if token in canonical_lower:
        return canonical_lower[token], "exact"

    # Substring hints
    for needle, target in SHAPE_SUBSTRING_HINTS.items():
        if needle in token:
            return target, "substring"

    # Fuzzy
    if fuzzy_fn is not None:
        result = fuzzy_fn(token, CANONICAL_SHAPES, score_cutoff=85)
        if result is not None:
            return result[0], "fuzzy"

    return "Other", "unmatched"


def normalize_and_cluster_shapes(conn):
    """Assign a canonical shape to every sighting with a raw shape value."""
    try:
        from rapidfuzz import process as rf_process
        def fuzzy_fn(token, choices, score_cutoff):
            return rf_process.extractOne(token, choices, score_cutoff=score_cutoff)
    except ImportError:
        print("  (rapidfuzz not installed — falling back to exact/substring only)")
        fuzzy_fn = None

    cur = conn.cursor()
    cur.execute("SELECT id, shape FROM sighting WHERE shape IS NOT NULL")
    rows = cur.fetchall()

    sighting_updates = []
    analysis_updates = []
    stats = Counter()

    for sid, raw_shape in rows:
        token = _normalize_shape_token(raw_shape)
        canonical, method = _match_shape(token, fuzzy_fn)
        stats[method or "null"] += 1
        sighting_updates.append((canonical, sid))
        analysis_updates.append((method, sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET standardized_shape = ? WHERE id = ?",
        sighting_updates,
    )
    _executemany_batched(
        conn,
        "UPDATE sighting_analysis SET raw_shape_matched_via = ? WHERE sighting_id = ?",
        analysis_updates,
    )

    total = sum(stats.values())
    print(f"  Shapes normalized: {total:,} "
          f"(exact={stats['exact']:,}, substring={stats['substring']:,}, "
          f"fuzzy={stats['fuzzy']:,}, unmatched={stats['unmatched']:,})")


# ============================================================
# 2. Movement / behavior classification
# ============================================================

def _classify_text_behavior(text):
    """Return (behavior_tags_list, movement_type)."""
    if not text:
        return [], None

    tags = []
    for tag, patterns in _BEHAVIOR_PATTERNS.items():
        if any(p.search(text) for p in patterns):
            tags.append(tag)

    # Derive a single movement_type from the tags
    if "hovering" in tags:
        movement = "hover"
    elif "accelerated" in tags:
        movement = "fast"
    elif "zigzag" in tags:
        movement = "erratic"
    elif "vanished" in tags or "followed" in tags or "chased" in tags:
        movement = "linear"
    elif "landed" in tags:
        movement = "stationary"
    elif tags:
        movement = "linear"
    else:
        movement = None

    return tags, movement


def _classify_text_movement_categories(text):
    """Return list of movement category names that fired for `text`."""
    if not text:
        return []
    fired = []
    for cat, patterns in _MOVEMENT_CATEGORY_RE.items():
        if any(p.search(text) for p in patterns):
            fired.append(cat)
    return fired


def classify_movement(conn):
    """Regex-scan descriptions for movement/behavior patterns.

    Writes four derived values per row that has text:
      - sighting.movement_type              (scalar, legacy)
      - sighting.has_movement_mentioned     (0/1, new in v0.8.3)
      - sighting.movement_categories        (JSON array, new in v0.8.3)
      - sighting_analysis.behavior_tags     (JSON array, legacy)
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT id, COALESCE(description, summary) FROM sighting
        WHERE description IS NOT NULL OR summary IS NOT NULL
    """)
    rows = cur.fetchall()

    sighting_updates = []
    analysis_updates = []
    tag_counter = Counter()
    cat_counter = Counter()
    has_mov_count = 0

    for sid, text in rows:
        tags, movement = _classify_text_behavior(text)
        categories = _classify_text_movement_categories(text)
        tag_counter.update(tags)
        cat_counter.update(categories)
        has_mov = 1 if categories else 0
        if has_mov:
            has_mov_count += 1

        # Always emit the row so has_movement_mentioned is explicitly 0
        # (not NULL) for text we scanned — distinguishes "scanned, found
        # nothing" from "no text to scan".
        sighting_updates.append((
            movement,
            has_mov,
            json.dumps(categories),
            sid,
        ))
        analysis_updates.append((json.dumps(tags), sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET movement_type = ?, "
        "has_movement_mentioned = ?, movement_categories = ? WHERE id = ?",
        sighting_updates,
    )
    _executemany_batched(
        conn,
        "UPDATE sighting_analysis SET behavior_tags = ? WHERE sighting_id = ?",
        analysis_updates,
    )

    top_tags = ", ".join(f"{k}={v:,}" for k, v in tag_counter.most_common(5))
    top_cats = ", ".join(f"{k}={v:,}" for k, v in cat_counter.most_common(5))
    print(f"  Movement classified: {len(sighting_updates):,} rows with text; "
          f"{has_mov_count:,} had at least one movement category")
    print(f"    top behavior tags:  {top_tags or '(none)'}")
    print(f"    top movement cats:  {top_cats or '(none)'}")


# ============================================================
# 3. Color extraction
# ============================================================

def _extract_text_colors(text):
    """Return (primary_color, color_list) — primary is first-occurring."""
    if not text:
        return None, []

    found = []  # preserves insertion order
    lowered = text.lower()
    taken_spans = []  # track matched spans to avoid 'red' matching inside 'bright red'

    def overlaps(start, end):
        return any(s < end and e > start for s, e in taken_spans)

    for color, pattern in _COLOR_PATTERNS:
        for m in pattern.finditer(lowered):
            if overlaps(m.start(), m.end()):
                continue
            taken_spans.append((m.start(), m.end()))
            if color not in found:
                found.append(color)

    if not found:
        return None, []
    return found[0], found


def extract_colors(conn):
    """Regex-scan descriptions for color words."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, COALESCE(description, summary) FROM sighting
        WHERE description IS NOT NULL OR summary IS NOT NULL
    """)
    rows = cur.fetchall()

    sighting_updates = []
    analysis_updates = []
    matched = 0

    for sid, text in rows:
        primary, colors = _extract_text_colors(text)
        if colors:
            matched += 1
            sighting_updates.append((primary, sid))
            analysis_updates.append((json.dumps(colors), sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET primary_color = ? WHERE id = ?",
        sighting_updates,
    )
    _executemany_batched(
        conn,
        "UPDATE sighting_analysis SET color_list = ? WHERE sighting_id = ?",
        analysis_updates,
    )

    print(f"  Colors extracted: {matched:,} sightings with at least one color")


# ============================================================
# 4. Sentiment derivation (from existing sentiment_analysis table)
# ============================================================

def derive_sentiment_summary(conn):
    """Copy VADER compound and compute dominant emotion / normalized scores."""
    cur = conn.cursor()
    cur.execute("""
        SELECT sighting_id, vader_compound,
               emo_joy, emo_fear, emo_anger, emo_sadness,
               emo_surprise, emo_disgust, emo_trust, emo_anticipation
        FROM sentiment_analysis
    """)
    rows = cur.fetchall()

    sighting_updates = []
    analysis_updates = []

    for row in rows:
        sid = row[0]
        compound = row[1]
        emos = dict(zip(EMOTION_KEYS, row[2:10]))

        total = sum(emos.values())
        if total > 0:
            normalized = {k: round(v / total, 4) for k, v in emos.items()}
            # argmax with a no-tie rule
            max_val = max(emos.values())
            top = [k for k, v in emos.items() if v == max_val]
            dominant = top[0] if len(top) == 1 else None
        else:
            normalized = {k: 0.0 for k in EMOTION_KEYS}
            dominant = None

        sighting_updates.append((compound, dominant, sid))
        analysis_updates.append((json.dumps(normalized), sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET sentiment_score = ?, dominant_emotion = ? WHERE id = ?",
        sighting_updates,
    )
    _executemany_batched(
        conn,
        "UPDATE sighting_analysis SET emotion_scores = ? WHERE sighting_id = ?",
        analysis_updates,
    )

    print(f"  Sentiment derived: {len(sighting_updates):,} sightings")


# ============================================================
# 5. Quality / richness score
# ============================================================

# Remaining structured fields (after description / witnesses / movement / media
# are split out as heavily-weighted primary features). 9 × 2 = 18 points max.
QUALITY_STRUCTURED_FIELDS = [
    "time_raw", "shape", "color", "duration",
    "sound", "direction", "elevation_angle",
    "hynek", "vallee",
]

# Unknown-date cap: if date_event is NULL, quality_score is capped.
# Science-team requirement in v0.8.3: "Add logic to flag or drop records
# with completely unknown dates (or give them very low quality)." We pick
# very low quality — non-destructive, already below the usual >=60 filter.
#
# v0.8.3 refinement: text-rich historical reports (NICAP, Blue Book, etc.)
# often have a prose date the parser couldn't normalize, but their narrative
# content is high-signal. For those rows (richness_features_count >= 8 AND
# has_description) the cap relaxes to UNKNOWN_DATE_CAP_RICH so they can
# land in the 20–35 range instead of being floored at 15.
UNKNOWN_DATE_CAP = 15
UNKNOWN_DATE_CAP_RICH = 35
UNKNOWN_DATE_RICH_MIN_FEATURES = 8


def _score_sighting_quality(row):
    """Compute (quality_score 0-100, richness_features_count).

    Weighting (v0.8.3, per science-team brief, gentler rebalance):
      description length    0 / 5 / 15 / 25
      has_media             +15
      num_witnesses tier    0 / 5 / 10 / 15     (0 / 1 / 2 / 3+)
      has_movement_mentioned +10, +5 if 2+ categories
      9 structured fields   3 pts each (max 27)
      coords                +5
      specificity bonus     +5   (time-of-day, direction, or altitude in desc)
      UNKNOWN DATE CAP      min(score, 15) if row['date_event'] is NULL
                            (relaxed to 35 if features >= 8 and has_description)

    Max theoretical: ~107 before the min(100, ...) clamp.
    """
    score = 0
    features = 0

    # Description length (0–25)
    desc = row["description"] or row["summary"] or ""
    desc_len = len(desc)
    if desc_len >= 200:
        score += 25; features += 1
    elif desc_len >= 50:
        score += 15; features += 1
    elif desc_len > 0:
        score += 5; features += 1

    # Has media (+15)
    if row["has_media"]:
        score += 15
        features += 1

    # Num witnesses tier (0–15)
    nw = row["num_witnesses"] or 0
    if nw >= 3:
        score += 15; features += 1
    elif nw == 2:
        score += 10; features += 1
    elif nw == 1:
        score += 5; features += 1

    # Movement (+10, +5 bonus if 2+ categories)
    if row["has_movement_mentioned"]:
        score += 10
        features += 1
        try:
            cats = json.loads(row["movement_categories"] or "[]")
        except (ValueError, TypeError):
            cats = []
        if len(cats) >= 2:
            score += 5

    # 9 remaining structured fields (3 pts each, max 27)
    for field in QUALITY_STRUCTURED_FIELDS:
        if row[field] not in (None, ""):
            score += 3
            features += 1

    # Coords (+5)
    if row["lat"] is not None and row["lng"] is not None:
        score += 5
        features += 1

    # Specificity bonus (+5)
    if desc and (TIME_OF_DAY_RE.search(desc) or DIRECTION_RE.search(desc) or ALTITUDE_RE.search(desc)):
        score += 5

    # Unknown-date cap — overrides everything above.
    # Relaxed cap for text-rich rows whose date just failed to parse.
    if not row["date_event"]:
        has_desc = bool(row["description"] or row["summary"])
        if features >= UNKNOWN_DATE_RICH_MIN_FEATURES and has_desc:
            cap = UNKNOWN_DATE_CAP_RICH
        else:
            cap = UNKNOWN_DATE_CAP
        score = min(score, cap)

    return min(100, score), features


def calculate_quality_score(conn):
    """Assign a 0-100 quality score + richness feature count (on sighting).

    Must run AFTER classify_movement (reads has_movement_mentioned +
    movement_categories) and derive_public_fields (reads has_media + lat/lng).
    """
    cur = conn.cursor()
    struct_sql = ", ".join(f"s.{f}" for f in QUALITY_STRUCTURED_FIELDS)
    cur.execute(f"""
        SELECT s.id, s.description, s.summary,
               s.date_event, {struct_sql},
               s.num_witnesses,
               s.has_media, s.has_movement_mentioned, s.movement_categories,
               s.lat, s.lng
        FROM sighting s
    """)
    col_names = [c[0] for c in cur.description]
    rows = cur.fetchall()

    sighting_updates = []

    for row in rows:
        row_dict = dict(zip(col_names, row))
        sid = row_dict["id"]
        score, features = _score_sighting_quality(row_dict)
        sighting_updates.append((score, features, sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET quality_score = ?, richness_score = ? WHERE id = ?",
        sighting_updates,
    )

    # Stats
    cur.execute("SELECT AVG(quality_score), MIN(quality_score), MAX(quality_score) FROM sighting WHERE quality_score IS NOT NULL")
    avg_q, min_q, max_q = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM sighting WHERE quality_score >= 60")
    high_q = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE date_event IS NULL AND quality_score <= ?",
        (UNKNOWN_DATE_CAP,),
    )
    date_capped_floor = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE date_event IS NULL "
        "AND quality_score > ? AND quality_score <= ?",
        (UNKNOWN_DATE_CAP, UNKNOWN_DATE_CAP_RICH),
    )
    date_capped_relaxed = cur.fetchone()[0]
    print(f"  Quality scored: {len(sighting_updates):,} "
          f"(avg={avg_q:.1f}, min={min_q}, max={max_q}, >=60: {high_q:,})")
    print(f"    unknown-date floor cap ({UNKNOWN_DATE_CAP}):  {date_capped_floor:,}")
    print(f"    unknown-date relaxed cap ({UNKNOWN_DATE_CAP_RICH}): {date_capped_relaxed:,}")


# ============================================================
# 6. Hoax flags
# ============================================================

HOAX_WEIGHTS = {
    "very_short_text": 0.2,
    "generic_phrasing": 0.3,
    "duplicate_phrasing": 0.4,
    "dramatic_no_specifics": 0.3,
    "all_caps_text": 0.15,
}


def _is_all_caps(text, min_len=20):
    if not text or len(text) < min_len:
        return False
    letters = [c for c in text if c.isalpha()]
    if len(letters) < min_len:
        return False
    upper = sum(1 for c in letters if c.isupper())
    return upper / len(letters) > 0.8


def flag_potential_hoaxes(conn):
    """Rule-based hoax likelihood and per-flag breakdown."""
    cur = conn.cursor()

    # Pre-compute duplicate phrasing: any 120-char description prefix seen >= 10 times
    print("  Scanning for duplicate phrasing...")
    cur.execute("""
        SELECT SUBSTR(description, 1, 120), COUNT(*) as c
        FROM sighting
        WHERE description IS NOT NULL AND LENGTH(description) >= 40
        GROUP BY SUBSTR(description, 1, 120)
        HAVING c >= 10
    """)
    dup_prefixes = {row[0] for row in cur.fetchall()}
    print(f"    Found {len(dup_prefixes):,} duplicate-prefix patterns")

    # Pull the features each rule needs
    cur.execute("""
        SELECT s.id, s.description, s.summary,
               COALESCE(s.richness_score, 0)
        FROM sighting s
    """)
    rows = cur.fetchall()

    sighting_updates = []
    analysis_updates = []
    flag_counter = Counter()

    for sid, description, summary, features_count in rows:
        text = description or summary or ""
        flags = []

        if text and len(text.strip()) < 20:
            flags.append("very_short_text")

        if text and any(p.match(text.strip()) for p in GENERIC_PHRASE_PATTERNS):
            flags.append("generic_phrasing")

        if description:
            prefix = description[:120]
            if prefix in dup_prefixes:
                flags.append("duplicate_phrasing")

        if text and DRAMATIC_KEYWORDS.search(text) and features_count < 3:
            flags.append("dramatic_no_specifics")

        if _is_all_caps(text):
            flags.append("all_caps_text")

        if flags:
            weight = min(1.0, sum(HOAX_WEIGHTS[f] for f in flags))
            flag_counter.update(flags)
            sighting_updates.append((weight, sid))
            analysis_updates.append((json.dumps(flags), sid))
        else:
            sighting_updates.append((0.0, sid))
            analysis_updates.append((json.dumps([]), sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET hoax_likelihood = ? WHERE id = ?",
        sighting_updates,
    )
    _executemany_batched(
        conn,
        "UPDATE sighting_analysis SET hoax_flags = ? WHERE sighting_id = ?",
        analysis_updates,
    )

    breakdown = ", ".join(f"{k}={v:,}" for k, v in flag_counter.most_common())
    print(f"  Hoax flagged: {sum(flag_counter.values()):,} total flag hits "
          f"({breakdown or 'none'})")


# ============================================================
# 7. Topic modeling (STUB — deferred to v0.9)
# ============================================================

def run_topic_modeling(conn):
    """Placeholder — BERTopic implementation deferred to v0.9."""
    print("  Topic modeling: [deferred to v0.9]")


# ============================================================
# 8. Duration bucketing
# ============================================================

def clean_duration(conn):
    """Map duration_seconds into coarse buckets."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE sighting SET duration_bucket = CASE
            WHEN duration_seconds IS NULL OR duration_seconds = 0 THEN NULL
            WHEN duration_seconds < 5 THEN 'instant'
            WHEN duration_seconds < 60 THEN 'seconds'
            WHEN duration_seconds < 3600 THEN 'minutes'
            WHEN duration_seconds < 86400 THEN 'hours'
            ELSE 'days'
        END
    """)
    conn.commit()

    cur.execute("""
        SELECT duration_bucket, COUNT(*) FROM sighting
        WHERE duration_bucket IS NOT NULL
        GROUP BY duration_bucket
    """)
    dist = dict(cur.fetchall())
    breakdown = ", ".join(f"{k}={v:,}" for k, v in dist.items())
    print(f"  Duration bucketed: {sum(dist.values()):,} ({breakdown})")


# ============================================================
# 9. Public-field derivations (lat/lng, datetime, has_*)
# ============================================================

def _build_sighting_datetime(date_event, time_raw):
    """Combine ISO date + raw time string into a best-effort ISO datetime.

    Rules:
      - date_event is the authoritative base (already normalized during import).
      - If date_event is NULL -> return None.
      - If date_event already contains 'T' or a space + digits, assume it has
        a time component and return as-is.
      - Otherwise try to parse time_raw as HH:MM[:SS] [am|pm]; if successful,
        concat as 'YYYY-MM-DDTHH:MM:SS'.
      - If no time is parseable, return date_event unchanged (date-only or year-only).
    """
    if not date_event:
        return None

    if "T" in date_event or re.search(r"\d{4}-\d{2}-\d{2}\s+\d{1,2}:", date_event):
        return date_event

    if not time_raw:
        return date_event

    m = TIME_HHMM_RE.search(time_raw)
    if not m:
        return date_event

    hour = int(m.group(1))
    minute = int(m.group(2))
    ampm = (m.group(3) or "").lower()
    if ampm == "pm" and hour < 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return date_event

    # date_event may be 'YYYY', 'YYYY-MM', or 'YYYY-MM-DD' — only append time
    # when we have a full date.
    if len(date_event) >= 10 and date_event[4] == "-" and date_event[7] == "-":
        return f"{date_event[:10]}T{hour:02d}:{minute:02d}:00"
    return date_event


def derive_public_fields(conn):
    """Denormalize lat/lng, build sighting_datetime, set has_description/has_media."""
    cur = conn.cursor()

    # 1. lat / lng — one SQL update via location JOIN
    cur.execute("""
        UPDATE sighting SET
            lat = (SELECT latitude  FROM location WHERE location.id = sighting.location_id),
            lng = (SELECT longitude FROM location WHERE location.id = sighting.location_id)
    """)
    conn.commit()

    # 2. has_description (no need for Python — trivial SQL)
    cur.execute("""
        UPDATE sighting SET has_description = CASE
            WHEN (description IS NOT NULL AND TRIM(description) != '')
              OR (summary IS NOT NULL AND TRIM(summary) != '')
            THEN 1 ELSE 0
        END
    """)
    conn.commit()

    # 3. sighting_datetime — needs Python to parse time_raw
    cur.execute("SELECT id, date_event, time_raw FROM sighting")
    dt_updates = [
        (_build_sighting_datetime(d, t), sid) for sid, d, t in cur.fetchall()
    ]
    _executemany_batched(
        conn,
        "UPDATE sighting SET sighting_datetime = ? WHERE id = ?",
        dt_updates,
    )

    # 4. has_media — regex on description/summary OR attachment row exists
    cur.execute("SELECT DISTINCT sighting_id FROM attachment WHERE sighting_id IS NOT NULL")
    has_attachment = {row[0] for row in cur.fetchall()}

    cur.execute("SELECT id, description, summary FROM sighting")
    media_updates = []
    for sid, desc, summ in cur.fetchall():
        text = desc or summ or ""
        flag = 1 if (sid in has_attachment or MEDIA_RE.search(text)) else 0
        media_updates.append((flag, sid))

    _executemany_batched(
        conn,
        "UPDATE sighting SET has_media = ? WHERE id = ?",
        media_updates,
    )

    # Stats
    cur.execute("SELECT COUNT(*) FROM sighting WHERE lat IS NOT NULL AND lng IS NOT NULL")
    coord_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sighting WHERE sighting_datetime IS NOT NULL")
    dt_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sighting WHERE has_description = 1")
    desc_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sighting WHERE has_media = 1")
    media_count = cur.fetchone()[0]
    print(f"  Public fields derived: "
          f"coords={coord_count:,}, datetime={dt_count:,}, "
          f"has_description={desc_count:,}, has_media={media_count:,}")


# ============================================================
# Orchestration
# ============================================================

DERIVED_SIGHTING_COLUMNS = [
    "standardized_shape", "primary_color", "sentiment_score",
    "dominant_emotion", "quality_score", "richness_score", "hoax_likelihood",
    "topic_id", "duration_bucket", "movement_type",
    "has_movement_mentioned", "movement_categories",
    "lat", "lng", "sighting_datetime", "has_description", "has_media",
    # v0.11 emotion columns (populated by emotions.py, not by this module's
    # steps — but listed here so reset_analysis() clears them too)
    "emotion_28_dominant", "emotion_28_group",
    "emotion_7_dominant", "vader_compound", "roberta_sentiment",
    "emotion_7_surprise", "emotion_7_fear", "emotion_7_neutral",
    "emotion_7_anger", "emotion_7_disgust", "emotion_7_sadness", "emotion_7_joy",
    # NRC lexicon word-counts (denormalized from sentiment_analysis by gerb_overlay.py)
    "nrc_joy", "nrc_fear", "nrc_anger", "nrc_sadness",
    "nrc_surprise", "nrc_disgust", "nrc_trust", "nrc_anticipation",
    "nrc_positive", "nrc_negative",
    # Nuclear proximity (computed by gerb_overlay.py)
    "distance_to_nearest_nuclear_site_km", "nearest_nuclear_site_name",
]


# ============================================================
# Pipeline step registry
# ============================================================
#
# Plug-in point for future recursive-AI enrichment steps (v0.9+).
# Adding a new step is one line: append a (name, function, label) tuple.
# Steps receive a live sqlite connection and write back in place. Order
# matters — quality/hoax read fields the earlier steps populate — but
# within each dependency cluster the order is flexible.
#
# To run only a subset (e.g. during a partial re-analysis or a test),
# pass `steps=[...]` to run_analysis.

ANALYSIS_STEPS = [
    ("shapes",        normalize_and_cluster_shapes, "Normalizing shapes"),
    ("movement",      classify_movement,            "Classifying movement/behavior"),
    ("colors",        extract_colors,               "Extracting colors"),
    ("sentiment",     derive_sentiment_summary,     "Deriving sentiment summary"),
    ("duration",      clean_duration,               "Bucketing durations"),
    ("public_fields", derive_public_fields,         "Deriving public fields"),
    ("quality",       calculate_quality_score,      "Calculating quality score"),
    ("hoax",          flag_potential_hoaxes,        "Flagging potential hoaxes"),
    ("topic",         run_topic_modeling,           "Topic modeling"),
]


def run_analysis(db_path=DB_PATH, steps=None):
    """Run the derived-analysis pipeline.

    Args:
        db_path: Path to the SQLite database to enrich.
        steps:   Ordered list of (name, fn, label) tuples. Defaults to
                 ANALYSIS_STEPS. Filter or re-order to run a partial
                 pipeline (useful for incremental rebuilds or future
                 per-record AI enrichment that sits in front of or
                 behind the default steps).
    """
    if steps is None:
        steps = ANALYSIS_STEPS

    t0 = time.time()
    conn = sqlite3.connect(db_path)
    _pragmas(conn)

    print("  Ensuring sighting_analysis rows exist...")
    created = ensure_analysis_rows(conn)
    print(f"    Created {created:,} new analysis rows")

    total = len(steps)
    for i, (name, fn, label) in enumerate(steps, 1):
        print(f"\n[{i}/{total}] {label}...")
        fn(conn)

    elapsed = time.time() - t0
    print(f"\n  Analysis complete in {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    conn.close()


def print_stats(db_path=DB_PATH):
    """Print current derived-analysis statistics."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM sighting")
    total = cur.fetchone()[0]
    if total == 0:
        print("Empty sighting table.")
        conn.close()
        return

    print(f"Total sightings: {total:,}\n")

    cur.execute("SELECT COUNT(*) FROM sighting WHERE quality_score IS NOT NULL")
    scored = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sighting WHERE quality_score >= 60")
    high = cur.fetchone()[0]
    print(f"Quality scored: {scored:,} (>=60: {high:,})")

    cur.execute("""
        SELECT standardized_shape, COUNT(*) FROM sighting
        WHERE standardized_shape IS NOT NULL
        GROUP BY standardized_shape ORDER BY 2 DESC LIMIT 10
    """)
    print("\nTop standardized shapes:")
    for shape, count in cur.fetchall():
        print(f"  {shape:15s} {count:>8,}")

    cur.execute("""
        SELECT dominant_emotion, COUNT(*) FROM sighting
        WHERE dominant_emotion IS NOT NULL
        GROUP BY dominant_emotion ORDER BY 2 DESC
    """)
    print("\nDominant emotion distribution:")
    for emo, count in cur.fetchall():
        print(f"  {emo:15s} {count:>8,}")

    cur.execute("""
        SELECT duration_bucket, COUNT(*) FROM sighting
        WHERE duration_bucket IS NOT NULL
        GROUP BY duration_bucket
    """)
    print("\nDuration buckets:")
    for b, c in cur.fetchall():
        print(f"  {b:10s} {c:>8,}")

    cur.execute("SELECT ROUND(hoax_likelihood, 1), COUNT(*) FROM sighting GROUP BY 1 ORDER BY 1")
    print("\nHoax likelihood distribution:")
    for weight, count in cur.fetchall():
        print(f"  {str(weight):6s} {count:>8,}")

    print("\nPublic-field coverage:")
    cur.execute("SELECT COUNT(*) FROM sighting WHERE lat IS NOT NULL AND lng IS NOT NULL")
    print(f"  coords                  {cur.fetchone()[0]:>8,}")
    cur.execute("SELECT COUNT(*) FROM sighting WHERE sighting_datetime IS NOT NULL")
    print(f"  datetime                {cur.fetchone()[0]:>8,}")
    cur.execute("SELECT COUNT(*) FROM sighting WHERE has_description = 1")
    print(f"  has_description         {cur.fetchone()[0]:>8,}")
    cur.execute("SELECT COUNT(*) FROM sighting WHERE has_media = 1")
    print(f"  has_media               {cur.fetchone()[0]:>8,}")
    cur.execute("SELECT COUNT(*) FROM sighting WHERE has_movement_mentioned = 1")
    print(f"  has_movement_mentioned  {cur.fetchone()[0]:>8,}")
    cur.execute("""
        SELECT COUNT(*) FROM sighting
        WHERE movement_categories IS NOT NULL
          AND movement_categories != '[]'
    """)
    print(f"  movement_categories     {cur.fetchone()[0]:>8,}")

    print("\nMovement categories distribution (flattened, multi-count):")
    cur.execute("SELECT movement_categories FROM sighting WHERE movement_categories IS NOT NULL AND movement_categories != '[]'")
    cat_counter = Counter()
    for (mc,) in cur.fetchall():
        try:
            for c in json.loads(mc):
                cat_counter[c] += 1
        except (ValueError, TypeError):
            pass
    for cat, n in cat_counter.most_common():
        print(f"  {cat:<15} {n:>8,}")

    conn.close()


def reset_analysis(db_path=DB_PATH):
    """Null out derived columns and clear sighting_analysis rows."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    set_clause = ", ".join(f"{c} = NULL" for c in DERIVED_SIGHTING_COLUMNS)
    cur.execute(f"UPDATE sighting SET {set_clause}")
    cur.execute("DELETE FROM sighting_analysis")
    conn.commit()
    conn.close()
    print("Derived columns and sighting_analysis rows cleared.")


if __name__ == "__main__":
    if "--stats-only" in sys.argv:
        print_stats()
    elif "--reset" in sys.argv:
        reset_analysis()
        run_analysis()
    else:
        run_analysis()
