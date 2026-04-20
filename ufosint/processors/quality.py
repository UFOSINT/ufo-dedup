"""
Quality score processor.

Computes 0-100 quality score based on description length, structured fields,
coordinates, witnesses, movement, media. Must run AFTER shapes, movement,
colors, sentiment, duration, and public_fields.
"""

import json
import re

from ufosint.processors.base import Processor, executemany_batched

# Remaining structured fields (after description / witnesses / movement / media
# are split out as heavily-weighted primary features). 9 x 2 = 18 points max.
QUALITY_STRUCTURED_FIELDS = [
    "time_raw", "shape", "color", "duration",
    "sound", "direction", "elevation_angle",
    "hynek", "vallee",
]

# Unknown-date cap: if date_event is NULL, quality_score is capped.
UNKNOWN_DATE_CAP = 15
UNKNOWN_DATE_CAP_RICH = 35
UNKNOWN_DATE_RICH_MIN_FEATURES = 8

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

    # Description length (0-25)
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

    # Num witnesses tier (0-15)
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


class QualityScorer(Processor):
    name = "quality"
    label = "Calculating quality score"
    depends_on = ["shapes", "movement", "colors", "sentiment_derive",
                  "duration", "public_fields"]

    def process(self, conn):
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

        executemany_batched(
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
