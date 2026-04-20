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
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — thin wrapper around the ufosint package.
#
# The canonical implementation lives in ufosint/processors/*.
# This file re-exports the public API so that existing callers
# (rebuild_db.py, tests/test_analyze.py) continue to work.
#
# Prefer: pip install -e . && ufosint analyze
# ──────────────────────────────────────────────────────────────

import sqlite3
import os
import sys
import json
import time
from collections import Counter

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
BATCH_SIZE = 5000

# ============================================================
# Re-exported constants (used by test_analyze.py)
# ============================================================

# Import from processors so there's a single source of truth.
# Guard with try/except so analyze.py can still be imported even
# if the package isn't installed (e.g., raw checkout without pip).
try:
    from ufosint.processors.sentiment import EMOTION_KEYS
    from ufosint.processors.quality import (
        UNKNOWN_DATE_CAP,
        UNKNOWN_DATE_CAP_RICH,
        UNKNOWN_DATE_RICH_MIN_FEATURES,
        QUALITY_STRUCTURED_FIELDS,
    )
    from ufosint.processors.shapes import CANONICAL_SHAPES, SHAPE_ALIASES as SHAPE_SUBSTRING_HINTS
    from ufosint.processors.movement import (
        BEHAVIOR_KEYWORDS,
        MOVEMENT_CATEGORY_PATTERNS,
    )
    from ufosint.processors.colors import COLOR_WORDS
    from ufosint.processors.hoax import (
        GENERIC_PHRASE_PATTERNS,
        DRAMATIC_KEYWORDS,
        HOAX_WEIGHTS,
    )
    from ufosint.processors.public_fields import MEDIA_RE, TIME_HHMM_RE
    _PACKAGE_AVAILABLE = True
except ImportError:
    _PACKAGE_AVAILABLE = False
    # Fallback constants for environments without the package installed
    EMOTION_KEYS = [
        "joy", "fear", "anger", "sadness",
        "surprise", "disgust", "trust", "anticipation",
    ]
    UNKNOWN_DATE_CAP = 15
    UNKNOWN_DATE_CAP_RICH = 35
    UNKNOWN_DATE_RICH_MIN_FEATURES = 8


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
# Processor delegates — each function calls the package processor
# ============================================================

def normalize_and_cluster_shapes(conn):
    """Delegate to ufosint.processors.shapes."""
    from ufosint.processors.shapes import ShapeNormalizer
    ShapeNormalizer().process(conn)


def classify_movement(conn):
    """Delegate to ufosint.processors.movement."""
    from ufosint.processors.movement import MovementClassifier
    MovementClassifier().process(conn)


def extract_colors(conn):
    """Delegate to ufosint.processors.colors."""
    from ufosint.processors.colors import ColorExtractor
    ColorExtractor().process(conn)


def derive_sentiment_summary(conn):
    """Delegate to ufosint.processors.sentiment."""
    from ufosint.processors.sentiment import SentimentDeriver
    SentimentDeriver().process(conn)


def clean_duration(conn):
    """Delegate to ufosint.processors.duration."""
    from ufosint.processors.duration import DurationProcessor
    DurationProcessor().process(conn)


def derive_public_fields(conn):
    """Delegate to ufosint.processors.public_fields."""
    from ufosint.processors.public_fields import PublicFieldDeriver
    PublicFieldDeriver().process(conn)


def calculate_quality_score(conn):
    """Delegate to ufosint.processors.quality."""
    from ufosint.processors.quality import QualityScorer
    QualityScorer().process(conn)


def flag_potential_hoaxes(conn):
    """Delegate to ufosint.processors.hoax."""
    from ufosint.processors.hoax import HoaxFlagger
    HoaxFlagger().process(conn)


def run_topic_modeling(conn):
    """Delegate to ufosint.processors.topic."""
    from ufosint.processors.topic import TopicModeler
    TopicModeler().process(conn)


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
