"""
Transformer-based emotion classification for UFO sighting descriptions.

Runs three models + VADER on every sighting that has description/summary text,
then writes pre-computed labels and scores to the sighting table. The public
export (export_public.py) ships these columns to the frontend binary buffer;
no raw text is ever exposed.

Models:
  1. GoEmotions 28-class  (SamLowe/roberta-base-go_emotions)
     -> emotion_28_dominant, emotion_28_group
  2. 7-class RoBERTa     (j-hartmann/emotion-english-distilroberta-base)
     -> emotion_7_dominant, emotion_7_* probability columns
  3. RoBERTa sentiment   (cardiffnlp/twitter-roberta-base-sentiment-latest)
     -> roberta_sentiment (-1.0 to +1.0 compound)
  4. VADER               (vaderSentiment, CPU-only)
     -> vader_compound (-1.0 to +1.0)

GPU: uses CUDA if available, falls back to CPU. float16 on GPU for speed.
Batched: configurable BATCH_SIZE (default 64 for 8GB VRAM).
Idempotent: skips rows that already have emotion_28_dominant populated
(unless --reset is passed).

Usage:
    python emotions.py                    # Run on all unprocessed sightings
    python emotions.py --stats-only       # Print current coverage
    python emotions.py --reset            # Null emotion columns and re-run
    python emotions.py --db PATH          # Custom DB path

Designed to plug into analyze.py's ANALYSIS_STEPS registry as a late-stage
step (after text-dependent steps like classify_movement, but order-independent
from quality_score).
"""
import json
import os
import sqlite3
import sys
import time

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "output", "ufo_unified.db")
BATCH_SIZE = 64          # GPU batch size (64 fits comfortably in 8GB VRAM)
MAX_TEXT_LEN = 512       # Truncate text to this many chars before tokenization
MIN_TEXT_LEN = 10        # Skip very short texts

# GoEmotions 28-class -> sentiment group mapping
# Standard grouping from the GoEmotions paper (Demszky et al. 2020)
GOEMOTION_GROUPS = {
    "admiration": "positive",
    "amusement": "positive",
    "approval": "positive",
    "caring": "positive",
    "desire": "positive",
    "excitement": "positive",
    "gratitude": "positive",
    "joy": "positive",
    "love": "positive",
    "optimism": "positive",
    "pride": "positive",
    "relief": "positive",
    "anger": "negative",
    "annoyance": "negative",
    "disappointment": "negative",
    "disapproval": "negative",
    "disgust": "negative",
    "embarrassment": "negative",
    "fear": "negative",
    "grief": "negative",
    "nervousness": "negative",
    "remorse": "negative",
    "sadness": "negative",
    "confusion": "ambiguous",
    "curiosity": "ambiguous",
    "realization": "ambiguous",
    "surprise": "ambiguous",
    "neutral": "neutral",
}

# 7-class emotion labels in the order the app expects for the probability vector
EMOTION_7_LABELS = ["surprise", "fear", "neutral", "anger", "disgust", "sadness", "joy"]

# Emotion columns on sighting (for reset/stats)
EMOTION_COLUMNS = [
    "emotion_28_dominant", "emotion_28_group",
    "emotion_7_dominant", "vader_compound", "roberta_sentiment",
    "emotion_7_surprise", "emotion_7_fear", "emotion_7_neutral",
    "emotion_7_anger", "emotion_7_disgust", "emotion_7_sadness", "emotion_7_joy",
]


# ============================================================
# Model loading
# ============================================================

def _get_device():
    """Return the best available torch device."""
    import torch
    if torch.cuda.is_available():
        return 0  # GPU index for transformers pipeline
    return -1     # CPU


def _load_models(device):
    """Load all three transformer pipelines. Returns (go28, emo7, roberta_sent)."""
    import torch
    from transformers import pipeline as hf_pipeline

    dtype = torch.float16 if device >= 0 else torch.float32

    print("  Loading GoEmotions 28-class model...")
    go28 = hf_pipeline(
        "text-classification",
        model="SamLowe/roberta-base-go_emotions",
        top_k=1,
        device=device,
        torch_dtype=dtype,
        truncation=True,
        max_length=512,
    )

    print("  Loading 7-class RoBERTa emotion model...")
    emo7 = hf_pipeline(
        "text-classification",
        model="j-hartmann/emotion-english-distilroberta-base",
        top_k=None,  # return all 7 probabilities
        device=device,
        torch_dtype=dtype,
        truncation=True,
        max_length=512,
    )

    print("  Loading RoBERTa sentiment model...")
    roberta_sent = hf_pipeline(
        "text-classification",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
        top_k=None,  # returns positive/negative/neutral scores
        device=device,
        torch_dtype=dtype,
        truncation=True,
        max_length=512,
    )

    return go28, emo7, roberta_sent


def _load_vader():
    """Load the VADER sentiment analyzer (CPU-only, fast)."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    return SentimentIntensityAnalyzer()


# ============================================================
# Batch inference
# ============================================================

def _roberta_compound(scores_list):
    """Convert RoBERTa sentiment output [pos, neg, neu] to a -1 to +1 compound.

    Formula: compound = positive_score - negative_score
    Yields -1.0 (pure negative) to +1.0 (pure positive), similar semantics
    to VADER compound but from a transformer model.
    """
    scores = {s["label"]: s["score"] for s in scores_list}
    pos = scores.get("positive", scores.get("Positive", 0.0))
    neg = scores.get("negative", scores.get("Negative", 0.0))
    return round(pos - neg, 4)


def _process_batch(texts, go28_pipe, emo7_pipe, roberta_pipe, vader):
    """Run all 4 classifiers on a batch of texts. Returns list of result dicts."""
    results = []

    # Transformer batches — each pipeline handles its own tokenization/batching
    go28_out = go28_pipe(texts, batch_size=len(texts))
    emo7_out = emo7_pipe(texts, batch_size=len(texts))
    roberta_out = roberta_pipe(texts, batch_size=len(texts))

    for i, text in enumerate(texts):
        # GoEmotions 28-class: top-1 label
        go_label = go28_out[i][0]["label"]
        go_group = GOEMOTION_GROUPS.get(go_label, "ambiguous")

        # 7-class emotion: all probabilities + dominant
        emo_scores = {s["label"]: round(s["score"], 4) for s in emo7_out[i]}
        emo_dominant = max(emo_scores, key=emo_scores.get)

        # RoBERTa sentiment: compound score
        rob_compound = _roberta_compound(roberta_out[i])

        # VADER: compound score (CPU, fast)
        vader_scores = vader.polarity_scores(text)
        vader_c = round(vader_scores["compound"], 4)

        results.append({
            "emotion_28_dominant": go_label,
            "emotion_28_group": go_group,
            "emotion_7_dominant": emo_dominant,
            "vader_compound": vader_c,
            "roberta_sentiment": rob_compound,
            "emotion_7_surprise": emo_scores.get("surprise", 0.0),
            "emotion_7_fear": emo_scores.get("fear", 0.0),
            "emotion_7_neutral": emo_scores.get("neutral", 0.0),
            "emotion_7_anger": emo_scores.get("anger", 0.0),
            "emotion_7_disgust": emo_scores.get("disgust", 0.0),
            "emotion_7_sadness": emo_scores.get("sadness", 0.0),
            "emotion_7_joy": emo_scores.get("joy", 0.0),
        })

    return results


# ============================================================
# Main entry point
# ============================================================

def run_emotions(db_path=DB_PATH, batch_size=BATCH_SIZE):
    """Classify all sightings with text. GPU-accelerated, batched."""
    t0 = time.time()

    device = _get_device()
    device_name = "CPU"
    if device >= 0:
        import torch
        device_name = torch.cuda.get_device_name(device)
    print(f"  Device: {device_name}")
    print(f"  Batch size: {batch_size}")

    go28, emo7, roberta_sent = _load_models(device)
    vader = _load_vader()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # Get all sightings that have text and haven't been classified yet
    cur.execute(f"""
        SELECT id, COALESCE(description, summary) AS text
        FROM sighting
        WHERE (description IS NOT NULL OR summary IS NOT NULL)
          AND LENGTH(COALESCE(description, summary, '')) >= {MIN_TEXT_LEN}
          AND emotion_28_dominant IS NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"\n  Sightings to classify: {total:,}")

    if total == 0:
        print("  Nothing to process.")
        conn.close()
        return

    processed = 0
    commit_interval = 1000  # commit every N rows

    for batch_start in range(0, total, batch_size):
        batch_rows = rows[batch_start:batch_start + batch_size]
        sids = [r[0] for r in batch_rows]
        texts = [r[1][:MAX_TEXT_LEN] for r in batch_rows]

        results = _process_batch(texts, go28, emo7, roberta_sent, vader)

        update_data = []
        for sid, res in zip(sids, results):
            update_data.append((
                res["emotion_28_dominant"],
                res["emotion_28_group"],
                res["emotion_7_dominant"],
                res["vader_compound"],
                res["roberta_sentiment"],
                res["emotion_7_surprise"],
                res["emotion_7_fear"],
                res["emotion_7_neutral"],
                res["emotion_7_anger"],
                res["emotion_7_disgust"],
                res["emotion_7_sadness"],
                res["emotion_7_joy"],
                sid,
            ))

        cur.executemany("""
            UPDATE sighting SET
                emotion_28_dominant = ?,
                emotion_28_group = ?,
                emotion_7_dominant = ?,
                vader_compound = ?,
                roberta_sentiment = ?,
                emotion_7_surprise = ?,
                emotion_7_fear = ?,
                emotion_7_neutral = ?,
                emotion_7_anger = ?,
                emotion_7_disgust = ?,
                emotion_7_sadness = ?,
                emotion_7_joy = ?
            WHERE id = ?
        """, update_data)

        processed += len(batch_rows)

        # Commit every batch — batches are small (64 rows) so this is safe
        # and prevents the "95k committed, 400k lost" issue if the process
        # gets killed mid-run.
        conn.commit()

        elapsed = time.time() - t0
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0
        # Flush progress to stdout so backgrounded processes don't buffer
        sys.stdout.write(
            f"\r  {processed:,}/{total:,} "
            f"({100*processed/total:.1f}%, {rate:.0f}/s, "
            f"~{eta/60:.0f}m remaining)"
        )
        sys.stdout.flush()

    conn.commit()
    elapsed = time.time() - t0

    # Summary stats
    print(f"\n\n  Emotion classification complete:")
    print(f"    Records classified: {processed:,}")
    print(f"    Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"    Rate: {processed/elapsed:.0f} rows/s")

    _print_coverage(cur)

    conn.close()


def _print_coverage(cur):
    """Print coverage stats for emotion columns."""
    cur.execute("SELECT COUNT(*) FROM sighting")
    total = cur.fetchone()[0]

    print(f"\n  Coverage ({total:,} total sightings):")
    for col in EMOTION_COLUMNS[:5]:
        cur.execute(f"SELECT COUNT(*) FROM sighting WHERE {col} IS NOT NULL")
        n = cur.fetchone()[0]
        print(f"    {col:<25} {n:>10,}  ({100*n/total:.1f}%)")

    # GoEmotions distribution
    cur.execute("""
        SELECT emotion_28_dominant, COUNT(*)
        FROM sighting WHERE emotion_28_dominant IS NOT NULL
        GROUP BY emotion_28_dominant ORDER BY 2 DESC LIMIT 10
    """)
    print(f"\n  Top 10 GoEmotions 28-class labels:")
    for label, count in cur.fetchall():
        group = GOEMOTION_GROUPS.get(label, "?")
        print(f"    {label:<18} ({group:<9})  {count:>9,}")

    # emotion_28_group distribution
    cur.execute("""
        SELECT emotion_28_group, COUNT(*)
        FROM sighting WHERE emotion_28_group IS NOT NULL
        GROUP BY emotion_28_group ORDER BY 2 DESC
    """)
    print(f"\n  Sentiment group distribution:")
    for group, count in cur.fetchall():
        print(f"    {group:<12} {count:>10,}")

    # 7-class distribution
    cur.execute("""
        SELECT emotion_7_dominant, COUNT(*)
        FROM sighting WHERE emotion_7_dominant IS NOT NULL
        GROUP BY emotion_7_dominant ORDER BY 2 DESC
    """)
    print(f"\n  7-class emotion distribution:")
    for label, count in cur.fetchall():
        print(f"    {label:<12} {count:>10,}")

    # VADER vs RoBERTa average
    cur.execute("""
        SELECT AVG(vader_compound), AVG(roberta_sentiment)
        FROM sighting WHERE vader_compound IS NOT NULL
    """)
    v_avg, r_avg = cur.fetchone()
    print(f"\n  Average VADER compound:      {v_avg:.4f}")
    print(f"  Average RoBERTa sentiment:   {r_avg:.4f}")


def print_stats(db_path=DB_PATH):
    """Print current emotion classification statistics."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    _print_coverage(cur)
    conn.close()


def reset_emotions(db_path=DB_PATH):
    """Null out all emotion columns for re-classification."""
    conn = sqlite3.connect(db_path)
    set_clause = ", ".join(f"{c} = NULL" for c in EMOTION_COLUMNS)
    conn.execute(f"UPDATE sighting SET {set_clause}")
    conn.commit()
    count = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    print(f"  Nulled emotion columns on {count:,} rows.")


# ============================================================
# CLI
# ============================================================
# CACHE / REPLAY — avoid re-running GPU inference on rebuilds
# ============================================================

EMOTION_CACHE_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "output", "emotion_classification_cache.csv"
)

def export_emotion_cache(db_path=DB_PATH, csv_path=EMOTION_CACHE_CSV):
    """Export emotion columns to CSV for replay on future rebuilds."""
    import csv as csv_mod
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, emotion_28_dominant, emotion_28_group, emotion_7_dominant,
               vader_compound, roberta_sentiment,
               emotion_7_surprise, emotion_7_fear, emotion_7_neutral,
               emotion_7_anger, emotion_7_disgust, emotion_7_sadness, emotion_7_joy
        FROM sighting WHERE emotion_28_dominant IS NOT NULL
    """)
    rows = cur.fetchall()
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv_mod.writer(f)
        w.writerow([
            "sighting_id", "emotion_28_dominant", "emotion_28_group", "emotion_7_dominant",
            "vader_compound", "roberta_sentiment",
            "emotion_7_surprise", "emotion_7_fear", "emotion_7_neutral",
            "emotion_7_anger", "emotion_7_disgust", "emotion_7_sadness", "emotion_7_joy",
        ])
        w.writerows(rows)
    size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"  Exported {len(rows):,} emotion records to {csv_path} ({size_mb:.1f} MB)")
    conn.close()
    return len(rows)


def replay_emotion_cache(db_path=DB_PATH, csv_path=EMOTION_CACHE_CSV):
    """
    Re-apply cached emotion classifications without running GPU inference.

    Matches by sighting_id. Only fills rows where emotion_28_dominant IS NULL.
    On a fresh rebuild, sighting IDs are stable (same import order = same IDs),
    so the cache maps directly.
    """
    import csv as csv_mod
    if not os.path.exists(csv_path):
        print(f"  No emotion cache found at {csv_path}")
        print(f"  Run emotions.py with GPU first, then: python emotions.py --export-cache")
        return 0

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # Check if already populated
    cur.execute("SELECT COUNT(*) FROM sighting WHERE emotion_28_dominant IS NOT NULL")
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"  {existing:,} rows already have emotion data — skipping replay.")
        conn.close()
        return 0

    print(f"\n=== Replaying cached emotion classifications ===\n")

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        batch = []
        applied = 0
        for row in reader:
            try:
                batch.append((
                    row["emotion_28_dominant"] or None,
                    row["emotion_28_group"] or None,
                    row["emotion_7_dominant"] or None,
                    float(row["vader_compound"]) if row["vader_compound"] else None,
                    float(row["roberta_sentiment"]) if row["roberta_sentiment"] else None,
                    float(row["emotion_7_surprise"]) if row["emotion_7_surprise"] else None,
                    float(row["emotion_7_fear"]) if row["emotion_7_fear"] else None,
                    float(row["emotion_7_neutral"]) if row["emotion_7_neutral"] else None,
                    float(row["emotion_7_anger"]) if row["emotion_7_anger"] else None,
                    float(row["emotion_7_disgust"]) if row["emotion_7_disgust"] else None,
                    float(row["emotion_7_sadness"]) if row["emotion_7_sadness"] else None,
                    float(row["emotion_7_joy"]) if row["emotion_7_joy"] else None,
                    int(row["sighting_id"]),
                ))
            except (ValueError, KeyError):
                continue

            if len(batch) >= 5000:
                cur.executemany("""
                    UPDATE sighting SET
                        emotion_28_dominant = ?, emotion_28_group = ?,
                        emotion_7_dominant = ?, vader_compound = ?,
                        roberta_sentiment = ?,
                        emotion_7_surprise = ?, emotion_7_fear = ?,
                        emotion_7_neutral = ?, emotion_7_anger = ?,
                        emotion_7_disgust = ?, emotion_7_sadness = ?,
                        emotion_7_joy = ?
                    WHERE id = ? AND emotion_28_dominant IS NULL
                """, batch)
                applied += len(batch)
                conn.commit()
                print(f"\r  Applied: {applied:,}", end="")
                batch = []

        if batch:
            cur.executemany("""
                UPDATE sighting SET
                    emotion_28_dominant = ?, emotion_28_group = ?,
                    emotion_7_dominant = ?, vader_compound = ?,
                    roberta_sentiment = ?,
                    emotion_7_surprise = ?, emotion_7_fear = ?,
                    emotion_7_neutral = ?, emotion_7_anger = ?,
                    emotion_7_disgust = ?, emotion_7_sadness = ?,
                    emotion_7_joy = ?
                WHERE id = ? AND emotion_28_dominant IS NULL
            """, batch)
            applied += len(batch)
            conn.commit()

    print(f"\n  Replay complete: {applied:,} emotion records applied from cache")
    conn.close()
    return applied


# ============================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run emotion classification on UFO sightings")
    parser.add_argument("--db", default=DB_PATH, help="SQLite database path")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="GPU batch size")
    parser.add_argument("--stats-only", action="store_true", help="Print coverage stats only")
    parser.add_argument("--reset", action="store_true", help="Null emotion columns and re-run")
    parser.add_argument("--export-cache", action="store_true", help="Export emotion data to CSV cache")
    parser.add_argument("--replay", action="store_true", help="Replay cached emotions (no GPU needed)")
    args = parser.parse_args()

    if args.stats_only:
        print_stats(args.db)
    elif args.export_cache:
        export_emotion_cache(args.db)
    elif args.replay:
        replay_emotion_cache(args.db)
    elif args.reset:
        reset_emotions(args.db)
        run_emotions(args.db, args.batch_size)
    else:
        run_emotions(args.db, args.batch_size)
