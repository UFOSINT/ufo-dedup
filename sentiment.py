"""
Batch sentiment/emotion analysis for UFO sighting descriptions.

Uses VADER for sentiment polarity and NRCLex for 8-emotion classification.
Pre-computes scores and stores in the sentiment_analysis table so the
web app can query results without NLP dependencies.

Usage:
    python sentiment.py                # Analyze all unprocessed sightings
    python sentiment.py --stats-only   # Print current analysis stats
    python sentiment.py --reset        # Delete all and re-analyze
"""
import sqlite3
import os
import sys
import time

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
BATCH_SIZE = 5000
MIN_TEXT_LENGTH = 10

EMOTION_KEYS = [
    "joy", "fear", "anger", "sadness",
    "surprise", "disgust", "trust", "anticipation",
]


def run_sentiment(db_path=DB_PATH):
    """Analyze all sightings with text and store sentiment/emotion scores."""
    # Import NLP libs at call time so the module can be imported without them
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    from nrclex import NRCLex

    analyzer = SentimentIntensityAnalyzer()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # Get all sightings not yet analyzed, with text >= MIN_TEXT_LENGTH
    cur.execute(f"""
        SELECT s.id, s.description, s.summary
        FROM sighting s
        LEFT JOIN sentiment_analysis sa ON s.id = sa.sighting_id
        WHERE sa.id IS NULL
          AND LENGTH(COALESCE(s.description, s.summary, '')) >= {MIN_TEXT_LENGTH}
        ORDER BY s.id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"\nSightings to analyze: {total:,}")

    if total == 0:
        print("Nothing to process.")
        conn.close()
        return

    analyzed = 0
    batch = []
    t0 = time.time()

    for i, (sid, description, summary) in enumerate(rows):
        # Pick best text source
        if description and len(description) >= MIN_TEXT_LENGTH:
            text = description
            text_source = "description"
        elif summary and len(summary) >= MIN_TEXT_LENGTH:
            text = summary
            text_source = "summary"
        else:
            continue

        # VADER sentiment
        vs = analyzer.polarity_scores(text)

        # NRC emotion counts
        try:
            nrc = NRCLex(text)
            emo = nrc.raw_emotion_scores
        except Exception:
            emo = {}

        batch.append((
            sid,
            vs["compound"], vs["pos"], vs["neg"], vs["neu"],
            emo.get("joy", 0),
            emo.get("fear", 0),
            emo.get("anger", 0),
            emo.get("sadness", 0),
            emo.get("surprise", 0),
            emo.get("disgust", 0),
            emo.get("trust", 0),
            emo.get("anticipation", 0),
            text_source,
            len(text),
        ))
        analyzed += 1

        if len(batch) >= BATCH_SIZE:
            cur.executemany("""
                INSERT OR IGNORE INTO sentiment_analysis
                (sighting_id, vader_compound, vader_positive, vader_negative, vader_neutral,
                 emo_joy, emo_fear, emo_anger, emo_sadness, emo_surprise, emo_disgust,
                 emo_trust, emo_anticipation, text_source, text_length)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            batch = []
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(f"  ... {i + 1:,}/{total:,} processed, "
                  f"{analyzed:,} analyzed ({rate:.0f}/s, ~{eta / 60:.0f}m remaining)", end="\r")

    # Final batch
    if batch:
        cur.executemany("""
            INSERT OR IGNORE INTO sentiment_analysis
            (sighting_id, vader_compound, vader_positive, vader_negative, vader_neutral,
             emo_joy, emo_fear, emo_anger, emo_sadness, emo_surprise, emo_disgust,
             emo_trust, emo_anticipation, text_source, text_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()

    elapsed = time.time() - t0

    # Summary stats
    cur.execute("SELECT AVG(vader_compound) FROM sentiment_analysis")
    avg_compound = cur.fetchone()[0] or 0

    cur.execute("""
        SELECT text_source, COUNT(*) FROM sentiment_analysis
        GROUP BY text_source
    """)
    by_source = {row[0]: row[1] for row in cur.fetchall()}

    print(f"\n\nSentiment analysis complete:")
    print(f"  Records analyzed: {analyzed:,}")
    print(f"  Avg VADER compound: {avg_compound:.3f}")
    print(f"  By text source: {', '.join(f'{k}={v:,}' for k, v in by_source.items())}")
    print(f"  Elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} min)")

    # Per-source breakdown
    print(f"\n  By database source:")
    cur.execute("""
        SELECT sd.name, COUNT(*), AVG(sa.vader_compound)
        FROM sentiment_analysis sa
        JOIN sighting s ON sa.sighting_id = s.id
        JOIN source_database sd ON s.source_db_id = sd.id
        GROUP BY sd.name
        ORDER BY COUNT(*) DESC
    """)
    for name, count, avg in cur.fetchall():
        print(f"    {name:12s}  {count:>8,}  avg_compound={avg:.3f}")

    conn.close()


def print_stats(db_path=DB_PATH):
    """Print current sentiment analysis statistics."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM sentiment_analysis")
    total = cur.fetchone()[0]

    if total == 0:
        print("No sentiment data. Run: python sentiment.py")
        conn.close()
        return

    cur.execute("SELECT AVG(vader_compound), MIN(vader_compound), MAX(vader_compound) FROM sentiment_analysis")
    avg_c, min_c, max_c = cur.fetchone()

    print(f"Sentiment records: {total:,}")
    print(f"VADER compound: avg={avg_c:.3f}, min={min_c:.3f}, max={max_c:.3f}")

    print(f"\nBy source:")
    cur.execute("""
        SELECT sd.name, COUNT(*), AVG(sa.vader_compound)
        FROM sentiment_analysis sa
        JOIN sighting s ON sa.sighting_id = s.id
        JOIN source_database sd ON s.source_db_id = sd.id
        GROUP BY sd.name
        ORDER BY COUNT(*) DESC
    """)
    for name, count, avg in cur.fetchall():
        print(f"  {name:12s}  {count:>8,}  avg={avg:.3f}")

    print(f"\nTop emotions (total word counts):")
    cur.execute("""
        SELECT SUM(emo_joy) as joy, SUM(emo_fear) as fear,
               SUM(emo_anger) as anger, SUM(emo_sadness) as sadness,
               SUM(emo_surprise) as surprise, SUM(emo_disgust) as disgust,
               SUM(emo_trust) as trust, SUM(emo_anticipation) as anticipation
        FROM sentiment_analysis
    """)
    row = cur.fetchone()
    emotions = list(zip(EMOTION_KEYS, row))
    emotions.sort(key=lambda x: x[1], reverse=True)
    for emo, count in emotions:
        print(f"  {emo:15s} {count:>10,}")

    conn.close()


def reset_sentiment(db_path=DB_PATH):
    """Delete all sentiment data for re-analysis."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM sentiment_analysis")
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    print(f"Deleted {deleted:,} sentiment records.")


if __name__ == "__main__":
    if "--stats-only" in sys.argv:
        print_stats()
    elif "--reset" in sys.argv:
        reset_sentiment()
        run_sentiment()
    else:
        run_sentiment()
