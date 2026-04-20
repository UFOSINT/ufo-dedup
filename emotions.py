"""
Transformer-based emotion classification for UFO sighting descriptions.

Usage:
    python emotions.py                    # Run on all unprocessed sightings
    python emotions.py --stats-only       # Print current coverage
    python emotions.py --replay           # Replay cached emotions (no GPU)
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint emotions
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import sys

from ufosint.processors.emotions import (
    run_emotions,
    print_stats,
    reset_emotions,
    export_emotion_cache,
    replay_emotion_cache,
    EMOTION_CACHE_CSV,
    DB_PATH,
    BATCH_SIZE,
)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run emotion classification on UFO sightings")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--stats-only", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--export-cache", action="store_true")
    parser.add_argument("--replay", action="store_true")
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
