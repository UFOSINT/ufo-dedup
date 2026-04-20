"""
Sentiment derivation processor.

Copies VADER compound score and NRC emotion argmax from sentiment_analysis
table to sighting columns. Does NOT re-run NLP models.
"""

import json

from ufosint.processors.base import Processor, executemany_batched

EMOTION_KEYS = [
    "joy", "fear", "anger", "sadness",
    "surprise", "disgust", "trust", "anticipation",
]


class SentimentDeriver(Processor):
    name = "sentiment_derive"
    label = "Deriving sentiment summary"

    def process(self, conn):
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

        executemany_batched(
            conn,
            "UPDATE sighting SET sentiment_score = ?, dominant_emotion = ? WHERE id = ?",
            sighting_updates,
        )
        executemany_batched(
            conn,
            "UPDATE sighting_analysis SET emotion_scores = ? WHERE sighting_id = ?",
            analysis_updates,
        )

        print(f"  Sentiment derived: {len(sighting_updates):,} sightings")
