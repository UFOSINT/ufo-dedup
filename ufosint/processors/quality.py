"""
Quality score processor.

Computes 0-100 quality score based on description length, structured fields,
coordinates, witnesses, movement, media. Must run AFTER shapes, movement,
colors, sentiment, duration, and public_fields.
"""

from ufosint.processors.base import Processor


class QualityScorer(Processor):
    name = "quality"
    label = "Calculating quality score"
    depends_on = ["shapes", "movement", "colors", "sentiment_derive",
                  "duration", "public_fields"]

    def process(self, conn):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from analyze import calculate_quality_score
        calculate_quality_score(conn)
