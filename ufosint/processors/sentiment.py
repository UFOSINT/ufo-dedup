"""
Sentiment derivation processor.

Copies VADER compound score and NRC emotion argmax from sentiment_analysis
table to sighting columns. Does NOT re-run NLP models.
"""

from ufosint.processors.base import Processor


class SentimentDeriver(Processor):
    name = "sentiment_derive"
    label = "Deriving sentiment summary"

    def process(self, conn):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from analyze import derive_sentiment_summary
        derive_sentiment_summary(conn)
