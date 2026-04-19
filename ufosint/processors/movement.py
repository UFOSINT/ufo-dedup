"""
Movement and behavior classification processor.

Regex-based extraction of 10 movement categories and 14 behavior tags
from narrative descriptions.
"""

from ufosint.processors.base import Processor


class MovementClassifier(Processor):
    name = "movement"
    label = "Classifying movement/behavior"

    def process(self, conn):
        # Delegate to existing analyze.py during transition
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from analyze import classify_movement
        classify_movement(conn)
