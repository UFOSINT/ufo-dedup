"""
Topic modeling processor — STUB.

Reserved for future BERTopic implementation. Currently a no-op
that keeps topic_id NULL on all rows.
"""

from ufosint.processors.base import Processor


class TopicModeler(Processor):
    name = "topic"
    label = "Topic modeling"

    def process(self, conn):
        print("  Topic modeling: [deferred — stub]")
