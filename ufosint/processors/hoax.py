"""
Hoax detection processor.

Rule-based hoax likelihood (0.0-1.0) from text analysis:
very_short_text, generic_phrasing, duplicate_phrasing,
dramatic_no_specifics, all_caps_text.
"""

from ufosint.processors.base import Processor


class HoaxFlagger(Processor):
    name = "hoax"
    label = "Flagging potential hoaxes"
    depends_on = ["quality"]

    def process(self, conn):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from analyze import flag_potential_hoaxes
        flag_potential_hoaxes(conn)
