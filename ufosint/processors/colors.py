"""
Color extraction processor.

Word-boundary regex scan of descriptions for ~20 color terms.
Writes primary_color to sighting and color_list JSON to sighting_analysis.
"""

from ufosint.processors.base import Processor


class ColorExtractor(Processor):
    name = "colors"
    label = "Extracting colors"

    def process(self, conn):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from analyze import extract_colors
        extract_colors(conn)
