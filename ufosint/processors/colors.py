"""
Color extraction processor.

Word-boundary regex scan of descriptions for ~20 color terms.
Writes primary_color to sighting and color_list JSON to sighting_analysis.
"""

import json
import re

from ufosint.processors.base import Processor, executemany_batched

# Color whitelist — compound forms first so longer matches win
COLOR_WORDS = [
    "metallic silver", "bright white", "bright red", "bright green",
    "red", "orange", "yellow", "green", "blue", "purple", "violet",
    "white", "black", "silver", "gold", "gray", "grey", "pink",
    "amber", "brown", "turquoise",
]
_COLOR_PATTERNS = [
    (c, re.compile(r"\b" + re.escape(c) + r"\b", re.IGNORECASE))
    for c in COLOR_WORDS
]


def _extract_text_colors(text):
    """Return (primary_color, color_list) — primary is first-occurring."""
    if not text:
        return None, []

    found = []  # preserves insertion order
    lowered = text.lower()
    taken_spans = []  # track matched spans to avoid 'red' matching inside 'bright red'

    def overlaps(start, end):
        return any(s < end and e > start for s, e in taken_spans)

    for color, pattern in _COLOR_PATTERNS:
        for m in pattern.finditer(lowered):
            if overlaps(m.start(), m.end()):
                continue
            taken_spans.append((m.start(), m.end()))
            if color not in found:
                found.append(color)

    if not found:
        return None, []
    return found[0], found


class ColorExtractor(Processor):
    name = "colors"
    label = "Extracting colors"

    def process(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT id, COALESCE(description, summary) FROM sighting
            WHERE description IS NOT NULL OR summary IS NOT NULL
        """)
        rows = cur.fetchall()

        sighting_updates = []
        analysis_updates = []
        matched = 0

        for sid, text in rows:
            primary, colors = _extract_text_colors(text)
            if colors:
                matched += 1
                sighting_updates.append((primary, sid))
                analysis_updates.append((json.dumps(colors), sid))

        executemany_batched(
            conn,
            "UPDATE sighting SET primary_color = ? WHERE id = ?",
            sighting_updates,
        )
        executemany_batched(
            conn,
            "UPDATE sighting_analysis SET color_list = ? WHERE sighting_id = ?",
            analysis_updates,
        )

        print(f"  Colors extracted: {matched:,} sightings with at least one color")
