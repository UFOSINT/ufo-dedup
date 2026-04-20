"""
Hoax detection processor.

Rule-based hoax likelihood (0.0-1.0) from text analysis:
very_short_text, generic_phrasing, duplicate_phrasing,
dramatic_no_specifics, all_caps_text.
"""

import json
import re
from collections import Counter

from ufosint.processors.base import Processor, executemany_batched

# Canned phrases that show up copy-pasted across bad reports
GENERIC_PHRASE_PATTERNS = [
    re.compile(r"^saw a ufo\.?$", re.IGNORECASE),
    re.compile(r"^ufo sighting\.?$", re.IGNORECASE),
    re.compile(r"^strange lights?\.?$", re.IGNORECASE),
    re.compile(r"^unknown object\.?$", re.IGNORECASE),
    re.compile(r"^i saw something\.?$", re.IGNORECASE),
]

DRAMATIC_KEYWORDS = re.compile(
    r"\b(alien|abduct(?:ed|ion)|probed|reptilian|grey|illuminati)\b",
    re.IGNORECASE,
)

HOAX_WEIGHTS = {
    "very_short_text": 0.2,
    "generic_phrasing": 0.3,
    "duplicate_phrasing": 0.4,
    "dramatic_no_specifics": 0.3,
    "all_caps_text": 0.15,
}


def _is_all_caps(text, min_len=20):
    if not text or len(text) < min_len:
        return False
    letters = [c for c in text if c.isalpha()]
    if len(letters) < min_len:
        return False
    upper = sum(1 for c in letters if c.isupper())
    return upper / len(letters) > 0.8


class HoaxFlagger(Processor):
    name = "hoax"
    label = "Flagging potential hoaxes"
    depends_on = ["quality"]

    def process(self, conn):
        cur = conn.cursor()

        # Pre-compute duplicate phrasing: any 120-char description prefix seen >= 10 times
        print("  Scanning for duplicate phrasing...")
        cur.execute("""
            SELECT SUBSTR(description, 1, 120), COUNT(*) as c
            FROM sighting
            WHERE description IS NOT NULL AND LENGTH(description) >= 40
            GROUP BY SUBSTR(description, 1, 120)
            HAVING c >= 10
        """)
        dup_prefixes = {row[0] for row in cur.fetchall()}
        print(f"    Found {len(dup_prefixes):,} duplicate-prefix patterns")

        # Pull the features each rule needs
        cur.execute("""
            SELECT s.id, s.description, s.summary,
                   COALESCE(s.richness_score, 0)
            FROM sighting s
        """)
        rows = cur.fetchall()

        sighting_updates = []
        analysis_updates = []
        flag_counter = Counter()

        for sid, description, summary, features_count in rows:
            text = description or summary or ""
            flags = []

            if text and len(text.strip()) < 20:
                flags.append("very_short_text")

            if text and any(p.match(text.strip()) for p in GENERIC_PHRASE_PATTERNS):
                flags.append("generic_phrasing")

            if description:
                prefix = description[:120]
                if prefix in dup_prefixes:
                    flags.append("duplicate_phrasing")

            if text and DRAMATIC_KEYWORDS.search(text) and features_count < 3:
                flags.append("dramatic_no_specifics")

            if _is_all_caps(text):
                flags.append("all_caps_text")

            if flags:
                weight = min(1.0, sum(HOAX_WEIGHTS[f] for f in flags))
                flag_counter.update(flags)
                sighting_updates.append((weight, sid))
                analysis_updates.append((json.dumps(flags), sid))
            else:
                sighting_updates.append((0.0, sid))
                analysis_updates.append((json.dumps([]), sid))

        executemany_batched(
            conn,
            "UPDATE sighting SET hoax_likelihood = ? WHERE id = ?",
            sighting_updates,
        )
        executemany_batched(
            conn,
            "UPDATE sighting_analysis SET hoax_flags = ? WHERE sighting_id = ?",
            analysis_updates,
        )

        breakdown = ", ".join(f"{k}={v:,}" for k, v in flag_counter.most_common())
        print(f"  Hoax flagged: {sum(flag_counter.values()):,} total flag hits "
              f"({breakdown or 'none'})")
