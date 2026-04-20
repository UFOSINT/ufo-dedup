"""
Movement and behavior classification processor.

Regex-based extraction of 10 movement categories and 14 behavior tags
from narrative descriptions.
"""

import json
import re
from collections import Counter

from ufosint.processors.base import Processor, executemany_batched

# Behavior keyword -> tag name
BEHAVIOR_KEYWORDS = {
    "hovering": [r"\bhover(?:ed|ing|s)?\b", r"\bstationary\b", r"\bsuspended\b"],
    "silent": [r"\bsilent\b", r"\bno sound\b", r"\bmade no noise\b", r"\bsoundless\b"],
    "bright": [r"\bbright\b", r"\bbrilliant\b", r"\bglowing\b", r"\billuminated\b"],
    "pulsing": [r"\bpuls(?:ed|ing|ates?)\b", r"\bflash(?:ed|ing)\b", r"\bblink(?:ed|ing)?\b"],
    "rotating": [r"\brotat(?:ed|ing|es)\b", r"\bspin(?:ning)?\b", r"\brevolv(?:ed|ing)\b"],
    "zigzag": [r"\bzig.?zag\b", r"\berratic\b", r"\bzipped\b"],
    "vanished": [r"\bvanish(?:ed|es|ing)?\b", r"\bdisappear(?:ed)?\b", r"\bgone in\b"],
    "accelerated": [r"\baccelerat(?:ed|ing|ion)\b", r"\bhigh speed\b", r"\bshot (?:off|up|away)\b"],
    "split": [r"\bsplit\b", r"\bdivided\b", r"\bsepar(?:ated|ating)\b"],
    "merged": [r"\bmerged\b", r"\bjoined\b", r"\bcombined\b"],
    "formation": [r"\bformation\b", r"\bin a line\b", r"\bv-shape\b"],
    "chased": [r"\bchased\b", r"\bpursued\b"],
    "followed": [r"\bfollowed\b", r"\btrail(?:ed|ing)\b"],
    "landed": [r"\blanded\b", r"\btouched down\b", r"\bon the ground\b"],
}

# Compiled once for speed
_BEHAVIOR_PATTERNS = {
    tag: [re.compile(p, re.IGNORECASE) for p in pats]
    for tag, pats in BEHAVIOR_KEYWORDS.items()
}

# Movement-only category taxonomy (v0.8.3, per science team brief).
MOVEMENT_CATEGORY_PATTERNS = {
    "hovering":     [r"\bhover(?:ed|ing|s)?\b", r"\bstationary\b", r"\bsuspended\b", r"\bmotionless\b"],
    "linear":       [r"\bstraight line\b", r"\bstraight path\b", r"\bin a line\b",
                     r"\bheaded (?:north|south|east|west)\b"],
    "erratic":      [r"\bzig.?zag\b", r"\berratic\b", r"\bdarted\b", r"\bjerky\b"],
    "accelerating": [r"\baccelerat\w*\b", r"\bshot (?:off|up|away|out)\b",
                     r"\bhigh speed\b", r"\bsped (?:off|away)\b", r"\bzipped\b"],
    "rotating":     [r"\brotat\w*\b", r"\bspin(?:ning|ned)?\b", r"\brevolv\w*\b", r"\bwobbl\w*\b"],
    "ascending":    [r"\bascend\w*\b", r"\bclimb(?:ed|ing)?\b", r"\bshot up\b",
                     r"\bstraight up\b", r"\bupward\b"],
    "descending":   [r"\bdescend\w*\b", r"\bdropp\w*\b", r"\bfell\b", r"\bdownward\b",
                     r"\bplummet\w*\b"],
    "vanished":     [r"\bvanish\w*\b", r"\bdisappear\w*\b", r"\bgone in\b", r"\bfaded\b"],
    "followed":     [r"\bfollow(?:ed|ing)?\b", r"\btrail(?:ed|ing)?\b",
                     r"\bchased\b", r"\bpursued\b"],
    "landed":       [r"\bland(?:ed|ing)?\b", r"\btouched down\b", r"\bon the ground\b"],
}

_MOVEMENT_CATEGORY_RE = {
    cat: [re.compile(p, re.IGNORECASE) for p in pats]
    for cat, pats in MOVEMENT_CATEGORY_PATTERNS.items()
}


def _classify_text_behavior(text):
    """Return (behavior_tags_list, movement_type)."""
    if not text:
        return [], None

    tags = []
    for tag, patterns in _BEHAVIOR_PATTERNS.items():
        if any(p.search(text) for p in patterns):
            tags.append(tag)

    # Derive a single movement_type from the tags
    if "hovering" in tags:
        movement = "hover"
    elif "accelerated" in tags:
        movement = "fast"
    elif "zigzag" in tags:
        movement = "erratic"
    elif "vanished" in tags or "followed" in tags or "chased" in tags:
        movement = "linear"
    elif "landed" in tags:
        movement = "stationary"
    elif tags:
        movement = "linear"
    else:
        movement = None

    return tags, movement


def _classify_text_movement_categories(text):
    """Return list of movement category names that fired for `text`."""
    if not text:
        return []
    fired = []
    for cat, patterns in _MOVEMENT_CATEGORY_RE.items():
        if any(p.search(text) for p in patterns):
            fired.append(cat)
    return fired


class MovementClassifier(Processor):
    name = "movement"
    label = "Classifying movement/behavior"

    def process(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT id, COALESCE(description, summary) FROM sighting
            WHERE description IS NOT NULL OR summary IS NOT NULL
        """)
        rows = cur.fetchall()

        sighting_updates = []
        analysis_updates = []
        tag_counter = Counter()
        cat_counter = Counter()
        has_mov_count = 0

        for sid, text in rows:
            tags, movement = _classify_text_behavior(text)
            categories = _classify_text_movement_categories(text)
            tag_counter.update(tags)
            cat_counter.update(categories)
            has_mov = 1 if categories else 0
            if has_mov:
                has_mov_count += 1

            sighting_updates.append((
                movement,
                has_mov,
                json.dumps(categories),
                sid,
            ))
            analysis_updates.append((json.dumps(tags), sid))

        executemany_batched(
            conn,
            "UPDATE sighting SET movement_type = ?, "
            "has_movement_mentioned = ?, movement_categories = ? WHERE id = ?",
            sighting_updates,
        )
        executemany_batched(
            conn,
            "UPDATE sighting_analysis SET behavior_tags = ? WHERE sighting_id = ?",
            analysis_updates,
        )

        top_tags = ", ".join(f"{k}={v:,}" for k, v in tag_counter.most_common(5))
        top_cats = ", ".join(f"{k}={v:,}" for k, v in cat_counter.most_common(5))
        print(f"  Movement classified: {len(sighting_updates):,} rows with text; "
              f"{has_mov_count:,} had at least one movement category")
        print(f"    top behavior tags:  {top_tags or '(none)'}")
        print(f"    top movement cats:  {top_cats or '(none)'}")
