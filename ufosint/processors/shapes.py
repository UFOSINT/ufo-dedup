"""
Shape normalization processor.

Fuzzy-matches raw shape strings against 28 canonical shapes using:
  1. Exact match (lowercase)
  2. Substring/alias match (70+ mappings)
  3. Fuzzy match via rapidfuzz (score >= 85)
"""

import re

from ufosint.processors.base import Processor, executemany_batched

CANONICAL_SHAPES = [
    "Sphere", "Disc", "Triangle", "Cigar", "Oval", "Circle", "Light",
    "Fireball", "Cylinder", "Diamond", "Rectangle", "Chevron", "Cross",
    "Teardrop", "Star", "Egg", "Cone", "Cube", "Saucer", "Boomerang",
    "Flash", "Formation", "Changing", "Crescent", "Cloud", "Dome",
    "Unknown", "Other",
]

# Substring/alias mappings — checked before fuzzy matching
SHAPE_ALIASES = {
    # Original
    "triangular": "Triangle", "triangle": "Triangle",
    "disc": "Disc", "disk": "Disc", "saucer": "Saucer",
    "cigar": "Cigar", "sphere": "Sphere", "spherical": "Sphere",
    "orb": "Sphere", "ball": "Sphere", "round": "Circle",
    "circular": "Circle", "oval": "Oval", "egg": "Egg",
    "cylinder": "Cylinder", "cylindrical": "Cylinder", "tube": "Cylinder",
    "cone": "Cone", "chevron": "Chevron", "boomerang": "Boomerang",
    "rectangle": "Rectangle", "rectangular": "Rectangle",
    "diamond": "Diamond", "teardrop": "Teardrop", "fireball": "Fireball",
    "flash": "Flash", "light": "Light", "star": "Star", "cube": "Cube",
    "cross": "Cross", "formation": "Formation", "changing": "Changing",
    "unknown": "Unknown",
    # v0.14 extended — UFOCAT abbreviations + synonyms
    "ovoid": "Oval", "ellipse": "Oval", "elliptic": "Oval", "football": "Oval",
    "oblong": "Cigar", "elongate": "Cigar", "torpedo": "Cigar",
    "bullet": "Cigar", "fuselage": "Cigar", "linear": "Cigar",
    "blimp": "Cigar", "airship": "Cigar", "banana": "Cigar",
    "spike": "Cigar", "rod": "Cigar", "stick": "Cigar",
    "cigar-shaped": "Cigar",
    "rectangl": "Rectangle", "box": "Rectangle", "trapezoid": "Rectangle",
    "flat": "Disc", "hat": "Disc", "saturn": "Disc",
    "disc-shaped": "Disc", "saucer-shaped": "Saucer",
    "v-shape": "Chevron", "v-form": "Chevron", "v-shaped": "Chevron",
    "v shaped": "Chevron", "arrow": "Chevron", "mantaray": "Chevron",
    "horseshoe": "Chevron",
    "delta": "Triangle", "pyramid": "Triangle", "wedge": "Triangle",
    "formatn": "Formation",
    "polymorf": "Changing", "blob": "Changing", "amorphous": "Changing",
    "crescent": "Crescent",
    "cloud": "Cloud",
    "dome": "Dome", "mushroom": "Dome",
    "top": "Cone", "bell": "Cone", "conical": "Cone",
    "ring": "Circle", "wheel": "Circle", "semi-circle": "Circle",
    "semicircle": "Circle", "half-circle": "Circle",
    "pellet": "Sphere", "acorn": "Egg", "egg-shaped": "Egg",
    "pear": "Teardrop", "tear-drop": "Teardrop", "tear drop": "Teardrop",
    "point": "Light", "dot": "Light", "beam": "Light",
    "barrel": "Cylinder", "rocket": "Cylinder",
    "hexagon": "Diamond", "pentagon": "Diamond", "polygon": "Diamond",
    "boomerang-shaped": "Boomerang",
}


def _normalize_token(raw):
    """Clean a raw shape string for matching."""
    if not raw:
        return None
    token = raw.strip().lower()
    token = re.sub(r'[^a-z0-9\s\-]', '', token)
    token = re.sub(r'\s+', ' ', token).strip()
    # Strip plurals
    if token.endswith("s") and len(token) > 3:
        token = token[:-1]
    return token or None


def _match_shape(token, fuzzy_fn):
    """Match a normalized token to a canonical shape.

    Returns (canonical_shape, method).
    """
    if not token:
        return "Unknown", "exact"

    canonical_lower = {c.lower(): c for c in CANONICAL_SHAPES}

    # 1. Exact match
    if token in canonical_lower:
        return canonical_lower[token], "exact"

    # 2. Alias/substring
    for alias, canonical in SHAPE_ALIASES.items():
        if alias in token:
            return canonical, "substring"

    # 3. Fuzzy match
    if fuzzy_fn:
        result = fuzzy_fn(token, CANONICAL_SHAPES, score_cutoff=85)
        if result:
            return result[0], "fuzzy"

    return "Other", "unmatched"


class ShapeNormalizer(Processor):
    name = "shapes"
    label = "Normalizing shapes"

    def process(self, conn):
        try:
            from rapidfuzz.process import extractOne as fuzzy_fn
        except ImportError:
            fuzzy_fn = None

        cur = conn.cursor()
        cur.execute("SELECT id, shape FROM sighting WHERE shape IS NOT NULL")
        rows = cur.fetchall()

        sighting_updates = []
        analysis_updates = []
        counts = {"exact": 0, "substring": 0, "fuzzy": 0, "unmatched": 0}

        for sid, raw_shape in rows:
            token = _normalize_token(raw_shape)
            canonical, method = _match_shape(token, fuzzy_fn)
            counts[method] = counts.get(method, 0) + 1
            sighting_updates.append((canonical, sid))
            analysis_updates.append((method, sid))

        executemany_batched(
            conn,
            "UPDATE sighting SET standardized_shape = ? WHERE id = ?",
            sighting_updates,
        )
        executemany_batched(
            conn,
            "UPDATE sighting_analysis SET raw_shape_matched_via = ? WHERE sighting_id = ?",
            analysis_updates,
        )

        breakdown = ", ".join(f"{k}={v:,}" for k, v in counts.items())
        print(f"  Shapes normalized: {len(rows):,} ({breakdown})")
