"""
Non-destructive enrichment: duration parsing + shape mapping improvements.

Generates preview CSVs for review before applying to the database.
Each fix is staged in data/output/ — inspect before committing to the DB.

Usage:
    python enrich_free_wins.py --preview          # generate CSVs, don't touch DB
    python enrich_free_wins.py --apply-durations   # apply duration fixes to DB
    python enrich_free_wins.py --apply-shapes      # apply shape fixes to DB
    python enrich_free_wins.py --apply-all         # apply both
    python enrich_free_wins.py --stats             # show current gaps
"""

import csv
import json
import os
import re
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "output")

DURATION_CSV = os.path.join(OUTPUT_DIR, "staged_duration_fixes.csv")
SHAPE_CSV = os.path.join(OUTPUT_DIR, "staged_shape_fixes.csv")


# ============================================================
# DURATION PARSING
# ============================================================

# UFOCAT duration codes (from the UFOCAT codebook)
UFOCAT_DURATION_CODES = {
    "I": 0,          # Instantaneous
    "B": 3,          # Brief (< 5 seconds)
    "S": 15,         # Short (seconds)
    "M": 120,        # Medium (minutes)
    "L": 1800,       # Long (30 min+)
    "E": 7200,       # Extended (hours)
    "F": 1,          # Flash
    "H": 3600,       # Hour
    "SH": 1800,      # Short-hour (~30 min)
    ".F": 1,         # sub-Flash
    "2H": 7200,      # 2 hours
    "3H": 10800,     # 3 hours
    "4H": 14400,     # 4 hours
    "5H": 18000,     # 5 hours
    "6H": 21600,     # 6 hours
    ".S": 10,         # sub-Short
    ".M": 60,         # sub-Medium
    ".B": 2,          # sub-Brief
    "+H": 3600,       # more than an hour
    "+M": 300,        # more than medium (5 min)
    "+L": 3600,       # more than long
}

# Time unit multipliers
UNIT_SECONDS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}

# Regex patterns for duration strings
DURATION_PATTERNS = [
    # "5 minutes", "30 seconds", "1.5 hours", "2-3 minutes" (take first number)
    (r'^(\d+(?:\.\d+)?)\s*[-–to]*\s*\d*\s*(seconds?|secs?|sec|minutes?|mins?|min|hours?|hrs?|hr|days?)\s*[+]?$',
     lambda m: float(m.group(1)) * UNIT_SECONDS.get(m.group(2).lower().rstrip('s') if m.group(2).lower() not in UNIT_SECONDS else m.group(2).lower(), 60)),

    # "about 5 minutes", "approximately 10 seconds", "~20 minutes"
    (r'^(?:about|approximately|approx\.?|~|around|roughly|maybe|est\.?|over|under|less than|more than)\s*(\d+(?:\.\d+)?)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?|days?)\s*[+]?$',
     lambda m: float(m.group(1)) * UNIT_SECONDS.get(m.group(2).lower().rstrip('s') if m.group(2).lower() not in UNIT_SECONDS else m.group(2).lower(), 60)),

    # "a few minutes", "several seconds", "a couple hours"
    (r'^(?:a\s+)?few\s+(seconds?|minutes?|hours?)', lambda m: 3 * UNIT_SECONDS.get(m.group(1).lower().rstrip('s'), 60)),
    (r'^several\s+(seconds?|minutes?|hours?)', lambda m: 5 * UNIT_SECONDS.get(m.group(1).lower().rstrip('s'), 60)),
    (r'^(?:a\s+)?couple\s+(?:of\s+)?(seconds?|minutes?|hours?)', lambda m: 2 * UNIT_SECONDS.get(m.group(1).lower().rstrip('s'), 60)),

    # Bare numbers: assume minutes for UFOCAT/UPDB sources (their convention)
    (r'^(\d+(?:\.\d+)?)\s*$', lambda m: float(m.group(1)) * 60),

    # Decimal fractions: "0.1" = 6 seconds (UFOCAT uses decimal minutes)
    # Already caught by bare numbers above

    # "1-2 min", "5-10 secs"
    (r'^(\d+)\s*[-–]\s*(\d+)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)',
     lambda m: ((int(m.group(1)) + int(m.group(2))) / 2) * UNIT_SECONDS.get(m.group(3).lower().rstrip('s'), 60)),

    # "45min", "5min", "10sec" (no space)
    (r'^(\d+(?:\.\d+)?)(sec|min|hr|hour)s?\s*$',
     lambda m: float(m.group(1)) * UNIT_SECONDS.get(m.group(2).lower(), 60)),

    # "5 min.", "10 sec.", "2 hr." (with period)
    (r'^(\d+(?:\.\d+)?)\s*(min|sec|hr)s?\.?\s*$',
     lambda m: float(m.group(1)) * UNIT_SECONDS.get(m.group(2).lower(), 60)),

    # "<1 minute", ">5 seconds"
    (r'^[<>]\s*(\d+(?:\.\d+)?)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)',
     lambda m: float(m.group(1)) * UNIT_SECONDS.get(m.group(2).lower().rstrip('s') if m.group(2).lower() not in UNIT_SECONDS else m.group(2).lower(), 60)),

    # Bare decimals like ".5", ".3" — UFOCAT decimal minutes
    (r'^\.(\d+)\s*$', lambda m: float("0." + m.group(1)) * 60),

    # "+1", "+2" — UFOCAT "more than N minutes"
    (r'^\+(\d+)\s*$', lambda m: float(m.group(1)) * 60),

    # Bare words
    (r'^seconds?\s*$', lambda _: 10),        # "seconds" with no number ~ 10s
    (r'^minutes?\s*$', lambda _: 60),         # "minutes" ~ 1 min
    (r'^hours?\s*$', lambda _: 3600),
    (r'^instant(?:aneous)?\s*$', lambda _: 1),
    (r'^brief\s*$', lambda _: 3),
    (r'^moment(?:ary)?\s*$', lambda _: 2),
    (r'^split\s*second\s*$', lambda _: 1),
    (r'^ongoing\s*$', lambda _: 3600),
    (r'^continuous\s*$', lambda _: 3600),
    (r'^all\s*(?:night|day)\s*$', lambda _: 28800),   # 8 hours
]


def parse_duration(raw):
    """Parse a duration string to seconds. Returns (seconds, method) or (None, None)."""
    if not raw:
        return None, None

    text = raw.strip()

    # UFOCAT single-letter codes
    if text.upper() in UFOCAT_DURATION_CODES:
        return UFOCAT_DURATION_CODES[text.upper()], "ufocat_code"

    # Try each regex pattern
    text_lower = text.lower().strip().rstrip("+").strip()
    for pattern, calculator in DURATION_PATTERNS:
        m = re.match(pattern, text_lower, re.IGNORECASE)
        if m:
            try:
                seconds = calculator(m)
                if seconds is not None and 0 < seconds <= 365 * 86400:  # cap at 1 year
                    return int(round(seconds)), "regex"
            except (ValueError, TypeError):
                pass

    return None, None


def stage_duration_fixes(db_path=DB_PATH):
    """Parse all unparsed durations and write results to a staging CSV."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, duration, source_db_id FROM sighting
        WHERE duration IS NOT NULL AND duration_seconds IS NULL
    """)
    rows = cur.fetchall()
    conn.close()

    print(f"\n=== Duration Parsing ===\n")
    print(f"  Rows with unparsed duration: {len(rows):,}")

    parsed = []
    unparsed = []
    method_counts = {}

    for sid, duration, src_id in rows:
        seconds, method = parse_duration(duration)
        if seconds is not None:
            parsed.append((sid, duration, seconds, method, src_id))
            method_counts[method] = method_counts.get(method, 0) + 1
        else:
            unparsed.append((sid, duration, src_id))

    print(f"  Successfully parsed: {len(parsed):,}")
    print(f"  Still unparsed: {len(unparsed):,}")
    print(f"  By method: {method_counts}")

    # Write staged fixes
    with open(DURATION_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sighting_id", "original_duration", "parsed_seconds", "parse_method", "source_db_id"])
        for row in parsed:
            w.writerow(row)

    print(f"\n  Staged to: {DURATION_CSV}")
    print(f"  Review the CSV, then run: python enrich_free_wins.py --apply-durations")

    # Show top still-unparsed for debugging
    if unparsed:
        from collections import Counter
        top = Counter(r[1] for r in unparsed).most_common(15)
        print(f"\n  Top 15 still-unparsed:")
        for dur, n in top:
            print(f"    {n:>5}  \"{dur[:50]}\"")

    return len(parsed)


# ============================================================
# SHAPE MAPPING
# ============================================================

# Extended shape aliases for the 27,599 that fall through to "Other"
EXTENDED_SHAPE_MAP = {
    # Direct mappings from UFOCAT abbreviated forms
    "ovoid": "Oval",
    "rectangl": "Rectangle",
    "v-shape": "Chevron",
    "v-form": "Chevron",
    "polymorf": "Changing",        # polymorphic = changing shape
    "irregulr": "Other",           # genuinely irregular — keep as Other
    "dome": "Disc",                # dome-shaped = disc variant
    "formatn": "Formation",
    "ellipse": "Oval",
    "elliptic": "Oval",
    "delta": "Triangle",
    "crescent": "Crescent",        # new canonical
    "saturn": "Disc",              # saturn-shaped = disc with ring
    "top": "Cone",                 # spinning top = cone-like
    "elongate": "Cigar",           # elongated = cigar-like
    "cloud": "Cloud",              # new canonical — genuinely cloud-like
    "beam": "Light",               # beam of light
    "oblong": "Cigar",             # oblong = elongated
    "polygon": "Diamond",          # polygonal, closest match
    "ring": "Circle",              # ring-shaped
    "copter": "Other",             # helicopter-like — not a UFO shape
    "bullet": "Cigar",             # bullet-shaped
    "fuselage": "Cigar",           # fuselage-shaped
    "torpedo": "Cigar",            # torpedo = cigar
    "linear": "Cigar",             # linear = elongated
    "blimp": "Cigar",              # blimp-shaped
    "box": "Rectangle",            # box = rectangular
    "airship": "Cigar",            # airship = cigar-like
    "pear": "Teardrop",            # pear-shaped ~ teardrop
    "wheel": "Circle",             # wheel = circular
    "dumbbell": "Other",           # dumbbell is unique enough
    "rocket": "Cylinder",          # rocket-shaped
    "arrow": "Chevron",            # arrow ~ chevron
    "pyramid": "Triangle",         # pyramid ~ triangular
    "aircraft": "Other",           # aircraft-like — not a standard shape
    "mantaray": "Chevron",         # manta ray ~ chevron/wing
    "wedge": "Triangle",           # wedge ~ triangular
    "banana": "Cigar",             # banana ~ elongated
    "barrel": "Cylinder",          # barrel = cylindrical
    "horseshoe": "Chevron",        # horseshoe ~ V-shape
    "mushroom": "Dome",            # new canonical
    "bell": "Cone",                # bell ~ conical
    "hat": "Disc",                 # hat-shaped ~ disc
    "shield": "Other",
    "jellyfish": "Other",          # unique enough to keep
    "acorn": "Egg",                # acorn ~ egg-shaped
    "football": "Oval",            # football = oval
    "pellet": "Sphere",            # pellet ~ spherical
    "hexagon": "Diamond",          # hexagonal ~ diamond
    "pentagon": "Diamond",
    "trapezoid": "Rectangle",
    "semi-circle": "Circle",
    "semicircle": "Circle",
    "half-circle": "Circle",
    "blob": "Changing",
    "amorphous": "Changing",
    "point": "Light",              # point of light
    "dot": "Light",
    "spike": "Cigar",
    "rod": "Cigar",
    "stick": "Cigar",
    "flat": "Disc",
    "round": "Circle",
    "circular": "Circle",
    "spherical": "Sphere",
    "triangular": "Triangle",
    "rectangular": "Rectangle",
    "cylindrical": "Cylinder",
    "conical": "Cone",
    "saucer-shaped": "Saucer",
    "disc-shaped": "Disc",
    "cigar-shaped": "Cigar",
    "egg-shaped": "Egg",
    "tear-drop": "Teardrop",
    "tear drop": "Teardrop",
    "v-shaped": "Chevron",
    "v shaped": "Chevron",
    "boomerang-shaped": "Boomerang",
}

# New canonical shapes to add (keeping the existing 25 + adding a few)
NEW_CANONICAL = ["Crescent", "Cloud", "Dome"]


def stage_shape_fixes(db_path=DB_PATH):
    """Map unmapped shapes and write results to a staging CSV."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, shape, standardized_shape FROM sighting
        WHERE shape IS NOT NULL AND standardized_shape = 'Other'
        AND shape != 'Other'
    """)
    rows = cur.fetchall()
    conn.close()

    print(f"\n=== Shape Mapping ===\n")
    print(f"  Shapes currently mapped to Other: {len(rows):,}")

    remapped = []
    still_other = []
    remap_counts = {}

    for sid, raw_shape, old_std in rows:
        key = raw_shape.lower().strip()

        new_std = EXTENDED_SHAPE_MAP.get(key)
        if new_std and new_std != "Other":
            remapped.append((sid, raw_shape, old_std, new_std))
            remap_counts[new_std] = remap_counts.get(new_std, 0) + 1
        else:
            still_other.append((sid, raw_shape))

    print(f"  Remappable: {len(remapped):,}")
    print(f"  Still Other: {len(still_other):,}")
    print(f"\n  Remap distribution:")
    for shape, n in sorted(remap_counts.items(), key=lambda x: -x[1]):
        print(f"    {shape:<15} {n:>6,}")

    # Write staged fixes
    with open(SHAPE_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sighting_id", "original_shape", "old_standardized", "new_standardized"])
        for row in remapped:
            w.writerow(row)

    print(f"\n  Staged to: {SHAPE_CSV}")
    print(f"  Review the CSV, then run: python enrich_free_wins.py --apply-shapes")

    # Top still-Other
    if still_other:
        from collections import Counter
        top = Counter(r[1] for r in still_other).most_common(10)
        print(f"\n  Top 10 still-Other shapes:")
        for shape, n in top:
            s = (shape or "")[:30]
            print(f"    {n:>5}  \"{s}\"")

    return len(remapped)


# ============================================================
# APPLY STAGED FIXES
# ============================================================

def apply_durations(db_path=DB_PATH):
    """Apply staged duration fixes from CSV to the database."""
    if not os.path.exists(DURATION_CSV):
        print(f"  No staged duration fixes found at {DURATION_CSV}")
        print(f"  Run: python enrich_free_wins.py --preview")
        return 0

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    with open(DURATION_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        updates = [(int(r["parsed_seconds"]), int(r["sighting_id"])) for r in reader]

    print(f"\n  Applying {len(updates):,} duration fixes...")
    cur.executemany(
        "UPDATE sighting SET duration_seconds = ? WHERE id = ? AND duration_seconds IS NULL",
        updates
    )
    conn.commit()
    print(f"  Applied: {cur.rowcount:,} rows updated")
    conn.close()
    return len(updates)


def apply_shapes(db_path=DB_PATH):
    """Apply staged shape fixes from CSV to the database."""
    if not os.path.exists(SHAPE_CSV):
        print(f"  No staged shape fixes found at {SHAPE_CSV}")
        print(f"  Run: python enrich_free_wins.py --preview")
        return 0

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    with open(SHAPE_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        updates = [(r["new_standardized"], int(r["sighting_id"])) for r in reader]

    print(f"\n  Applying {len(updates):,} shape fixes...")
    cur.executemany(
        "UPDATE sighting SET standardized_shape = ? WHERE id = ? AND standardized_shape = 'Other'",
        updates
    )
    conn.commit()
    print(f"  Applied: {cur.rowcount:,} rows updated")
    conn.close()
    return len(updates)


def print_stats(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print(f"\n=== Enrichment Gaps ===\n")

    cur.execute("SELECT COUNT(*) FROM sighting WHERE duration IS NOT NULL AND duration_seconds IS NULL")
    print(f"  Unparsed durations:         {cur.fetchone()[0]:>10,}")

    cur.execute("SELECT COUNT(*) FROM sighting WHERE duration_seconds IS NOT NULL")
    print(f"  Parsed durations:           {cur.fetchone()[0]:>10,}")

    cur.execute("SELECT COUNT(*) FROM sighting WHERE shape IS NOT NULL AND standardized_shape = 'Other' AND shape != 'Other'")
    print(f"  Shapes mapped to Other:     {cur.fetchone()[0]:>10,}")

    cur.execute("SELECT COUNT(DISTINCT standardized_shape) FROM sighting WHERE standardized_shape IS NOT NULL")
    print(f"  Distinct canonical shapes:  {cur.fetchone()[0]:>10,}")

    cur.execute("SELECT duration_bucket, COUNT(*) FROM sighting WHERE duration_bucket IS NOT NULL GROUP BY 1 ORDER BY COUNT(*) DESC")
    print(f"\n  Duration buckets:")
    for bucket, n in cur.fetchall():
        print(f"    {bucket:<12} {n:>10,}")

    conn.close()


# ============================================================
# CLI
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Non-destructive data enrichment")
    parser.add_argument("--preview", action="store_true", help="Stage fixes to CSV for review (no DB changes)")
    parser.add_argument("--apply-durations", action="store_true", help="Apply staged duration fixes to DB")
    parser.add_argument("--apply-shapes", action="store_true", help="Apply staged shape fixes to DB")
    parser.add_argument("--apply-all", action="store_true", help="Apply both duration and shape fixes")
    parser.add_argument("--stats", action="store_true", help="Show current enrichment gaps")
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    if args.stats:
        print_stats(args.db)
    elif args.preview:
        stage_duration_fixes(args.db)
        stage_shape_fixes(args.db)
    elif args.apply_durations or args.apply_all:
        apply_durations(args.db)
        if args.apply_all:
            apply_shapes(args.db)
    elif args.apply_shapes:
        apply_shapes(args.db)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
