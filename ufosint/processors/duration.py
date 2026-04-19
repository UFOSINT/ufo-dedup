"""
Duration parsing and bucketing processor.

Parses raw duration text strings ("5 minutes", "B", "0.5") into integer
seconds, then buckets into coarse categories (instant/seconds/minutes/hours/days).
"""

import re

from ufosint.processors.base import Processor, executemany_batched

# UFOCAT single-letter duration codes
UFOCAT_CODES = {
    "I": 0, "B": 3, "S": 15, "M": 120, "L": 1800, "E": 7200,
    "F": 1, "H": 3600, "SH": 1800, ".F": 1, ".S": 10, ".M": 60, ".B": 2,
    "2H": 7200, "3H": 10800, "4H": 14400, "5H": 18000, "6H": 21600,
    "+H": 3600, "+M": 300, "+L": 3600,
}

_UNIT_SECS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}


def _lookup_unit(raw):
    key = raw.lower().rstrip("s.").strip()
    return _UNIT_SECS.get(key, _UNIT_SECS.get(raw.lower().strip(), 60))


_PATTERNS = [
    (re.compile(r'^(\d+(?:\.\d+)?)\s*[-\u2013to]*\s*\d*\s*(seconds?|secs?|sec|minutes?|mins?|min|hours?|hrs?|hr|days?)\.?\s*[+]?$', re.I),
     lambda m: float(m.group(1)) * _lookup_unit(m.group(2))),
    (re.compile(r'^(?:about|approximately|approx\.?|~|around|roughly|maybe|est\.?|over|under|less than|more than)\s*(\d+(?:\.\d+)?)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?|days?)\.?\s*[+]?$', re.I),
     lambda m: float(m.group(1)) * _lookup_unit(m.group(2))),
    (re.compile(r'^(?:a\s+)?few\s+(seconds?|minutes?|hours?)', re.I),
     lambda m: 3 * _lookup_unit(m.group(1))),
    (re.compile(r'^several\s+(seconds?|minutes?|hours?)', re.I),
     lambda m: 5 * _lookup_unit(m.group(1))),
    (re.compile(r'^(?:a\s+)?couple\s+(?:of\s+)?(seconds?|minutes?|hours?)', re.I),
     lambda m: 2 * _lookup_unit(m.group(1))),
    (re.compile(r'^(\d+)\s*[-\u2013]\s*(\d+)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)\.?', re.I),
     lambda m: ((int(m.group(1)) + int(m.group(2))) / 2) * _lookup_unit(m.group(3))),
    (re.compile(r'^(\d+(?:\.\d+)?)(sec|min|hr|hour)s?\.?\s*$', re.I),
     lambda m: float(m.group(1)) * _lookup_unit(m.group(2))),
    (re.compile(r'^(\d+(?:\.\d+)?)\s*(min|sec|hr)s?\.?\s*$', re.I),
     lambda m: float(m.group(1)) * _lookup_unit(m.group(2))),
    (re.compile(r'^[<>]\s*(\d+(?:\.\d+)?)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)', re.I),
     lambda m: float(m.group(1)) * _lookup_unit(m.group(2))),
    (re.compile(r'^\.(\d+)\s*$'), lambda m: float("0." + m.group(1)) * 60),
    (re.compile(r'^\+(\d+)\s*$'), lambda m: float(m.group(1)) * 60),
    (re.compile(r'^(\d+(?:\.\d+)?)\s*$'), lambda m: float(m.group(1)) * 60),
    (re.compile(r'^seconds?\s*$', re.I), lambda _: 10),
    (re.compile(r'^minutes?\s*$', re.I), lambda _: 60),
    (re.compile(r'^hours?\s*$', re.I), lambda _: 3600),
    (re.compile(r'^instant(?:aneous)?\s*$', re.I), lambda _: 1),
    (re.compile(r'^brief\s*$', re.I), lambda _: 3),
    (re.compile(r'^moment(?:ary)?\s*$', re.I), lambda _: 2),
    (re.compile(r'^split\s*second\s*$', re.I), lambda _: 1),
]


def parse_duration_text(raw):
    """Parse a duration string to integer seconds, or None."""
    if not raw:
        return None
    text = raw.strip()
    if text.upper() in UFOCAT_CODES:
        return UFOCAT_CODES[text.upper()]
    text_clean = text.rstrip("+").strip()
    for pattern, calc in _PATTERNS:
        m = pattern.match(text_clean)
        if m:
            try:
                secs = calc(m)
                if secs is not None and 0 < secs <= 365 * 86400:
                    return int(round(secs))
            except (ValueError, TypeError):
                pass
    return None


class DurationProcessor(Processor):
    name = "duration"
    label = "Parsing durations and bucketing"

    def process(self, conn):
        cur = conn.cursor()

        # Phase 1: parse text to seconds
        cur.execute("""
            SELECT id, duration FROM sighting
            WHERE duration IS NOT NULL AND duration_seconds IS NULL
        """)
        rows = cur.fetchall()
        updates = []
        for sid, duration in rows:
            secs = parse_duration_text(duration)
            if secs is not None:
                updates.append((secs, sid))

        if updates:
            executemany_batched(
                conn,
                "UPDATE sighting SET duration_seconds = ? WHERE id = ?",
                updates,
            )

        # Phase 2: bucket
        cur.execute("""
            UPDATE sighting SET duration_bucket = CASE
                WHEN duration_seconds IS NULL OR duration_seconds = 0 THEN NULL
                WHEN duration_seconds < 5 THEN 'instant'
                WHEN duration_seconds < 60 THEN 'seconds'
                WHEN duration_seconds < 3600 THEN 'minutes'
                WHEN duration_seconds < 86400 THEN 'hours'
                ELSE 'days'
            END
        """)
        conn.commit()

        cur.execute("""
            SELECT duration_bucket, COUNT(*) FROM sighting
            WHERE duration_bucket IS NOT NULL GROUP BY duration_bucket
        """)
        dist = dict(cur.fetchall())
        breakdown = ", ".join(f"{k}={v:,}" for k, v in dist.items())
        total = sum(dist.values())
        print(f"  Duration: parsed {len(updates):,} text strings, "
              f"bucketed {total:,} total ({breakdown})")
