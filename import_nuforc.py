"""
Import NUFORC CSV into the unified database.
~159K rows, 18 columns.
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint import nuforc
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
DATA_DIR = os.environ.get(
    "UFOSINT_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw"),
)
CSV_PATH = os.path.join(DATA_DIR, "nuforc.csv")

BATCH_SIZE = 5000


def safe_str(val):
    """Safely get a string from a value that might be a list (CSV parsing artifact)."""
    if val is None:
        return ''
    if isinstance(val, list):
        return ', '.join(str(x) for x in val if x)
    return str(val)


def parse_nuforc_date(date_str):
    """Parse NUFORC date like ' 1995-02-02 23:00 Local' into ISO."""
    if not date_str or not date_str.strip():
        return None, None

    raw = date_str.strip()
    # Extract timezone hint
    tz = None
    for tzname in ['Local', 'Pacific', 'Eastern', 'Central', 'Mountain', 'UTC', 'GMT']:
        if tzname in raw:
            tz = tzname
            raw = raw.replace(tzname, '').strip()

    m = re.match(r'(\d{4}-\d{2}-\d{2})\s*(\d{2}:\d{2})?', raw)
    if m:
        iso = m.group(1)
        if m.group(2):
            iso += "T" + m.group(2)
        return iso, date_str.strip()

    return None, date_str.strip()


def parse_nuforc_location(loc_str):
    """Parse NUFORC location like ' Shady Grove, OR, USA'."""
    if not loc_str or not loc_str.strip():
        return None, None, None

    parts = [p.strip() for p in loc_str.strip().split(',')]
    city = parts[0] if len(parts) > 0 else None
    state = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None
    return city, state, country


def safe_int(val):
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def run_import():
    """Delegate to the ufosint.importers.nuforc package module."""
    from ufosint.importers.nuforc import NuforcImporter
    from ufosint.db import Database
    NuforcImporter().run(Database(DB_PATH))


if __name__ == "__main__":
    run_import()
