"""
Import MUFON CSV into the unified database.
~138K rows, 7 columns.
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint import mufon
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import sqlite3
import csv
import json
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
DATA_DIR = os.environ.get(
    "UFOSINT_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw"),
)
CSV_PATH = os.path.join(DATA_DIR, "mufon.csv")

BATCH_SIZE = 5000


def parse_mufon_date(date_str):
    """Parse MUFON date format like '1992-08-19\\n5:45AM' into ISO."""
    if not date_str or not date_str.strip():
        return None, None

    parts = date_str.strip().split('\n')
    date_part = parts[0].strip() if parts else None
    time_part = parts[1].strip() if len(parts) > 1 else None

    # date_part should be like YYYY-MM-DD
    if date_part and re.match(r'\d{4}-\d{2}-\d{2}', date_part):
        iso = date_part
        if time_part:
            # Convert 12hr to 24hr
            t = time_part.upper().strip()
            m = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM)?', t)
            if m:
                h, mi, ampm = int(m.group(1)), m.group(2), m.group(3)
                if ampm == 'PM' and h != 12:
                    h += 12
                elif ampm == 'AM' and h == 12:
                    h = 0
                iso += f"T{h:02d}:{mi}"
        return iso, date_str.strip()

    return None, date_str.strip()


def parse_mufon_location(loc_str):
    """Parse MUFON location like 'Newscandia\\, MN\\, US'."""
    if not loc_str:
        return None, None, None
    # MUFON uses \, as escaped commas
    loc = loc_str.replace('\\,', ',').strip()
    parts = [p.strip() for p in loc.split(',')]

    city = parts[0] if len(parts) > 0 else None
    state = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None

    return city, state, country


def run_import():
    """Delegate to the ufosint.importers.mufon package module."""
    from ufosint.importers.mufon import MufonImporter
    from ufosint.db import Database
    MufonImporter().run(Database(DB_PATH))


if __name__ == "__main__":
    run_import()
