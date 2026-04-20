"""
Import UFO-search (formerly Geldreich) Majestic Timeline JSON into the unified database.
~54.7K records from 19 historical source compilations.
Source: ufo-search.com
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint import geldreich
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
JSON_PATH = os.path.join(DATA_DIR, "UFO-search", "majestic.json")

BATCH_SIZE = 5000


def parse_geldreich_date(date_str, time_str=None):
    """
    Parse Geldreich's varied date formats:
      "0's", "4/34", "4/4/34", "5/21/70", "1947", "6/24/1947", "Summer 1947"
    Returns (iso_date_or_none, raw_string)
    """
    if not date_str or not date_str.strip():
        return None, None

    raw = date_str.strip()
    d = raw

    # Remove leading/trailing whitespace
    d = d.strip()

    # Handle "Summer 1947", "Fall 1952", etc.
    season_match = re.match(r'(Spring|Summer|Fall|Winter|Early|Late|Mid|End of|Beginning of)\s+(\d{4})', d, re.I)
    if season_match:
        return f"{season_match.group(2)}-01-01", raw

    # Handle just a year like "1947" or "0's"
    year_match = re.match(r"^(\d{1,4})'?s?$", d)
    if year_match:
        y = int(year_match.group(1))
        if y > 0:
            return f"{y:04d}-01-01", raw
        return None, raw

    # Handle M/D/YYYY or M/YYYY or M/D/YY
    slash_match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{1,4})$', d)
    if slash_match:
        a, b, c = int(slash_match.group(1)), int(slash_match.group(2)), int(slash_match.group(3))
        # Determine if M/D/Y
        if c < 100:
            c = c + 1900 if c > 25 else c + 2000
        return f"{c:04d}-{a:02d}-{b:02d}", raw

    # Handle M/YYYY like "4/34" meaning April year 34
    slash2 = re.match(r'^(\d{1,2})/(\d{1,4})$', d)
    if slash2:
        m, y = int(slash2.group(1)), int(slash2.group(2))
        if y < 100:
            y = y + 1900 if y > 25 else y + 2000
        if 1 <= m <= 12:
            return f"{y:04d}-{m:02d}-01", raw

    # Handle YYYY-MM-DD already
    iso_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})', d)
    if iso_match:
        return d[:10], raw

    # Handle plain 4-digit year
    plain_year = re.match(r'^(\d{4})$', d)
    if plain_year:
        return f"{d}-01-01", raw

    return None, raw


def run_import():
    """Delegate to the ufosint.importers.geldreich package module."""
    from ufosint.importers.geldreich import GeldreichImporter
    from ufosint.db import Database
    GeldreichImporter().run(Database(DB_PATH))


if __name__ == "__main__":
    run_import()
