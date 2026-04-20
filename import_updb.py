"""
Import UPDB (phenomenAInon) CSV into the unified database.
~1.9M rows, 9 columns. Selectively skips rows whose 'name' is MUFON or NUFORC
since we already imported those from their richer original CSVs.
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint import updb
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
CSV_PATH = os.path.join(DATA_DIR, "UPDB.app", "phenomenAInon_UPDB.csv")

BATCH_SIZE = 10000

# Skip these sources since we already imported them from their richer original files
SKIP_SOURCES = {'MUFON', 'NUFORC'}


def parse_updb_date(date_str):
    """Parse UPDB date like '1993-05-20 00:00:00'."""
    if not date_str or not date_str.strip():
        return None

    d = date_str.strip()
    # Already in ISO-ish format
    m = re.match(r'(\d{4}-\d{2}-\d{2})', d)
    if m:
        iso = m.group(1)
        # Add time if not 00:00:00
        time_m = re.search(r'(\d{2}:\d{2}:\d{2})', d)
        if time_m and time_m.group(1) != '00:00:00':
            iso += "T" + time_m.group(1)
        return iso
    return None


def run_import():
    """Delegate to the ufosint.importers.updb package module."""
    from ufosint.importers.updb import UpdbImporter
    from ufosint.db import Database
    UpdbImporter().run(Database(DB_PATH))


if __name__ == "__main__":
    run_import()
