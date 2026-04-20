"""
Import UFOCAT 2023 CSV into the unified database.
UFOCAT is the richest structured dataset with 55 columns.

Skips records whose SOURCE field matches SKIP_SOURCES (e.g. 'UFOReportCtr'
= NUFORC-origin records). Those skipped records are saved to a sidecar
JSON file so their metadata (Hynek, Vallee, shape) can be transferred
to the canonical NUFORC records by enrich.py.
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint import ufocat
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import sqlite3
import csv
import json
import os
import sys
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
DATA_DIR = os.environ.get(
    "UFOSINT_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw"),
)
CSV_PATH = os.path.join(DATA_DIR, "UFOCAT", "ufocat2023.csv")
ENRICHMENT_PATH = os.path.join(os.path.dirname(__file__), "ufocat_enrichment.jsonl")

BATCH_SIZE = 5000

# Skip these UFOCAT sub-sources — already imported from richer originals.
# Same pattern as import_updb.py's SKIP_SOURCES.
SKIP_SOURCES = {'UFOReportCtr'}  # NUFORC-origin records (~123K)

def parse_ufocat_date(year, mo, day, time_str):
    """Try to build an ISO date from UFOCAT's split date fields."""
    try:
        y = int(year) if year and year.strip() else None
        m = int(mo) if mo and mo.strip() else None
        d = int(day) if day and day.strip() else None
    except (ValueError, TypeError):
        return None

    if y is None or y == 0:
        return None

    parts = [f"{y:04d}"]
    if m and 1 <= m <= 12:
        parts.append(f"{m:02d}")
        if d and 1 <= d <= 31:
            parts.append(f"{d:02d}")
        else:
            parts.append("01")
    else:
        parts.extend(["01", "01"])

    date_str = "-".join(parts)

    if time_str and time_str.strip():
        t = time_str.strip()
        # Try to parse HH:MM or HHMM formats
        t = t.replace(".", ":").replace(";", ":")
        if re.match(r'^\d{3,4}$', t):
            t = t.zfill(4)
            t = t[:2] + ":" + t[2:]
        if re.match(r'^\d{1,2}:\d{2}', t):
            date_str += "T" + t

    return date_str


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


def safe_float(val):
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        f = float(val)
        if f == 0.0:
            return None  # UFOCAT uses 0 for unknown
        return f
    except (ValueError, TypeError):
        return None


def run_import():
    """Delegate to the ufosint.importers.ufocat package module."""
    from ufosint.importers.ufocat import UfocatImporter
    from ufosint.db import Database
    UfocatImporter().run(Database(DB_PATH))


if __name__ == "__main__":
    run_import()
