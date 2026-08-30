"""
UFOCAT importer — CUFOS UFOCAT 2023 catalog.

~320K rows, 55 columns. Richest metadata: Hynek/Vallee classifications,
lat/lon, witness counts, durations.

123K records with SOURCE=UFOReportCtr (NUFORC-origin) are skipped at
import time; their metadata is transferred to NUFORC via enrich.py.
"""

import json
import os
import re

from ufosint.config import Config
from ufosint.importers.base import Importer

SKIP_SOURCES = {"UFOReportCtr"}
ENRICHMENT_PATH = os.path.join(Config.project_root(), "ufocat_enrichment.jsonl")


def parse_ufocat_date(year, mo, day, time_str):
    """Build ISO date from split UFOCAT date fields."""
    try:
        y = int(year) if year else None
    except (ValueError, TypeError):
        y = None
    if not y or y <= 0:
        return None

    try:
        m = int(mo) if mo else 0
        d = int(day) if day else 0
    except (ValueError, TypeError):
        m, d = 0, 0

    if m < 1 or m > 12:
        return str(y)
    if d < 1 or d > 31:
        return f"{y:04d}-{m:02d}"

    iso = f"{y:04d}-{m:02d}-{d:02d}"

    if time_str and time_str.strip():
        t = time_str.strip()
        tm = re.match(r"(\d{1,2}):?(\d{2})", t)
        if tm:
            h, mi = int(tm.group(1)), int(tm.group(2))
            if 0 <= h <= 23 and 0 <= mi <= 59:
                iso += f"T{h:02d}:{mi:02d}"

    return iso


class UfocatImporter(Importer):
    source_name = "UFOCAT"

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "UFOCAT", "ufocat2023.csv")

    @property
    def csv_encoding(self):
        return "utf-8"

    def __init__(self):
        self._enrichment_file = None
        self._skipped_for_enrichment = 0

    def should_skip_row(self, raw):
        source = (raw.get("SOURCE", "") or "").strip()
        return source in SKIP_SOURCES

    def on_skip(self, raw, reason=None):
        """Write skipped UFOReportCtr records to enrichment sidecar."""
        if self._enrichment_file is None:
            self._enrichment_file = open(ENRICHMENT_PATH, "w", encoding="utf-8")

        self._enrichment_file.write(json.dumps({
            "hynek": (raw.get("HYNEK", "") or "").strip() or None,
            "vallee": (raw.get("VALLEE", "") or "").strip() or None,
            "year": raw.get("YEAR"),
            "month": raw.get("MO"),
            "day": raw.get("DAY"),
            "location": (raw.get("LOCATION", "") or "").strip(),
        }, ensure_ascii=False) + "\n")
        self._skipped_for_enrichment += 1

    def on_complete(self, stats):
        if self._enrichment_file:
            self._enrichment_file.close()
            print(f"  Enrichment sidecar: {self._skipped_for_enrichment:,} records -> {ENRICHMENT_PATH}")

    def parse_row(self, raw):
        # Location — UFOCAT has LOCATION in ALL CAPS + separate lat/lon
        raw_text = (raw.get("LOCATION", "") or "").strip() or None
        city = raw_text  # Best we have — often just city name
        state = (raw.get("STATE", "") or "").strip() or None
        # UFOCAT genuinely has no COUNTRY column — REGION/STATE carry the
        # 3-letter code instead. Left as None deliberately, not an oversight.
        country = None

        # v0.16.4 — the columns are LATITUDE / LONGITUDE, not LAT / LON.
        # raw.get("LAT") always returned "", so every UFOCAT coordinate was
        # thrown away and the rows fell back to name-only geocoding: 44.7%
        # of UFOCAT sightings mapped against 89.3% in the April corpus.
        # UFOCAT ships its own coordinates and they are the best we have for
        # this source, so losing them is expensive.
        # Values carry leading whitespace ("  43.33"); float() copes.
        lat = None
        lon = None
        try:
            lat_str = (raw.get("LATITUDE") or "").strip()
            lon_str = (raw.get("LONGITUDE") or "").strip()
            if lat_str and lon_str:
                lat = float(lat_str)
                lon = float(lon_str)
        except (ValueError, TypeError):
            pass

        location = {
            "raw_text": raw_text,
            "city": city,
            "state": state,
            "country": country,
            "latitude": lat,
            "longitude": lon,
        }

        # Date
        date_event = parse_ufocat_date(
            raw.get("YEAR"), raw.get("MO"), raw.get("DAY"), raw.get("TIME")
        )

        # Duration
        duration = (raw.get("DUR", "") or "").strip() or None  # v0.16.4: column is DUR

        # Safe int helper
        def _int(key):
            v = raw.get(key, "")
            try:
                return int(v) if v and str(v).strip() else None
            except (ValueError, TypeError):
                return None

        sighting = {
            # v0.16.4 — no RECORD_ID column exists. PRN and URN are the file's
            # record numbers and agree with each other; PRN is used.
            "source_record_id": (raw.get("PRN", "") or "").strip() or None,
            "date_event": date_event,
            "date_event_raw": f"{raw.get('YEAR','')}-{raw.get('MO','')}-{raw.get('DAY','')}",
            "time_raw": (raw.get("TIME", "") or "").strip() or None,
            # v0.16.4 — the narrative lives in NOTES; there is no DESCRIPTION
            # column, so every UFOCAT description was NULL (April had 98,792).
            "description": (raw.get("NOTES", "") or "").strip() or None,
            "shape": (raw.get("SHAPE", "") or "").strip() or None,
            "color": (raw.get("COLOR", "") or "").strip() or None,
            "duration": duration,
            "num_witnesses": _int("WITNESSES"),
            "num_objects": _int("NO_OBJECTS"),
            "sound": (raw.get("SOUND", "") or "").strip() or None,
            "hynek": (raw.get("HYNEK", "") or "").strip().upper() or None,
            "vallee": (raw.get("VALLEE", "") or "").strip().upper() or None,
            "event_type": (raw.get("TYPE", "") or "").strip() or None,
            "source_ref": (raw.get("SOURCE", "") or "").strip() or None,
        }

        return location, sighting
