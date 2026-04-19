"""
UFO-search importer — Rich Geldreich's Majestic Timeline.

~55K records from majestic.json. 19+ historical source compilations.
Highly variable date formats ("Summer 1947", "4/34", "0's", "6/24/1947").
"""

import json
import os
import re

from ufosint.config import Config
from ufosint.importers.base import Importer


def parse_geldreich_date(date_str, time_str=None):
    """Parse wildly variable date formats into best-effort ISO."""
    if not date_str or not date_str.strip():
        return None, None

    raw = date_str.strip()

    # Standard ISO: 1947-06-24
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        iso = m.group(0)
        if time_str and re.match(r"\d{1,2}:\d{2}", time_str.strip()):
            iso += "T" + time_str.strip()
        return iso, raw

    # US format: 6/24/1947 or 06/24/1947
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        mo, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{mo:02d}-{day:02d}", raw

    # Year/month: 6/1947, 06/1947
    m = re.match(r"(\d{1,2})/(\d{4})", raw)
    if m:
        mo, year = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return f"{year:04d}-{mo:02d}", raw

    # Bare year: 1947
    m = re.match(r"(\d{4})$", raw)
    if m:
        return m.group(1), raw

    # Year with suffix: "1947-06" or "1947-6"
    m = re.match(r"(\d{4})-(\d{1,2})$", raw)
    if m:
        year, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return f"{year:04d}-{mo:02d}", raw

    return None, raw


def parse_geldreich_location(loc_str):
    """Parse free-text location into (city, state, country, raw)."""
    if not loc_str or not loc_str.strip():
        return None, None, None, None

    raw = loc_str.strip()

    # Try "City, State" pattern (US)
    m = re.match(r"^(.+?),\s*([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip(), m.group(2), "US", raw

    # Try "COUNTRY, City" pattern (UFO-search uses this for non-US)
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 2:
        return parts[-1], None, None, raw

    return None, None, None, raw


class GeldreichImporter(Importer):
    source_name = "UFO-search"

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "UFO-search", "majestic.json")

    @property
    def file_format(self):
        return "json"

    @property
    def csv_encoding(self):
        return "utf-8-sig"

    def parse_row(self, raw):
        # Location
        loc_str = raw.get("location", "")
        city, state, country, raw_text = parse_geldreich_location(loc_str)

        location = {
            "raw_text": raw_text,
            "city": city,
            "state": state,
            "country": country,
        }

        # Date
        date_event, date_raw = parse_geldreich_date(
            raw.get("date", ""), raw.get("time")
        )

        # Sources array → source_ref
        sources = raw.get("sources", [])
        source_ref = ", ".join(sources) if isinstance(sources, list) else str(sources or "")

        sighting = {
            "source_record_id": raw.get("id") or raw.get("record_id"),
            "date_event": date_event,
            "date_event_raw": date_raw,
            "description": (raw.get("description", "") or "").strip() or None,
            "source_ref": source_ref or None,
            "raw_json": json.dumps(raw, ensure_ascii=False),
        }

        return location, sighting
