"""
NUFORC importer — National UFO Reporting Center.

~159K rows from nuforc.csv. 18 columns including detailed free-text descriptions.
Enriched post-import with UFOCAT Hynek/Vallee classifications via enrich.py.
"""

import json
import os
import re

from ufosint.config import Config
from ufosint.importers.base import Importer


def _safe_str(val):
    """CSV DictReader sometimes returns lists for malformed fields."""
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join(str(x) for x in val if x)
    return str(val)


def _safe_int(val):
    s = _safe_str(val).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def parse_nuforc_date(date_str):
    """Parse NUFORC date like '1995-02-02 23:00 Local' into (ISO, raw)."""
    if not date_str or not date_str.strip():
        return None, None

    raw = date_str.strip()
    for tzname in ["Local", "Pacific", "Eastern", "Central", "Mountain", "UTC", "GMT"]:
        if tzname in raw:
            raw = raw.replace(tzname, "").strip()

    m = re.match(r"(\d{4}-\d{2}-\d{2})\s*(\d{2}:\d{2})?", raw)
    if m:
        iso = m.group(1)
        if m.group(2):
            iso += "T" + m.group(2)
        return iso, date_str.strip()

    return None, date_str.strip()


def parse_nuforc_location(loc_str):
    """Parse 'Shady Grove, OR, USA' into (city, state, country)."""
    if not loc_str or not loc_str.strip():
        return None, None, None
    parts = [p.strip() for p in loc_str.strip().split(",")]
    city = parts[0] if len(parts) > 0 else None
    state = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None
    return city, state, country


class NuforcImporter(Importer):
    source_name = "NUFORC"

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "nuforc.csv")

    def parse_row(self, raw):
        # Location
        raw_loc = _safe_str(raw.get("Location", "")).strip()
        city, state, country = parse_nuforc_location(raw_loc)

        location = {
            "raw_text": raw_loc or None,
            "city": city,
            "state": state,
            "country": country,
        }

        # Dates
        date_event, date_raw = parse_nuforc_date(_safe_str(raw.get("Occurred", "")))
        date_reported, _ = parse_nuforc_date(_safe_str(raw.get("Reported", "")))
        date_posted = _safe_str(raw.get("Posted", "")).strip() or None

        # Build raw_json from all non-empty fields
        raw_json = {}
        for k, v in raw.items():
            s = _safe_str(v).strip()
            if s:
                raw_json[k] = s

        sighting = {
            "source_record_id": _safe_str(raw.get("No", "")).strip() or None,
            "date_event": date_event,
            "date_event_raw": date_raw,
            "date_reported": date_reported,
            "date_posted": date_posted,
            "description": _safe_str(raw.get("Description", "")).strip() or None,
            "shape": _safe_str(raw.get("Shape", "")).strip() or None,
            "color": _safe_str(raw.get("Color", "")).strip() or None,
            "size_estimated": _safe_str(raw.get("Estimated Size", "")).strip() or None,
            "duration": _safe_str(raw.get("Duration", "")).strip() or None,
            "num_witnesses": _safe_int(raw.get("No of observers")),
            "direction": _safe_str(
                raw.get(" Direction from Viewer", "") or raw.get("Direction from Viewer", "")
            ).strip() or None,
            "elevation_angle": _safe_str(
                raw.get(" Angle of Elevation", "") or raw.get("Angle of Elevation", "")
            ).strip() or None,
            "viewed_from": _safe_str(
                raw.get(" Viewed from", "") or raw.get("Viewed from", "")
            ).strip() or None,
            "explanation": _safe_str(raw.get("Explanation", "")).strip() or None,
            "characteristics": _safe_str(raw.get("Characteristics", "")).strip() or None,
            "notes": _safe_str(raw.get("note", "")).strip() or None,
            "raw_json": json.dumps(raw_json, ensure_ascii=False),
        }

        return location, sighting
