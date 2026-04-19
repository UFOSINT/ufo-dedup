"""
MUFON importer — Mutual UFO Network.

~138K rows from mufon.csv. 7 columns. Date format: 'YYYY-MM-DD\\n5:45AM'.
Locations use escaped commas: 'Newscandia\\, MN\\, US'.
"""

import json
import os
import re

from ufosint.config import Config
from ufosint.importers.base import Importer


def parse_mufon_date(date_str):
    """Parse MUFON date like '1992-08-19\\n5:45AM' into (ISO, raw)."""
    if not date_str or not date_str.strip():
        return None, None

    parts = date_str.strip().split("\n")
    date_part = parts[0].strip() if parts else None
    time_part = parts[1].strip() if len(parts) > 1 else None

    if date_part and re.match(r"\d{4}-\d{2}-\d{2}", date_part):
        iso = date_part
        if time_part:
            t = time_part.upper().strip()
            m = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", t)
            if m:
                h, mi, ampm = int(m.group(1)), m.group(2), m.group(3)
                if ampm == "PM" and h != 12:
                    h += 12
                elif ampm == "AM" and h == 12:
                    h = 0
                iso += f"T{h:02d}:{mi}"
        return iso, date_str.strip()

    return None, date_str.strip()


def parse_mufon_location(loc_str):
    """Parse 'Newscandia\\, MN\\, US' into (city, state, country)."""
    if not loc_str:
        return None, None, None
    loc = loc_str.replace("\\,", ",").strip()
    parts = [p.strip() for p in loc.split(",")]
    city = parts[0] if len(parts) > 0 else None
    state = parts[1] if len(parts) > 1 else None
    country = parts[2] if len(parts) > 2 else None
    return city, state, country


class MufonImporter(Importer):
    source_name = "MUFON"

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "mufon.csv")

    def parse_row(self, raw):
        raw_loc = (raw.get("Location of Event", "") or "").strip()
        city, state, country = parse_mufon_location(raw_loc)

        location = {
            "raw_text": raw_loc or None,
            "city": city,
            "state": state,
            "country": country,
        }

        date_event, date_raw = parse_mufon_date(raw.get("Date of Event", ""))

        # MUFON has short_desc + long_desc
        short = (raw.get("Short Description", "") or "").strip()
        long = (raw.get("Long Description", "") or "").strip()
        description = long or short or None
        summary = short if long else None

        sighting = {
            "source_record_id": (raw.get("Case Number", "") or "").strip() or None,
            "date_event": date_event,
            "date_event_raw": date_raw,
            "summary": summary,
            "description": description,
            "raw_json": json.dumps(
                {k: v for k, v in raw.items() if v and str(v).strip()},
                ensure_ascii=False,
            ),
        }

        return location, sighting
