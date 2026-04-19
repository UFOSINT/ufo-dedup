"""
UPDB importer — PhenomAInon Unified Phenomena Database.

~1.9M rows, 9 columns. Skips rows whose 'name' field is MUFON or NUFORC
(1.82M rows) since we import those from their richer original sources.
Retains ~65K from UFODNA, Blue Book, NICAP, and other aggregators.
"""

import json
import os
import re

from ufosint.config import Config
from ufosint.importers.base import Importer

# Sources to skip (already imported from richer originals)
SKIP_NAMES = {
    "MUFON", "NUFORC", "National UFO Reporting Center",
    "Mutual UFO Network",
}


def parse_updb_date(date_str):
    """Parse UPDB date into ISO. Formats vary widely."""
    if not date_str or not date_str.strip():
        return None, None
    raw = date_str.strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return m.group(0), raw
    m = re.match(r"(\d{4})", raw)
    if m:
        return m.group(1), raw
    return None, raw


class UpdbImporter(Importer):
    source_name = "UPDB"
    batch_size = 10000

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "UPDB.app", "phenomenAInon_UPDB.csv")

    def should_skip_row(self, raw):
        name = (raw.get("name", "") or "").strip()
        return any(skip.lower() in name.lower() for skip in SKIP_NAMES)

    def parse_row(self, raw):
        raw_loc = (raw.get("location", "") or "").strip()
        parts = [p.strip() for p in raw_loc.replace("\\,", ",").split(",")]
        city = parts[0] if len(parts) > 0 else None
        state = parts[1] if len(parts) > 1 else None
        country = parts[2] if len(parts) > 2 else None

        location = {
            "raw_text": raw_loc or None,
            "city": city,
            "state": state,
            "country": country,
        }

        date_event, date_raw = parse_updb_date(raw.get("date", ""))

        short = (raw.get("short_desc", "") or "").strip()
        long = (raw.get("long_desc", "") or "").strip()

        sighting = {
            "source_record_id": (raw.get("case_number", "") or "").strip() or None,
            "origin_record_id": (raw.get("name", "") or "").strip() or None,
            "date_event": date_event,
            "date_event_raw": date_raw,
            "summary": short or None,
            "description": long or short or None,
            "raw_json": json.dumps(
                {k: v for k, v in raw.items() if v and str(v).strip()},
                ensure_ascii=False,
            ),
        }

        return location, sighting
