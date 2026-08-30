"""
UPDB importer — PhenomAInon Unified Phenomena Database.

~1.9M rows, 9 columns. UPDB is an aggregator: its `name` column records
which body originally reported each case.

Rows are skipped only when we import that origin directly from its own,
richer dataset — currently NUFORC alone. Everything else is retained and
labelled with its origin, so a case that reached us only through UPDB still
counts.

v0.16.3 — MUFON is no longer skipped. It was, on the rationale that
mufon.csv gave us a richer copy; that import was retired in v0.16, so the
skip had quietly turned from "deduplicate" into "delete". The skip set is
now derived from DIRECTLY_IMPORTED_ORIGINS rather than hardcoded here, so
retiring an importer can't silently strip that source from the aggregators
as well.
"""

import json
import os
import re

from ufosint.config import Config
from ufosint.importers.base import DIRECTLY_IMPORTED_ORIGINS, Importer

# Aliases UPDB uses for each origin, flattened for substring matching. Derived
# from DIRECTLY_IMPORTED_ORIGINS — do not hardcode; see that constant.
SKIP_NAMES = {
    alias for aliases in DIRECTLY_IMPORTED_ORIGINS.values() for alias in aliases
}

# Origin names UPDB reports, mapped to the canonical source_origin name. Used
# to label retained rows so "MUFON via UPDB" stays distinguishable from the
# retired mufon.csv import, which is the distinction the v0.16 purge keyed on.
ORIGIN_ALIASES = {
    "MUFON": ("MUFON", "Mutual UFO Network"),
    "NUFORC": ("NUFORC", "National UFO Reporting Center"),
    "UFODNA": ("UFODNA",),
    "BLUEBOOK": ("BLUEBOOK", "Blue Book", "Project Blue Book"),
    "NICAP": ("NICAP",),
    "BAASS": ("BAASS",),
    "NIDS": ("NIDS",),
    "SKINWALKER": ("SKINWALKER", "Skinwalker"),
    "PILOTS": ("PILOTS",),
    "BRAZILGOV": ("BRAZILGOV",),
    "CANADAGOV": ("CANADAGOV",),
    "UKTNA": ("UKTNA",),
}


def canonical_origin(name):
    """Map a raw UPDB `name` value to a canonical source_origin name.

    Matching is substring and case-insensitive because the column mixes plain
    source names with case identifiers ("N-971"). Longest alias first so
    "National UFO Reporting Center" cannot be shadowed by a shorter match.
    """
    if not name:
        return None
    hay = name.strip().lower()
    best = None
    for canon, aliases in ORIGIN_ALIASES.items():
        for alias in aliases:
            if alias.lower() in hay:
                if best is None or len(alias) > best[1]:
                    best = (canon, len(alias))
    return best[0] if best else None


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
        if not name:
            return False
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

        origin_raw = (raw.get("name", "") or "").strip() or None

        sighting = {
            "source_record_id": (raw.get("case_number", "") or "").strip() or None,
            "origin_record_id": origin_raw,
            # Resolved to a source_origin FK by Importer._flush_batch. Always
            # present, even when None, so every dict in a batch shares a key
            # set and the generated INSERT column list stays stable.
            "origin_name": canonical_origin(origin_raw),
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
