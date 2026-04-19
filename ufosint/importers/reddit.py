"""
Reddit r/UFOs importer.

~3.8K LLM-extracted sighting reports from reddit_sightings_extracted.csv.
Three-pass pipeline: scrape_reddit.py -> extract_reddit.py -> this importer.

Content policy: description stores LLM-generated summary, NOT raw Reddit text.
"""

import json
import os

from ufosint.config import Config
from ufosint.importers.base import Importer

# Value normalization for CHECK constraints
CONFIDENCE_MAP = {
    "high": "high", "medium": "medium", "low": "low",
    "none": None, "": None,
}
ANOMALY_MAP = {
    "anomalous": "anomalous", "likely_prosaic": "prosaic", "prosaic": "prosaic",
    "insufficient_data": "ambiguous", "ambiguous": "ambiguous",
    "none": None, "": None,
}


def _normalize(val, mapping):
    if val is None:
        return None
    return mapping.get(str(val).lower().strip(), None)


def _safe_int(val):
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _bool_int(val):
    if val is None or val == "":
        return None
    s = str(val).lower().strip()
    if s in ("true", "yes", "1"):
        return 1
    if s in ("false", "no", "0"):
        return 0
    return None


class RedditImporter(Importer):
    source_name = "r/UFOs"
    batch_size = 500

    @property
    def file_path(self):
        return os.path.join(
            Config.project_root(), "data", "raw", "reddit",
            "reddit_sightings_extracted.csv",
        )

    def parse_row(self, raw):
        post_id = (raw.get("post_id") or "").strip()
        if not post_id:
            return None, None

        # Location
        city = raw.get("city") or None
        state = raw.get("state") or None
        country = raw.get("country") or None
        lat = _safe_float(raw.get("latitude"))
        lon = _safe_float(raw.get("longitude"))

        raw_text = raw.get("xlsx_location") or ""
        if not raw_text and city:
            parts = [p for p in [city, state, country] if p]
            raw_text = ", ".join(parts)

        location = {
            "raw_text": raw_text or None,
            "city": city,
            "state": state,
            "country": country,
            "latitude": lat,
            "longitude": lon,
        }

        reddit_url = f"https://www.reddit.com/r/UFOs/comments/{post_id}/"

        sighting = {
            "source_record_id": post_id,
            "date_event": raw.get("date_event") or None,
            "date_event_raw": raw.get("xlsx_date") or None,
            "time_raw": raw.get("time_of_day") or None,
            "description": raw.get("description") or None,
            "shape": raw.get("shape") or None,
            "color": raw.get("color") or None,
            "duration": raw.get("duration") or None,
            "duration_seconds": _safe_int(raw.get("duration_seconds")),
            "num_objects": _safe_int(raw.get("num_objects")),
            "num_witnesses": _safe_int(raw.get("num_witnesses")),
            "sound": raw.get("sound") or None,
            "direction": raw.get("direction") or None,
            "elevation_angle": raw.get("elevation") or None,
            "source_ref": reddit_url,
            "has_photo": _bool_int(raw.get("has_photo")),
            "has_video": _bool_int(raw.get("has_video")),
            "reddit_post_id": post_id,
            "reddit_url": reddit_url,
            "llm_confidence": _normalize(raw.get("confidence"), CONFIDENCE_MAP),
            "llm_anomaly_assessment": _normalize(raw.get("anomaly_assessment"), ANOMALY_MAP),
            "llm_prosaic_candidate": raw.get("prosaic_candidate") or None,
            "llm_strangeness_rating": _safe_int(raw.get("strangeness_rating")),
            "llm_model": "google/gemini-2.0-flash-001",
        }

        return location, sighting
