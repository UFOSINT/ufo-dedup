"""
LLM-powered data quality audit pipeline for the unified UFO sightings database.

Three audit tiers:
  Tier A: Geocoding verification — detect coords that don't match stated city/state
  Tier B: Location normalization — LLM cleans unparseable location strings for re-geocoding
  Tier C: Data mining — extract missing structured fields from rich narrative text

Each tier is batch-capable, resume-safe, and stores results back into audit_* columns
on the sighting table. Batches are tracked in the audit_batch table.

Usage:
    python audit.py --tier a --batch-size 500         # geocode verification (no LLM, pure SQL/code)
    python audit.py --tier b --batch-size 200 --limit 1000   # location normalization (LLM)
    python audit.py --tier c --batch-size 100 --limit 500    # data mining (LLM)
    python audit.py --fix-geocodes                    # apply Tier A fixes (swap wrong-country coords)
    python audit.py --stats                           # print audit status overview
    python audit.py --preview --tier b --limit 10     # dry-run: show what would be sent to LLM

Environment:
    OPENROUTER_API_KEY  — required for Tiers B and C
    AUDIT_MODEL         — override default model (default: google/gemini-2.0-flash-001)

Dependencies: requests (for OpenRouter API calls). No torch, no transformers.
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint audit
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────


import json
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import radians, sin, cos, sqrt, atan2

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
DEFAULT_MODEL = os.environ.get("AUDIT_MODEL", "google/gemini-2.0-flash-001")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

BATCH_SIZE = 200
MAX_WORKERS = 5  # parallel LLM requests


# ============================================================
# US state bounding boxes (approximate lat/lng ranges)
# Used by Tier A to detect wrong-country geocoding
# ============================================================

US_STATE_BOUNDS = {
    "AL": (30.2, 35.0, -88.5, -84.9), "AK": (51.2, 71.4, -179.1, -129.9),
    "AZ": (31.3, 37.0, -114.8, -109.0), "AR": (33.0, 36.5, -94.6, -89.6),
    "CA": (32.5, 42.0, -124.4, -114.1), "CO": (37.0, 41.0, -109.1, -102.0),
    "CT": (41.0, 42.1, -73.7, -71.8), "DE": (38.5, 39.8, -75.8, -75.0),
    "FL": (24.5, 31.0, -87.6, -80.0), "GA": (30.4, 35.0, -85.6, -80.8),
    "HI": (18.9, 22.2, -160.2, -154.8), "ID": (42.0, 49.0, -117.2, -111.0),
    "IL": (37.0, 42.5, -91.5, -87.5), "IN": (37.8, 41.8, -88.1, -84.8),
    "IA": (40.4, 43.5, -96.6, -90.1), "KS": (37.0, 40.0, -102.1, -94.6),
    "KY": (36.5, 39.1, -89.6, -82.0), "LA": (29.0, 33.0, -94.0, -89.0),
    "ME": (43.1, 47.5, -71.1, -66.9), "MD": (37.9, 39.7, -79.5, -75.0),
    "MA": (41.2, 42.9, -73.5, -69.9), "MI": (41.7, 48.3, -90.4, -82.4),
    "MN": (43.5, 49.4, -97.2, -89.5), "MS": (30.2, 35.0, -91.7, -88.1),
    "MO": (36.0, 40.6, -95.8, -89.1), "MT": (44.4, 49.0, -116.0, -104.0),
    "NE": (40.0, 43.0, -104.1, -95.3), "NV": (35.0, 42.0, -120.0, -114.0),
    "NH": (42.7, 45.3, -72.6, -70.7), "NJ": (38.9, 41.4, -75.6, -73.9),
    "NM": (31.3, 37.0, -109.1, -103.0), "NY": (40.5, 45.0, -79.8, -71.9),
    "NC": (33.8, 36.6, -84.3, -75.5), "ND": (45.9, 49.0, -104.0, -96.6),
    "OH": (38.4, 42.0, -84.8, -80.5), "OK": (33.6, 37.0, -103.0, -94.4),
    "OR": (42.0, 46.3, -124.6, -116.5), "PA": (39.7, 42.3, -80.5, -74.7),
    "RI": (41.1, 42.0, -71.9, -71.1), "SC": (32.0, 35.2, -83.4, -78.5),
    "SD": (42.5, 45.9, -104.1, -96.4), "TN": (35.0, 36.7, -90.3, -81.6),
    "TX": (25.8, 36.5, -106.6, -93.5), "UT": (37.0, 42.0, -114.1, -109.0),
    "VT": (42.7, 45.0, -73.4, -71.5), "VA": (36.5, 39.5, -83.7, -75.2),
    "WA": (45.5, 49.0, -124.8, -116.9), "WV": (37.2, 40.6, -82.6, -77.7),
    "WI": (42.5, 47.1, -92.9, -86.8), "WY": (41.0, 45.0, -111.1, -104.1),
    "DC": (38.8, 39.0, -77.1, -76.9),
}

# Canadian province approximate bounds
CA_PROVINCE_BOUNDS = {
    "AB": (49.0, 60.0, -120.0, -110.0), "BC": (48.3, 60.0, -139.1, -114.0),
    "MB": (49.0, 60.0, -102.0, -88.0), "NB": (45.0, 48.1, -69.1, -63.8),
    "NL": (46.6, 60.4, -67.8, -52.6), "NS": (43.4, 47.0, -66.4, -59.7),
    "ON": (41.7, 56.9, -95.2, -74.3), "PE": (46.0, 47.1, -64.4, -62.0),
    "QC": (45.0, 62.6, -79.8, -57.1), "SK": (49.0, 60.0, -110.0, -101.4),
}


def _haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    la1, lo1, la2, lo2 = map(radians, [lat1, lng1, lat2, lng2])
    dlat, dlon = la2 - la1, lo2 - lo1
    a = sin(dlat/2)**2 + cos(la1)*cos(la2)*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def _coords_in_bounds(lat, lng, bounds):
    """Check if lat/lng falls within (min_lat, max_lat, min_lng, max_lng)."""
    return bounds[0] <= lat <= bounds[1] and bounds[2] <= lng <= bounds[3]


# ============================================================
# TIER A: Geocoding verification (no LLM needed)
# ============================================================

def tier_a_geocode_verify(db_path=DB_PATH, fix=False):
    """
    Detect geocoding errors: coords that don't match the stated state/province.

    Two checks:
    1. State bounds check: if state is a known US/CA code and coords are outside
       that state's bounding box, flag as 'mismatch'.
    2. Wrong-hemisphere check: US/CA locations with southern-hemisphere coords
       (lat < 0) or eastern-hemisphere coords (lng > 0 for US).

    With --fix: swap wrong geocodes to NULL so they can be re-geocoded.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("\n=== TIER A: Geocoding Verification ===\n")

    # Get all sightings with coords + parsed state
    cur.execute("""
        SELECT s.id, l.city, l.state, l.country, s.lat, s.lng, l.raw_text, l.id as loc_id
        FROM sighting s
        JOIN location l ON s.location_id = l.id
        WHERE s.lat IS NOT NULL AND s.lng IS NOT NULL
        AND l.state IS NOT NULL AND LENGTH(l.state) >= 2
    """)
    rows = cur.fetchall()
    print(f"  Rows with coords + state: {len(rows):,}")

    mismatches = []
    wrong_hemisphere = []
    checked = 0

    for sid, city, state, country, lat, lng, raw_text, loc_id in rows:
        state_upper = (state or "").upper().strip()
        country_upper = (country or "").upper().strip()

        # Determine if this is a US location
        is_us = country_upper in ("US", "USA", "UNITED STATES", "") and state_upper in US_STATE_BOUNDS
        is_ca = country_upper in ("CA", "CAN", "CANADA", "") and state_upper in CA_PROVINCE_BOUNDS

        if is_us:
            bounds = US_STATE_BOUNDS[state_upper]
            # Wrong hemisphere is a dead giveaway
            if lat < 0 or lng > 0:
                wrong_hemisphere.append((sid, city, state, lat, lng, raw_text, loc_id))
            elif not _coords_in_bounds(lat, lng, bounds):
                # Not even close to the right state
                mismatches.append((sid, city, state, lat, lng, raw_text, loc_id, "out_of_state_bounds"))

        elif is_ca:
            bounds = CA_PROVINCE_BOUNDS[state_upper]
            if lat < 0 or lng > 0:
                wrong_hemisphere.append((sid, city, state, lat, lng, raw_text, loc_id))
            elif not _coords_in_bounds(lat, lng, bounds):
                mismatches.append((sid, city, state, lat, lng, raw_text, loc_id, "out_of_province_bounds"))

        checked += 1

    print(f"  Checked: {checked:,}")
    print(f"  Wrong hemisphere (US/CA state but lat<0 or lng>0): {len(wrong_hemisphere):,}")
    print(f"  Out of state/province bounds: {len(mismatches):,}")

    # Show samples
    if wrong_hemisphere:
        print(f"\n  --- Wrong hemisphere (first 20) ---")
        for sid, city, state, lat, lng, raw_text, loc_id in wrong_hemisphere[:20]:
            print(f"    id={sid:>7}: \"{raw_text}\" st={state} -> ({lat:.2f}, {lng:.2f})")

    if mismatches:
        print(f"\n  --- Out of bounds (first 20) ---")
        for sid, city, state, lat, lng, raw_text, loc_id, reason in mismatches[:20]:
            print(f"    id={sid:>7}: \"{raw_text}\" st={state} -> ({lat:.2f}, {lng:.2f})")

    all_bad = [(s[0], s[6]) for s in wrong_hemisphere] + [(s[0], s[6]) for s in mismatches]

    if fix and all_bad:
        print(f"\n  Fixing {len(all_bad):,} bad geocodes...")
        batch_id = _create_batch(conn, "geocode_verify", None, len(all_bad),
                                 {"action": "null_bad_coords"})
        for sid, loc_id in all_bad:
            cur.execute("UPDATE sighting SET lat = NULL, lng = NULL, audit_geocode_check = 'mismatch', audit_batch_id = ? WHERE id = ?", (batch_id, sid))
            cur.execute("UPDATE location SET latitude = NULL, longitude = NULL, geoname_id = NULL, geocode_src = NULL WHERE id = ?", (loc_id,))
        conn.commit()
        _complete_batch(conn, batch_id, {
            "wrong_hemisphere": len(wrong_hemisphere),
            "out_of_bounds": len(mismatches),
            "total_fixed": len(all_bad),
        })
        print(f"  Fixed: {len(all_bad):,} rows — coords set to NULL for re-geocoding.")
        print(f"  Run `python geocode.py` to re-geocode these locations.")
    elif all_bad:
        # Just mark them without fixing
        batch_id = _create_batch(conn, "geocode_verify", None, len(all_bad),
                                 {"action": "flag_only"})
        for sid, _ in all_bad:
            cur.execute("UPDATE sighting SET audit_geocode_check = 'mismatch', audit_batch_id = ? WHERE id = ?", (batch_id, sid))
        conn.commit()
        _complete_batch(conn, batch_id, {
            "wrong_hemisphere": len(wrong_hemisphere),
            "out_of_bounds": len(mismatches),
        })
        print(f"\n  Flagged {len(all_bad):,} rows as audit_geocode_check='mismatch'.")
        print(f"  Re-run with --fix-geocodes to NULL the bad coords and re-geocode.")

    conn.close()
    return len(all_bad)


# ============================================================
# TIER B: Location normalization (LLM-powered)
# ============================================================

TIER_B_SYSTEM_PROMPT = """You are a data cleaning assistant for a UFO sighting database. Your job is to normalize messy location strings into clean structured data.

Given a raw location string (and optionally a description excerpt for context), return a JSON object with:

{
  "city": "clean city name or null",
  "state": "2-letter code for US/CA, full name for other countries, or null",
  "country": "2-letter ISO code (US, CA, GB, AU, etc.) or null",
  "confidence": "high|medium|low",
  "notes": "brief note if ambiguous"
}

Rules:
- For US locations, always use 2-letter state codes (NY, CA, TX, etc.)
- For Canadian locations, use province codes (ON, BC, QC, etc.)
- Convert country names to 2-letter ISO codes
- "Pacific Ocean", "Atlantic", "at sea" -> country=null, city=null, notes="maritime"
- "Undisclosed" or similar -> all null, notes="undisclosed"
- If the raw string is just a state code (e.g. "CA, US"), set city=null and state/country
- Parenthetical qualifiers like "New York City (Brooklyn)" -> city="Brooklyn", state="NY", country="US"
- If the string has escaped commas like "Dixon\\, IA\\, US", parse normally
- Respond with ONLY the JSON object, no markdown, no explanation."""

TIER_B_BATCH_PROMPT = """Normalize these {count} location strings. Return a JSON array of {count} objects, one per input, in the same order.

Each object: {{"city": str|null, "state": str|null, "country": str|null, "confidence": "high|medium|low", "notes": str|null}}

Inputs:
{inputs}

Return ONLY the JSON array."""


def _call_openrouter(messages, model=DEFAULT_MODEL, temperature=0.0):
    """Call OpenRouter API. Returns the response content string."""
    import requests

    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY not set. Export it before running LLM tiers.")

    resp = requests.post(
        OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 4096,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def _parse_json_response(text):
    """Extract JSON from LLM response, handling markdown fences."""
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON array or object in the response
        match = re.search(r'[\[{].*[\]}]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


def _process_tier_b_batch(batch_items, model):
    """Process a single Tier B batch via LLM. Thread-safe (no DB writes).

    Returns list of (raw_text, result_dict_or_None) tuples.
    """
    inputs = "\n".join(
        f"{j+1}. \"{r[0]}\""
        for j, r in enumerate(batch_items)
    )
    messages = [
        {"role": "system", "content": TIER_B_SYSTEM_PROMPT},
        {"role": "user", "content": TIER_B_BATCH_PROMPT.format(
            count=len(batch_items), inputs=inputs
        )},
    ]
    try:
        response = _call_openrouter(messages, model=model)
        results = _parse_json_response(response)
        if not results or not isinstance(results, list):
            return [(r[0], None) for r in batch_items]
        # Pad if needed
        while len(results) < len(batch_items):
            results.append(None)
        return [(batch_items[j][0], results[j]) for j in range(len(batch_items))]
    except Exception as e:
        print(f"\n  ERROR in parallel batch: {e}")
        return [(r[0], None) for r in batch_items]


def tier_b_location_normalize(db_path=DB_PATH, limit=1000, batch_size=BATCH_SIZE,
                               preview=False, model=DEFAULT_MODEL, workers=10):
    """
    LLM-clean failed location strings so they can be re-geocoded.

    Targets: sightings with location text but no coords, grouped by unique
    location string (deduped — many sightings share the same raw_text).
    Uses parallel workers for high throughput (no delay between batches).
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print(f"\n=== TIER B: Location Normalization (LLM) ===\n")
    print(f"  Model: {model}")
    print(f"  Batch size: {batch_size}")
    print(f"  Workers: {workers}")
    print(f"  Limit: {limit}")

    # Get unique failed location strings with their counts
    cur.execute("""
        SELECT l.raw_text, l.city, l.state, l.country, COUNT(*) as n,
               GROUP_CONCAT(DISTINCT l.id) as loc_ids
        FROM location l
        JOIN sighting s ON s.location_id = l.id
        WHERE (s.lat IS NULL OR s.lng IS NULL)
        AND l.raw_text IS NOT NULL AND LENGTH(l.raw_text) > 3
        AND s.audit_location_check IS NULL
        GROUP BY l.raw_text
        ORDER BY n DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()

    if not rows:
        print("  No un-audited failed location strings found.")
        conn.close()
        return 0

    total_sightings = sum(r[4] for r in rows)
    print(f"  Unique location strings to process: {len(rows):,}")
    print(f"  Total sightings affected: {total_sightings:,}")

    if preview:
        print(f"\n  --- Preview (first 30) ---")
        for raw, city, state, country, n, _ in rows[:30]:
            print(f"    \"{raw}\" (city={city}, st={state}, ctry={country}) x{n}")
        conn.close()
        return 0

    batch_id = _create_batch(conn, "location_normalize", model, len(rows),
                             {"limit": limit, "batch_size": batch_size, "workers": workers})

    # Build batches
    batches = []
    for i in range(0, len(rows), batch_size):
        batches.append(rows[i:i + batch_size])

    print(f"  Batches: {len(batches)} (processing {workers} in parallel)\n")

    processed = 0
    fixed = 0
    errors = 0
    t0 = time.time()

    # Fire all batches in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_tier_b_batch, batch, model): (idx, batch)
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            idx, batch = futures[future]
            try:
                results_pairs = future.result()
            except Exception as e:
                print(f"\n  FATAL ERROR batch #{idx+1}: {e}")
                errors += len(batch)
                continue

            # Process results and write to DB (single-threaded DB writes)
            for j, (raw, old_city, old_state, old_country, n, loc_ids_str) in enumerate(batch):
                result = results_pairs[j][1] if j < len(results_pairs) else None
                if result is None:
                    errors += 1
                    continue

                new_city = result.get("city")
                new_state = result.get("state")
                new_country = result.get("country")
                confidence = result.get("confidence", "low")

                changed = (new_city != old_city or new_state != old_state or new_country != old_country)
                has_improvement = new_city or (new_state and new_state != old_state) or (new_country and new_country != old_country)

                loc_ids = [int(x) for x in loc_ids_str.split(",")]

                if changed and has_improvement and confidence in ("high", "medium"):
                    fix_json = json.dumps({
                        "city": new_city, "state": new_state, "country": new_country,
                        "confidence": confidence, "notes": result.get("notes"),
                        "original": {"city": old_city, "state": old_state, "country": old_country},
                    })
                    for lid in loc_ids:
                        cur.execute("""
                            UPDATE location SET city = ?, state = ?, country = ?
                            WHERE id = ? AND (city IS NULL OR city = ?)
                        """, (new_city, new_state, new_country, lid, old_city))
                    cur.execute(f"""
                        UPDATE sighting SET
                            audit_location_check = 'normalized',
                            audit_location_fix = ?,
                            audit_batch_id = ?,
                            audit_model = ?,
                            audit_timestamp = datetime('now')
                        WHERE location_id IN ({','.join('?' * len(loc_ids))})
                        AND audit_location_check IS NULL
                    """, [fix_json, batch_id, model] + loc_ids)
                    fixed += 1
                else:
                    check_val = "no_improvement" if not has_improvement else "low_confidence"
                    cur.execute(f"""
                        UPDATE sighting SET
                            audit_location_check = ?,
                            audit_batch_id = ?,
                            audit_timestamp = datetime('now')
                        WHERE location_id IN ({','.join('?' * len(loc_ids))})
                        AND audit_location_check IS NULL
                    """, [check_val, batch_id] + loc_ids)

                processed += 1

            conn.commit()
            elapsed = time.time() - t0
            rate = processed / elapsed if elapsed > 0 else 0
            sys.stdout.write(
                f"\r  {processed:,} / {len(rows):,} unique strings | "
                f"{fixed:,} improved | {errors:,} errors | "
                f"{rate:.1f}/s | {elapsed:.0f}s elapsed"
            )
            sys.stdout.flush()

    elapsed = time.time() - t0
    print(f"\n\n  Tier B complete ({elapsed:.0f}s):")
    print(f"    Processed: {processed:,} unique location strings")
    print(f"    Improved: {fixed:,}")
    print(f"    Errors: {errors:,}")
    print(f"    Rate: {processed/elapsed:.1f} strings/s")
    print(f"    Run `python geocode.py` to re-geocode the updated locations.")

    _complete_batch(conn, batch_id, {
        "processed": processed, "fixed": fixed, "errors": errors,
        "elapsed_s": round(elapsed, 1), "rate": round(processed/elapsed, 1),
    })
    conn.close()
    return fixed


# ============================================================
# TIER C: Data mining — extract structured fields from text
# ============================================================

TIER_C_SYSTEM_PROMPT = """You are a data analyst for a UFO sighting database. Given a sighting record with its existing structured fields and narrative description, extract any MISSING information that is clearly stated in the text.

Return a JSON object with ONLY the fields you can confidently extract. Omit fields that are already filled or that you can't determine from the text.

Extractable fields:
{
  "date_event": "YYYY-MM-DD if clearly stated and different from existing",
  "time_of_day": "HH:MM in 24h format",
  "duration_seconds": integer,
  "num_witnesses": integer,
  "num_objects": integer,
  "shape": "one word: sphere, triangle, disc, cigar, oval, light, etc.",
  "color": "primary color observed",
  "sound": "description of sound or 'silent'",
  "direction": "compass direction of travel (N, NE, E, etc.)",
  "elevation_angle": "low, medium, high, overhead",
  "movement_description": "brief movement pattern",
  "weather_conditions": "clear, cloudy, rain, etc.",
  "location_details": "any location info not captured in city/state",
  "location_check": "match|mismatch|ambiguous — does the text match the recorded location?",
  "location_correction": {"city": "...", "state": "...", "country": "..."} if mismatched,
  "data_quality": "high|medium|low — overall assessment of this record's reliability",
  "quality_notes": "brief note on data quality issues or notable details"
}

Rules:
- Only include fields where the text clearly provides the information
- If the existing value matches the text, do NOT include that field
- For location_check: compare the description's location references against the recorded city/state
- Be conservative — "low" confidence extractions are worse than no extraction
- Return ONLY the JSON object"""

TIER_C_ROW_TEMPLATE = """Record #{num}:
  Existing: date={date}, city={city}, state={state}, shape={shape}, duration={duration}, witnesses={witnesses}
  Description: {description}
---"""


def tier_c_data_mine(db_path=DB_PATH, limit=500, batch_size=20,
                     preview=False, model=DEFAULT_MODEL, min_desc_len=200,
                     max_quality_score=40):
    """
    LLM-extract missing structured fields from rich narrative descriptions.

    Targets: sightings with long descriptions but low quality scores
    (lots of text, few structured fields filled in).
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print(f"\n=== TIER C: Data Mining (LLM) ===\n")
    print(f"  Model: {model}")
    print(f"  Batch size: {batch_size}")
    print(f"  Limit: {limit}")
    print(f"  Min description length: {min_desc_len}")
    print(f"  Max quality score: {max_quality_score}")

    # Get rich-text, low-quality sightings that haven't been audited
    cur.execute("""
        SELECT s.id, s.date_event, l.city, l.state, l.country, s.shape,
               s.duration, s.num_witnesses, s.quality_score,
               SUBSTR(s.description, 1, 1500) as desc_excerpt,
               s.color, s.num_objects, s.sound, s.direction, s.duration_seconds
        FROM sighting s
        LEFT JOIN location l ON s.location_id = l.id
        WHERE LENGTH(s.description) > ?
        AND (s.quality_score IS NULL OR s.quality_score <= ?)
        AND s.audit_status IS NULL
        ORDER BY LENGTH(s.description) DESC
        LIMIT ?
    """, (min_desc_len, max_quality_score, limit))
    rows = cur.fetchall()

    if not rows:
        print("  No un-audited rows matching criteria.")
        conn.close()
        return 0

    print(f"  Rows to process: {len(rows):,}")

    if preview:
        print(f"\n  --- Preview (first 5) ---")
        for r in rows[:5]:
            desc_preview = (r[9] or "")[:100].encode("ascii", errors="replace").decode("ascii")
            print(f"    id={r[0]:>7}: QS={r[8]}, city={r[3]}, shape={r[5]}, desc={desc_preview}...")
        conn.close()
        return 0

    batch_id = _create_batch(conn, "data_mine", model, len(rows),
                             {"limit": limit, "min_desc_len": min_desc_len,
                              "max_quality_score": max_quality_score})

    processed = 0
    enriched = 0
    location_mismatches = 0
    errors = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]

        # Build batch prompt
        records = []
        for j, r in enumerate(batch):
            records.append(TIER_C_ROW_TEMPLATE.format(
                num=j+1,
                date=r[1] or "unknown",
                city=r[3] or "unknown",
                state=r[4] or "unknown",
                shape=r[5] or "unknown",
                duration=r[6] or "unknown",
                witnesses=r[7] or "unknown",
                description=(r[9] or "")[:1200],
            ))

        user_msg = (
            f"Analyze these {len(batch)} sighting records. For each, extract any missing "
            f"structured data from the description text. Return a JSON array of {len(batch)} objects.\n\n"
            + "\n".join(records)
            + "\n\nReturn ONLY the JSON array of extraction results."
        )

        messages = [
            {"role": "system", "content": TIER_C_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]

        try:
            response = _call_openrouter(messages, model=model)
            results = _parse_json_response(response)

            if not results or not isinstance(results, list):
                print(f"\n  WARN: batch {i//batch_size + 1} returned unparseable response")
                errors += len(batch)
                continue

            if len(results) < len(batch):
                results.extend([None] * (len(batch) - len(results)))

            for j, row in enumerate(batch):
                sid = row[0]
                if j >= len(results) or results[j] is None:
                    cur.execute("""
                        UPDATE sighting SET audit_status = 'error', audit_batch_id = ?,
                        audit_timestamp = datetime('now') WHERE id = ?
                    """, (batch_id, sid))
                    errors += 1
                    continue

                extraction = results[j]

                # Check location
                loc_check = extraction.pop("location_check", None)
                loc_correction = extraction.pop("location_correction", None)
                quality_notes = extraction.pop("quality_notes", None)
                data_quality = extraction.pop("data_quality", None)

                # Store extraction results
                extracted_fields = {}
                updatable_fields = {
                    "shape": "shape", "color": "color", "sound": "sound",
                    "direction": "direction", "num_witnesses": "num_witnesses",
                    "num_objects": "num_objects", "duration_seconds": "duration_seconds",
                }

                field_updates = []
                for ext_key, db_col in updatable_fields.items():
                    if ext_key in extraction and extraction[ext_key]:
                        # Only fill if currently empty
                        col_idx = {"shape": 5, "color": 10, "sound": 12,
                                   "direction": 13, "num_witnesses": 7,
                                   "num_objects": 11, "duration_seconds": 14}
                        current_val = row[col_idx[ext_key]] if ext_key in col_idx else None
                        if current_val is None or current_val == "":
                            extracted_fields[ext_key] = extraction[ext_key]

                # Build the audit update
                audit_loc_check = loc_check or "no_text"
                if loc_check == "mismatch":
                    location_mismatches += 1

                audit_data = json.dumps(extracted_fields) if extracted_fields else None
                loc_fix = json.dumps(loc_correction) if loc_correction else None

                cur.execute("""
                    UPDATE sighting SET
                        audit_status = 'audited',
                        audit_location_check = ?,
                        audit_location_fix = ?,
                        audit_data_extracted = ?,
                        audit_quality_notes = ?,
                        audit_batch_id = ?,
                        audit_model = ?,
                        audit_timestamp = datetime('now')
                    WHERE id = ?
                """, (audit_loc_check, loc_fix, audit_data, quality_notes,
                      batch_id, model, sid))

                if extracted_fields:
                    enriched += 1
                processed += 1

            conn.commit()
            sys.stdout.write(
                f"\r  Processed: {processed:,} / {len(rows):,}, "
                f"enriched: {enriched:,}, loc_mismatches: {location_mismatches:,}"
            )
            sys.stdout.flush()

        except Exception as e:
            print(f"\n  ERROR in batch {i//batch_size + 1}: {e}")
            errors += len(batch)
            continue

        time.sleep(0.5)

    print(f"\n\n  Tier C complete:")
    print(f"    Processed: {processed:,}")
    print(f"    Enriched (new fields extracted): {enriched:,}")
    print(f"    Location mismatches found: {location_mismatches:,}")
    print(f"    Errors: {errors:,}")

    _complete_batch(conn, batch_id, {
        "processed": processed, "enriched": enriched,
        "location_mismatches": location_mismatches, "errors": errors,
    })
    conn.close()
    return enriched


# ============================================================
# Apply audit results back to the DB
# ============================================================

def apply_tier_c_extractions(db_path=DB_PATH, min_confidence="medium"):
    """
    Apply Tier C extracted fields back to the sighting table.

    Only applies fields that are currently NULL on the sighting and
    were extracted with sufficient confidence.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("\n=== Applying Tier C extractions ===\n")

    cur.execute("""
        SELECT id, audit_data_extracted
        FROM sighting
        WHERE audit_data_extracted IS NOT NULL
        AND audit_status = 'audited'
    """)
    rows = cur.fetchall()
    print(f"  Rows with extractions: {len(rows):,}")

    applied = 0
    field_counts = {}

    for sid, extracted_json in rows:
        try:
            extracted = json.loads(extracted_json)
        except (json.JSONDecodeError, TypeError):
            continue

        if not extracted:
            continue

        updates = []
        params = []

        for field, value in extracted.items():
            if field in ("shape", "color", "sound", "direction"):
                updates.append(f"{field} = COALESCE({field}, ?)")
                params.append(str(value))
                field_counts[field] = field_counts.get(field, 0) + 1
            elif field in ("num_witnesses", "num_objects", "duration_seconds"):
                try:
                    v = int(value)
                    updates.append(f"{field} = COALESCE({field}, ?)")
                    params.append(v)
                    field_counts[field] = field_counts.get(field, 0) + 1
                except (ValueError, TypeError):
                    pass

        if updates:
            params.append(sid)
            cur.execute(
                f"UPDATE sighting SET {', '.join(updates)} WHERE id = ?",
                params
            )
            applied += 1

    conn.commit()
    print(f"  Applied extractions to {applied:,} rows")
    print(f"  Fields filled:")
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        print(f"    {field:<20} {count:>6,}")

    conn.close()
    return applied


# ============================================================
# Batch tracking helpers
# ============================================================

def _create_batch(conn, batch_type, model, row_count, config):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO audit_batch (batch_type, model, row_count, config)
        VALUES (?, ?, ?, ?)
    """, (batch_type, model, row_count, json.dumps(config)))
    conn.commit()
    return cur.lastrowid


def _complete_batch(conn, batch_id, summary):
    cur = conn.cursor()
    cur.execute("""
        UPDATE audit_batch SET
            completed_at = datetime('now'),
            status = 'completed',
            summary = ?
        WHERE id = ?
    """, (json.dumps(summary), batch_id))
    conn.commit()


# ============================================================
# Stats
# ============================================================

def print_stats(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("\n=== Audit Pipeline Status ===\n")

    # Overall audit status
    cur.execute("""
        SELECT audit_status, COUNT(*) FROM sighting
        GROUP BY audit_status ORDER BY COUNT(*) DESC
    """)
    print("  Audit status:")
    for status, count in cur.fetchall():
        print(f"    {str(status or 'not_started'):<20} {count:>10,}")

    # Location check results
    cur.execute("""
        SELECT audit_location_check, COUNT(*) FROM sighting
        WHERE audit_location_check IS NOT NULL
        GROUP BY audit_location_check ORDER BY COUNT(*) DESC
    """)
    results = cur.fetchall()
    if results:
        print("\n  Location check results:")
        for check, count in results:
            print(f"    {check:<20} {count:>10,}")

    # Geocode check results
    cur.execute("""
        SELECT audit_geocode_check, COUNT(*) FROM sighting
        WHERE audit_geocode_check IS NOT NULL
        GROUP BY audit_geocode_check ORDER BY COUNT(*) DESC
    """)
    results = cur.fetchall()
    if results:
        print("\n  Geocode check results:")
        for check, count in results:
            print(f"    {check:<20} {count:>10,}")

    # Batches
    cur.execute("""
        SELECT id, batch_type, model, row_count, status,
               started_at, completed_at, summary
        FROM audit_batch ORDER BY id DESC LIMIT 10
    """)
    batches = cur.fetchall()
    if batches:
        print("\n  Recent batches:")
        for b in batches:
            elapsed = ""
            if b[5] and b[6]:
                elapsed = f" ({b[6]})"
            print(f"    #{b[0]:>3}: {b[1]:<25} model={b[2] or 'n/a':<30} rows={b[3]:>6,} {b[4]}{elapsed}")
            if b[7]:
                try:
                    summary = json.loads(b[7])
                    print(f"           {summary}")
                except:
                    pass

    # Potential wins
    print("\n  --- Potential audit targets ---")

    cur.execute("""
        SELECT COUNT(DISTINCT l.raw_text) FROM location l
        JOIN sighting s ON s.location_id = l.id
        WHERE (s.lat IS NULL OR s.lng IS NULL)
        AND l.raw_text IS NOT NULL AND LENGTH(l.raw_text) > 3
        AND s.audit_location_check IS NULL
    """)
    print(f"    Tier B candidates (unique failed locations, un-audited): {cur.fetchone()[0]:>10,}")

    cur.execute("""
        SELECT COUNT(*) FROM sighting
        WHERE LENGTH(description) > 200
        AND (quality_score IS NULL OR quality_score <= 40)
        AND audit_status IS NULL
    """)
    print(f"    Tier C candidates (rich text, low QS, un-audited):      {cur.fetchone()[0]:>10,}")

    conn.close()


# ============================================================
# REPLAY — re-apply cached LLM results without calling the API
# ============================================================

FIXES_CSV_PATH = os.path.join(
    os.path.dirname(__file__), "data", "output", "audit_tier_b_fixes.csv"
)

def replay_tier_b(db_path=DB_PATH, csv_path=FIXES_CSV_PATH):
    """
    Re-apply Tier B location fixes from a cached CSV export.

    On a fresh rebuild the audit columns are empty and location rows
    have original (messy) city/state/country values. This function reads
    the fixes CSV and re-applies the LLM-derived normalizations without
    calling the API again.

    The CSV was produced by the backup export at the end of a live run.
    It maps raw_location -> new city/state/country.
    """
    import csv as csv_mod

    if not os.path.exists(csv_path):
        print(f"  No cached fixes found at {csv_path}")
        print(f"  Run a live Tier B pass first: python run_audit.py")
        return 0

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    print(f"\n=== TIER B REPLAY — applying cached LLM fixes ===\n")

    # Check if already replayed (any audit_location_check values exist)
    cur.execute("SELECT COUNT(*) FROM sighting WHERE audit_location_check IS NOT NULL")
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"  {existing:,} rows already have audit results — skipping replay.")
        print(f"  Use --reset-audit first if you want to replay from scratch.")
        conn.close()
        return 0

    # Read the fixes CSV — deduplicate by raw_location (many sightings per location)
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        fixes = {}
        for row in reader:
            raw = row.get("raw_location", "")
            if raw and raw not in fixes:
                fixes[raw] = {
                    "new_city": row.get("new_city") or None,
                    "new_state": row.get("new_state") or None,
                    "new_country": row.get("new_country") or None,
                    "confidence": row.get("confidence", "high"),
                    "notes": row.get("notes") or None,
                    "orig_city": row.get("original_city") or None,
                    "orig_state": row.get("original_state") or None,
                    "orig_country": row.get("original_country") or None,
                }

    print(f"  Loaded {len(fixes):,} unique location fixes from cache")

    batch_id = _create_batch(conn, "tier_b_replay", "cached", len(fixes),
                             {"source": csv_path})

    applied = 0
    skipped = 0

    for raw_text, fix in fixes.items():
        # Find location rows with this raw_text
        cur.execute("SELECT id FROM location WHERE raw_text = ?", (raw_text,))
        loc_ids = [r[0] for r in cur.fetchall()]
        if not loc_ids:
            skipped += 1
            continue

        # Update location rows
        for lid in loc_ids:
            cur.execute("""
                UPDATE location SET city = ?, state = ?, country = ?
                WHERE id = ?
            """, (fix["new_city"], fix["new_state"], fix["new_country"], lid))

        # Mark sightings
        fix_json = json.dumps({
            "city": fix["new_city"], "state": fix["new_state"],
            "country": fix["new_country"],
            "confidence": fix["confidence"], "notes": fix["notes"],
            "original": {
                "city": fix["orig_city"], "state": fix["orig_state"],
                "country": fix["orig_country"],
            },
        })
        placeholders = ",".join("?" * len(loc_ids))
        cur.execute(f"""
            UPDATE sighting SET
                audit_location_check = 'normalized',
                audit_location_fix = ?,
                audit_batch_id = ?,
                audit_model = 'cached',
                audit_timestamp = datetime('now')
            WHERE location_id IN ({placeholders})
        """, [fix_json, batch_id] + loc_ids)

        applied += 1
        if applied % 5000 == 0:
            conn.commit()
            print(f"\r  Applied: {applied:,} / {len(fixes):,}", end="")

    conn.commit()

    print(f"\n  Replay complete:")
    print(f"    Applied: {applied:,}")
    print(f"    Skipped (no matching location): {skipped:,}")
    print(f"    Run `python geocode.py` to geocode the updated locations.")

    _complete_batch(conn, batch_id, {"applied": applied, "skipped": skipped})
    conn.close()
    return applied


def reset_audit(db_path=DB_PATH):
    """Clear all audit columns so the pipeline can be re-run."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        UPDATE sighting SET
            audit_status = NULL,
            audit_location_check = NULL,
            audit_location_fix = NULL,
            audit_geocode_check = NULL,
            audit_data_extracted = NULL,
            audit_quality_notes = NULL,
            audit_batch_id = NULL,
            audit_model = NULL,
            audit_timestamp = NULL
    """)
    conn.commit()
    print(f"  Reset audit columns on {cur.rowcount:,} rows")
    conn.close()


def run_audit_pipeline(db_path=DB_PATH):
    """
    Full audit pipeline entry point for rebuild_db.py integration.

    1. Tier A — fix bad geocodes (code only, no LLM)
    2. Tier B — replay cached LLM fixes OR skip if no cache
    3. Re-geocode to pick up improved locations

    This does NOT run live LLM inference — that requires run_audit.py
    with an API key. This is the deterministic, reproducible path.
    """
    print("\n  [Audit] Tier A: Geocoding verification...")
    bad = tier_a_geocode_verify(db_path, fix=True)

    print("\n  [Audit] Tier B: Replaying cached location fixes...")
    replay_tier_b(db_path)

    # Re-geocode is handled by the caller (rebuild_db.py step ordering)
    return bad


# ============================================================
# CLI
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="LLM-powered data quality audit pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python audit.py --stats                             # overview
  python audit.py --tier a                            # detect bad geocodes (no LLM)
  python audit.py --fix-geocodes                      # fix bad geocodes (NULL them)
  python audit.py --tier b --limit 500 --preview      # preview location normalization
  python audit.py --tier b --limit 500                # run location normalization
  python audit.py --tier c --limit 100 --preview      # preview data mining
  python audit.py --tier c --limit 100                # run data mining
  python audit.py --apply-extractions                 # apply Tier C results to sighting
  python audit.py --replay                            # replay cached Tier B fixes (no LLM)
  python audit.py --reset-audit                       # clear all audit columns for re-run
  python audit.py --pipeline                          # full deterministic pipeline (Tier A + replay)
        """,
    )
    parser.add_argument("--db", default=DB_PATH, help="Path to ufo_unified.db")
    parser.add_argument("--tier", choices=["a", "b", "c"], help="Which audit tier to run")
    parser.add_argument("--limit", type=int, default=1000, help="Max rows to process")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Rows per LLM call")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model for Tiers B/C")
    parser.add_argument("--workers", type=int, default=10, help="Parallel LLM workers (default: 10)")
    parser.add_argument("--preview", action="store_true", help="Dry run — show what would be processed")
    parser.add_argument("--fix-geocodes", action="store_true", help="Fix Tier A geocode errors (NULL bad coords)")
    parser.add_argument("--apply-extractions", action="store_true", help="Apply Tier C extractions to sighting")
    parser.add_argument("--replay", action="store_true", help="Replay cached Tier B fixes (no LLM needed)")
    parser.add_argument("--reset-audit", action="store_true", help="Clear all audit columns for re-run")
    parser.add_argument("--pipeline", action="store_true", help="Full deterministic audit (Tier A + replay)")
    parser.add_argument("--stats", action="store_true", help="Print audit status overview")
    parser.add_argument("--min-desc-len", type=int, default=200, help="Tier C: min description length")
    parser.add_argument("--max-quality-score", type=int, default=40, help="Tier C: max quality score to target")
    args = parser.parse_args()

    if args.stats:
        print_stats(args.db)
        return

    if args.fix_geocodes:
        tier_a_geocode_verify(args.db, fix=True)
        return

    if args.apply_extractions:
        apply_tier_c_extractions(args.db)
        return

    if args.replay:
        replay_tier_b(args.db)
        return

    if args.reset_audit:
        reset_audit(args.db)
        return

    if args.pipeline:
        run_audit_pipeline(args.db)
        return

    if args.tier == "a":
        tier_a_geocode_verify(args.db, fix=False)
    elif args.tier == "b":
        tier_b_location_normalize(args.db, limit=args.limit, batch_size=args.batch_size,
                                   preview=args.preview, model=args.model,
                                   workers=args.workers)
    elif args.tier == "c":
        tier_c_data_mine(args.db, limit=args.limit, batch_size=args.batch_size,
                         preview=args.preview, model=args.model,
                         min_desc_len=args.min_desc_len,
                         max_quality_score=args.max_quality_score)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
