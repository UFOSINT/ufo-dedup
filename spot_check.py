"""
LLM spot-check: grade a random sample of sightings for data quality.

Pulls a stratified random sample (mix of sources, quality tiers), sends
each batch to Gemini Flash, and writes a graded CSV for human review.

The LLM checks:
  - Does the description match the recorded location?
  - Does the description match the recorded shape/color/duration?
  - Are there structured fields extractable from the text that are missing?
  - Overall data quality grade (A/B/C/D/F)
  - Any red flags (hoax indicators, duplicate text, nonsense)

Usage:
    python spot_check.py --count 500 --workers 10
    python spot_check.py --count 100 --preview     # show sample, don't call LLM
"""

import csv
import json
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "output")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "spot_check_results.csv")

DEFAULT_MODEL = os.environ.get("AUDIT_MODEL", "google/gemini-2.0-flash-001")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = """You are a data quality auditor for a UFO sighting database. You will be given sighting records with their structured fields and narrative description. Grade each record on data quality.

For each record, return a JSON object:

{
  "grade": "A|B|C|D|F",
  "location_match": "match|mismatch|ambiguous|no_text",
  "location_correction": "corrected city, state if mismatched, or null",
  "shape_in_text": "shape mentioned in description but not in shape field, or null",
  "color_in_text": "color mentioned in description but not in color field, or null",
  "duration_in_text": "duration mentioned in description but not parsed, or null",
  "witnesses_in_text": number if mentioned in text but not in num_witnesses field, or null,
  "extractable_fields": ["list of field names that could be filled from the text"],
  "red_flags": ["list of quality issues: hoax_indicator, nonsense, duplicate_boilerplate, wrong_date, etc."],
  "quality_notes": "brief free-text assessment (1-2 sentences)"
}

Grading rubric:
  A = Rich, consistent, all fields match the text, high-quality report
  B = Good report, minor gaps (missing color or duration that's in text)
  C = Adequate but thin — few details, or some fields don't match text
  D = Poor — very short, inconsistent, or mostly boilerplate
  F = Junk — nonsense, spam, test data, or clearly fabricated

Be strict but fair. Most reports should land B-C."""

BATCH_PROMPT = """Grade these {count} sighting records. Return a JSON array of {count} objects in the same order.

{records}

Return ONLY the JSON array."""

RECORD_TEMPLATE = """Record #{num} (id={id}, source={source}):
  Date: {date}
  Location: city={city}, state={state}, country={country}, coords=({lat}, {lng})
  Shape: {shape} (standardized: {std_shape})
  Color: {color}
  Duration: {duration} ({duration_seconds}s)
  Witnesses: {witnesses}
  Quality Score: {qs}
  Description: {desc}
---"""


def _call_openrouter(messages, model=DEFAULT_MODEL):
    import requests
    resp = requests.post(
        OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"model": model, "messages": messages, "temperature": 0.0, "max_tokens": 8192},
        timeout=90,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _parse_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'[\[{].*[\]}]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


def pull_sample(db_path, count=500):
    """Pull a stratified random sample across sources and quality tiers."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Stratify: proportional by source, with minimum 10 per source
    cur.execute("""
        SELECT sd.name, sd.id, COUNT(*) FROM sighting s
        JOIN source_database sd ON s.source_db_id = sd.id
        WHERE s.description IS NOT NULL AND LENGTH(s.description) > 50
        GROUP BY sd.id
    """)
    sources = cur.fetchall()
    total_with_desc = sum(r[2] for r in sources)

    sample = []
    for src_name, src_id, src_count in sources:
        # Proportional allocation, minimum 10
        n = max(10, int(count * src_count / total_with_desc))
        cur.execute("""
            SELECT s.id, ? as source, s.date_event,
                   l.city, l.state, l.country, s.lat, s.lng,
                   s.shape, s.standardized_shape, s.color,
                   s.duration, s.duration_seconds, s.num_witnesses,
                   s.quality_score,
                   SUBSTR(s.description, 1, 1200)
            FROM sighting s
            LEFT JOIN location l ON s.location_id = l.id
            WHERE s.source_db_id = ?
            AND s.description IS NOT NULL AND LENGTH(s.description) > 50
            ORDER BY RANDOM()
            LIMIT ?
        """, (src_name, src_id, n))
        sample.extend(cur.fetchall())

    conn.close()

    # Shuffle and trim to exact count
    import random
    random.shuffle(sample)
    return sample[:count]


def process_batch(batch, model):
    """Send a batch to the LLM and return parsed results."""
    records = []
    for i, r in enumerate(batch):
        desc = (r[15] or "")[:1000].encode("ascii", errors="replace").decode()
        records.append(RECORD_TEMPLATE.format(
            num=i+1, id=r[0], source=r[1], date=r[2] or "unknown",
            city=r[3] or "unknown", state=r[4] or "unknown",
            country=r[5] or "unknown", lat=r[6], lng=r[7],
            shape=r[8] or "none", std_shape=r[9] or "none",
            color=r[10] or "none", duration=r[11] or "none",
            duration_seconds=r[12], witnesses=r[13],
            qs=r[14], desc=desc,
        ))

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": BATCH_PROMPT.format(
            count=len(batch), records="\n".join(records)
        )},
    ]

    try:
        response = _call_openrouter(messages, model=model)
        results = _parse_json(response)
        if results and isinstance(results, list):
            while len(results) < len(batch):
                results.append(None)
            return results
    except Exception as e:
        print(f"\n  ERROR: {e}")

    return [None] * len(batch)


def run_spot_check(count=500, batch_size=20, workers=10, model=DEFAULT_MODEL,
                   preview=False, db_path=DB_PATH):
    """Run the full spot-check pipeline."""
    print(f"\n{'='*60}")
    print(f"  UFOSINT Spot Check — {count} record sample")
    print(f"{'='*60}\n")

    sample = pull_sample(db_path, count)
    print(f"  Sample pulled: {len(sample)} records")
    print(f"  Model: {model}")
    print(f"  Workers: {workers}")

    # Source distribution
    from collections import Counter
    src_dist = Counter(r[1] for r in sample)
    print(f"  Source distribution:")
    for src, n in src_dist.most_common():
        print(f"    {src:<14} {n:>4}")

    if preview:
        print(f"\n  --- Preview (first 5) ---")
        for r in sample[:5]:
            desc = (r[15] or "")[:100].encode("ascii", "replace").decode()
            print(f"    id={r[0]:>7} {r[1]:<10} QS={r[14]:>3} {r[3] or '?'}, {r[4] or '?'}")
            print(f"      {desc}...")
        return

    # Process in batches with parallel workers
    batches = [sample[i:i+batch_size] for i in range(0, len(sample), batch_size)]
    print(f"  Batches: {len(batches)} (x{batch_size})\n")

    all_results = [None] * len(sample)
    processed = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_batch, batch, model): (idx, batch)
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            idx, batch = futures[future]
            results = future.result()
            start_i = idx * batch_size
            for j, result in enumerate(results):
                if start_i + j < len(all_results):
                    all_results[start_i + j] = result
            processed += len(batch)
            elapsed = time.time() - t0
            sys.stdout.write(f"\r  {processed:>4} / {len(sample)} graded ({elapsed:.0f}s)")
            sys.stdout.flush()

    # Write results CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "sighting_id", "source", "date", "city", "state", "country",
            "lat", "lng", "shape", "std_shape", "color", "duration",
            "duration_seconds", "witnesses", "quality_score",
            "llm_grade", "location_match", "location_correction",
            "shape_in_text", "color_in_text", "duration_in_text",
            "witnesses_in_text", "extractable_fields", "red_flags",
            "quality_notes",
        ])
        for i, r in enumerate(sample):
            result = all_results[i] or {}
            w.writerow([
                r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
                r[8], r[9], r[10], r[11], r[12], r[13], r[14],
                result.get("grade", ""),
                result.get("location_match", ""),
                result.get("location_correction", ""),
                result.get("shape_in_text", ""),
                result.get("color_in_text", ""),
                result.get("duration_in_text", ""),
                result.get("witnesses_in_text", ""),
                json.dumps(result.get("extractable_fields", [])),
                json.dumps(result.get("red_flags", [])),
                result.get("quality_notes", ""),
            ])

    elapsed = time.time() - t0
    print(f"\n\n  Results written to: {OUTPUT_CSV}")

    # Print grade distribution
    grades = Counter(
        (all_results[i] or {}).get("grade", "?")
        for i in range(len(sample))
    )
    print(f"\n  Grade distribution:")
    for grade in ["A", "B", "C", "D", "F", "?"]:
        n = grades.get(grade, 0)
        bar = "#" * (n * 40 // max(len(sample), 1))
        print(f"    {grade}: {n:>4} ({100*n/len(sample):>5.1f}%)  {bar}")

    # Location mismatches
    loc_issues = sum(
        1 for r in all_results
        if r and r.get("location_match") == "mismatch"
    )
    print(f"\n  Location mismatches: {loc_issues}")

    # Extractable field opportunities
    all_extractable = []
    for r in all_results:
        if r and r.get("extractable_fields"):
            all_extractable.extend(r["extractable_fields"])
    ext_dist = Counter(all_extractable)
    if ext_dist:
        print(f"\n  Top extractable fields (from {len(sample)} records):")
        for field, n in ext_dist.most_common(10):
            print(f"    {field:<20} {n:>4} ({100*n/len(sample):.0f}%)")

    # Red flags
    all_flags = []
    for r in all_results:
        if r and r.get("red_flags"):
            all_flags.extend(r["red_flags"])
    flag_dist = Counter(all_flags)
    if flag_dist:
        print(f"\n  Red flags found:")
        for flag, n in flag_dist.most_common(10):
            print(f"    {flag:<30} {n:>4}")

    print(f"\n  Total time: {elapsed:.0f}s")
    print(f"  Open {OUTPUT_CSV} to review individual records")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="LLM spot-check random sample")
    parser.add_argument("--count", type=int, default=500, help="Sample size (default: 500)")
    parser.add_argument("--batch-size", type=int, default=20, help="Records per LLM call")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--preview", action="store_true", help="Show sample without calling LLM")
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    if not args.preview and not OPENROUTER_API_KEY:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    run_spot_check(
        count=args.count, batch_size=args.batch_size,
        workers=args.workers, model=args.model,
        preview=args.preview, db_path=args.db,
    )
