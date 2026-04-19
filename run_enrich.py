#!/usr/bin/env python3
r"""
  _   _ _____ ___  ____ ___ _   _ _____
 | | | |  ___/ _ \/ ___|_ _| \ | |_   _|
 | | | | |_ | | | \___ \| ||  \| | | |
 | |_| |  _|| |_| |___) | || |\  | | |
  \___/|_|   \___/|____/___|_| \_| |_|

  LLM Field Extraction — Mining structured data from descriptions

  Finds records with narrative text but missing structured fields,
  sends them to Gemini Flash to extract shape, color, duration,
  witnesses, sound, direction, and location verification.

  Results are cached to CSV for replay on future rebuilds.

Usage:
    python run_enrich.py                              # default: 5000 records
    python run_enrich.py --limit 50000 --workers 15   # bigger run
    python run_enrich.py --apply                      # apply cached results to DB
    python run_enrich.py --stats                      # show extraction coverage
"""

import csv
import json
import os
import re
import sqlite3
import sys
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "output")
EXTRACT_CSV = os.path.join(OUTPUT_DIR, "llm_field_extractions.csv")
LOG_FILE = os.path.join(os.path.dirname(__file__), "enrich_extract.log")

DEFAULT_MODEL = os.environ.get("AUDIT_MODEL", "google/gemini-2.0-flash-001")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


# ============================================================
# ANSI
# ============================================================
class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    CYAN    = "\033[36m"
    RED     = "\033[31m"
    MAGENTA = "\033[35m"
    WHITE   = "\033[97m"


# ============================================================
# LLM prompt
# ============================================================

SYSTEM_PROMPT = """You are a data extraction assistant for a UFO sighting database. Given a sighting record with existing structured fields and a narrative description, extract any structured data that is clearly stated in the text but MISSING from the structured fields.

For each record, return a JSON object with ONLY the fields you can confidently extract. Omit fields that are already filled or not mentioned in the text.

Extractable fields:
{
  "shape": "single word: sphere, triangle, disc, cigar, oval, circle, light, fireball, cylinder, diamond, rectangle, chevron, cross, teardrop, star, egg, cone, cube, saucer, boomerang, flash, formation, crescent, cloud, dome",
  "color": "primary color: red, orange, yellow, green, blue, white, silver, black, etc.",
  "duration_seconds": integer (convert '5 minutes' to 300, '2 hours' to 7200, etc.),
  "num_witnesses": integer,
  "sound": "silent, humming, buzzing, roaring, clicking, pulsing, whooshing, or brief description",
  "direction": "N, NE, E, SE, S, SW, W, NW, or description of travel direction",
  "location_match": "match|mismatch|unclear",
  "location_correction": "city, state" if mismatched (or null),
  "notes": "any notable detail in 1 sentence (optional)"
}

Rules:
- ONLY extract fields where the value is clearly stated in the text
- ONLY extract fields that are currently NULL/missing in the structured data
- For shape: use the canonical list above, pick the closest match
- For color: pick the single most prominent color mentioned
- For duration: convert to integer seconds
- Be conservative: if unsure, omit the field
- Return ONLY the JSON object, no explanation"""

BATCH_PROMPT = """Extract missing structured data from these {count} sighting descriptions. Return a JSON array of {count} objects, one per record, in order. If nothing is extractable for a record, return an empty object {{}}.

{records}

Return ONLY the JSON array."""

RECORD_TEMPLATE = """#{num} (id={id}):
  Existing: shape={shape}, color={color}, duration_s={dur_s}, witnesses={wit}, sound={snd}, direction={dir}
  Location: {city}, {state} | Coords: {lat},{lng}
  Text: {desc}
---"""


# ============================================================
# API
# ============================================================

def _call_openrouter(messages, model=DEFAULT_MODEL):
    import requests
    resp = requests.post(
        OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"model": model, "messages": messages, "temperature": 0.0, "max_tokens": 4096},
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
        m = re.search(r'[\[{].*[\]}]', text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return None


# ============================================================
# Dashboard
# ============================================================

def _bar(pct, width=44):
    filled = int(width * pct)
    return "\u2588" * filled + "\u2591" * (width - filled)

def _eta(secs):
    if secs <= 0: return "done"
    if secs < 60: return f"{secs:.0f}s"
    if secs < 3600: return f"{int(secs//60)}m {int(secs%60)}s"
    return f"{int(secs//3600)}h {int((secs%3600)//60)}m"


class Dashboard:
    def __init__(self, total, workers, logf):
        self.total = total
        self.workers = workers
        self.logf = logf
        self.lock = threading.Lock()
        self.processed = 0
        self.extracted = 0  # records where at least 1 field was extracted
        self.fields_found = Counter()
        self.loc_mismatches = 0
        self.errors = 0
        self.recent = []
        self.t0 = time.time()
        self.last_render = 0
        self.batches_done = 0
        self.batches_total = 0

    def update(self, results_batch):
        with self.lock:
            self.batches_done += 1
            for sid, extraction in results_batch:
                self.processed += 1
                if extraction and any(k not in ("location_match", "location_correction", "notes")
                                      for k in extraction):
                    self.extracted += 1
                    for k in extraction:
                        if k not in ("location_match", "location_correction", "notes"):
                            self.fields_found[k] += 1
                    self.recent.append((sid, extraction))
                    if len(self.recent) > 6:
                        self.recent.pop(0)
                if extraction and extraction.get("location_match") == "mismatch":
                    self.loc_mismatches += 1

    def log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        if self.logf:
            self.logf.write(line + "\n")
            self.logf.flush()

    def render(self):
        now = time.time()
        if now - self.last_render < 0.3:
            return
        self.last_render = now

        with self.lock:
            elapsed = now - self.t0
            pct = self.processed / self.total if self.total > 0 else 0
            rate = self.processed / elapsed if elapsed > 0 else 0
            eta = (self.total - self.processed) / rate if rate > 0 else 0

            lines = []
            lines.append("")
            lines.append(f"  {C.BOLD}{C.CYAN}{'='*62}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.WHITE}  UFOSINT LLM Field Extraction{C.RESET}")
            lines.append(f"  {C.BOLD}{C.CYAN}{'='*62}{C.RESET}")
            lines.append("")
            lines.append(f"  {C.BOLD}Progress:{C.RESET}  {_bar(pct)}  {C.BOLD}{pct*100:5.1f}%{C.RESET}")
            lines.append("")
            lines.append(f"  {C.DIM}Records:{C.RESET}   {C.BOLD}{self.processed:>8,}{C.RESET} / {self.total:,}      {C.DIM}Rate:{C.RESET}    {C.BOLD}{rate:>6.1f}{C.RESET}/s")
            lines.append(f"  {C.GREEN}Enriched:{C.RESET}  {C.BOLD}{C.GREEN}{self.extracted:>8,}{C.RESET}                   {C.DIM}Elapsed:{C.RESET} {C.BOLD}{_eta(elapsed):>6}{C.RESET}")
            lines.append(f"  {C.YELLOW}Loc fix:{C.RESET}   {self.loc_mismatches:>8,}                   {C.DIM}ETA:{C.RESET}     {C.BOLD}{_eta(eta):>6}{C.RESET}")
            lines.append(f"  {C.RED}Errors:{C.RESET}    {self.errors:>8,}                   {C.DIM}Workers:{C.RESET} {C.BOLD}{self.workers:>6}{C.RESET}")
            lines.append("")

            if self.fields_found:
                lines.append(f"  {C.BOLD}{C.MAGENTA}Fields extracted:{C.RESET}")
                for field, n in self.fields_found.most_common(8):
                    pct_f = 100 * n / max(self.processed, 1)
                    lines.append(f"    {field:<18} {C.GREEN}{n:>6,}{C.RESET}  ({pct_f:.0f}%)")
            lines.append("")

            if self.recent:
                lines.append(f"  {C.BOLD}{C.YELLOW}Recent extractions:{C.RESET}")
                for sid, ext in self.recent[-5:]:
                    fields = [f"{k}={str(v)[:20]}" for k, v in ext.items()
                              if k not in ("location_match", "location_correction", "notes") and v]
                    if fields:
                        lines.append(f"    {C.DIM}id={sid:<8}{C.RESET} {C.GREEN}{', '.join(fields[:4])}{C.RESET}")

            lines.append("")
            lines.append(f"  {C.DIM}Log: {LOG_FILE}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.CYAN}{'='*62}{C.RESET}")

            sys.stdout.write("\033[H\033[J" + "\n".join(lines))
            sys.stdout.flush()


# ============================================================
# Core extraction
# ============================================================

def _process_batch(batch_items, model):
    """Process a batch of records through the LLM. Thread-safe."""
    records = []
    for i, r in enumerate(batch_items):
        desc = (r["desc"] or "")[:1000].encode("ascii", errors="replace").decode()
        records.append(RECORD_TEMPLATE.format(
            num=i+1, id=r["id"],
            shape=r["shape"] or "NULL", color=r["color"] or "NULL",
            dur_s=r["duration_seconds"] or "NULL", wit=r["witnesses"] or "NULL",
            snd=r["sound"] or "NULL", dir=r["direction"] or "NULL",
            city=r["city"] or "?", state=r["state"] or "?",
            lat=r["lat"], lng=r["lng"],
            desc=desc,
        ))

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": BATCH_PROMPT.format(
            count=len(batch_items), records="\n".join(records)
        )},
    ]

    try:
        response = _call_openrouter(messages, model=model)
        results = _parse_json(response)
        if results and isinstance(results, list):
            while len(results) < len(batch_items):
                results.append({})
            return [(batch_items[j]["id"], results[j] or {}) for j in range(len(batch_items))]
    except Exception as e:
        return [(r["id"], None) for r in batch_items]

    return [(r["id"], {}) for r in batch_items]


def _load_already_extracted(csv_path):
    """Load sighting IDs already in the CSV to avoid duplicates on resume."""
    seen = set()
    if os.path.exists(csv_path):
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        seen.add(int(row["sighting_id"]))
                    except (ValueError, KeyError):
                        pass
        except Exception:
            pass
    return seen


def run_extraction(limit=5000, batch_size=25, workers=10, model=DEFAULT_MODEL,
                   min_missing=2, min_desc_len=100):
    """Full extraction run with live dashboard. Resume-safe."""
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()

    logf = open(LOG_FILE, "a", encoding="utf-8")
    logf.write(f"\n{'='*60}\n")
    logf.write(f"[{datetime.now():%H:%M:%S}] EXTRACTION START limit={limit} workers={workers} model={model}\n")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check how many already done (for resume display)
    cur.execute("SELECT COUNT(*) FROM sighting WHERE audit_status = 'extracted'")
    already_done = cur.fetchone()[0]

    # Load IDs already in the CSV (belt + suspenders with the DB flag)
    csv_seen = _load_already_extracted(EXTRACT_CSV)

    # Pull candidate records — skip already-extracted
    cur.execute(f"""
        SELECT s.id, s.shape, s.color, s.duration_seconds, s.num_witnesses,
               s.sound, s.direction, l.city, l.state, s.lat, s.lng,
               SUBSTR(s.description, 1, 1200) as desc_excerpt,
               s.source_db_id
        FROM sighting s
        LEFT JOIN location l ON s.location_id = l.id
        WHERE LENGTH(s.description) > ?
        AND (
            (s.shape IS NULL) + (s.color IS NULL) +
            (s.num_witnesses IS NULL) + (s.duration_seconds IS NULL) +
            (s.sound IS NULL) + (s.direction IS NULL)
        ) >= ?
        AND (s.audit_status IS NULL OR s.audit_status != 'extracted')
        ORDER BY RANDOM()
        LIMIT ?
    """, (min_desc_len, min_missing, limit))
    rows = cur.fetchall()

    if not rows:
        print(f"  No un-extracted records found. ({already_done:,} already extracted)")
        conn.close()
        logf.close()
        return

    # Filter out any IDs already in CSV (in case DB flag wasn't committed)
    items = []
    for r in rows:
        if r[0] not in csv_seen:
            items.append({
                "id": r[0], "shape": r[1], "color": r[2],
                "duration_seconds": r[3], "witnesses": r[4],
                "sound": r[5], "direction": r[6],
                "city": r[7], "state": r[8], "lat": r[9], "lng": r[10],
                "desc": r[11], "source_db_id": r[12],
            })

    if not items:
        print(f"  All candidate records already in CSV. ({already_done:,} extracted)")
        conn.close()
        logf.close()
        return

    if already_done > 0:
        logf.write(f"[{datetime.now():%H:%M:%S}] RESUMING: {already_done:,} already done, {len(csv_seen):,} in CSV, {len(items):,} new\n")

    logf.write(f"[{datetime.now():%H:%M:%S}] Loaded {len(items):,} records\n")

    # Build batches
    batches = [items[i:i+batch_size] for i in range(0, len(items), batch_size)]

    # Dashboard
    dash = Dashboard(len(items), workers, logf)
    dash.batches_total = len(batches)

    # Open/create the output CSV (append mode — safe for resume)
    csv_exists = os.path.exists(EXTRACT_CSV) and os.path.getsize(EXTRACT_CSV) > 0
    csvf = open(EXTRACT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.writer(csvf)
    if not csv_exists:
        writer.writerow([
            "sighting_id", "shape", "color", "duration_seconds",
            "num_witnesses", "sound", "direction",
            "location_match", "location_correction", "notes",
            "model", "timestamp",
        ])

    if already_done > 0:
        print(f"\n  {C.YELLOW}Resuming: {already_done:,} previously extracted, {len(items):,} remaining{C.RESET}\n")
        time.sleep(2)

    # Fire all batches
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_batch, batch, model): batch
            for batch in batches
        }

        for future in as_completed(futures):
            batch = futures[future]
            try:
                results = future.result()
            except Exception as e:
                dash.errors += len(batch)
                logf.write(f"[{datetime.now():%H:%M:%S}] BATCH ERROR: {e}\n")
                continue

            batch_results = []
            for sid, extraction in results:
                if extraction is None:
                    dash.errors += 1
                    continue

                batch_results.append((sid, extraction))

                # Write to CSV
                writer.writerow([
                    sid,
                    extraction.get("shape", ""),
                    extraction.get("color", ""),
                    extraction.get("duration_seconds", ""),
                    extraction.get("num_witnesses", ""),
                    extraction.get("sound", ""),
                    extraction.get("direction", ""),
                    extraction.get("location_match", ""),
                    extraction.get("location_correction", ""),
                    extraction.get("notes", ""),
                    model,
                    datetime.now().isoformat(),
                ])

                # Mark as extracted in DB
                cur.execute(
                    "UPDATE sighting SET audit_status = 'extracted', audit_timestamp = datetime('now') WHERE id = ?",
                    (sid,)
                )

                # Log interesting extractions
                fields = [k for k in extraction
                          if k not in ("location_match", "location_correction", "notes") and extraction[k]]
                if fields:
                    logf.write(f"[{datetime.now():%H:%M:%S}] EXTRACT id={sid}: {', '.join(f'{k}={extraction[k]}' for k in fields[:5])}\n")

            conn.commit()
            csvf.flush()
            dash.update(batch_results)
            dash.render()

    csvf.close()
    elapsed = time.time() - dash.t0

    summary = {
        "processed": dash.processed, "extracted": dash.extracted,
        "errors": dash.errors, "loc_mismatches": dash.loc_mismatches,
        "fields": dict(dash.fields_found),
        "elapsed_s": round(elapsed, 1),
    }
    logf.write(f"[{datetime.now():%H:%M:%S}] COMPLETE: {json.dumps(summary)}\n")
    logf.close()

    # Final output below dashboard
    print("\n\n")
    print(f"  {C.BOLD}{C.GREEN}COMPLETE{C.RESET}")
    print(f"  {C.DIM}{'_'*50}{C.RESET}")
    print(f"  Processed:  {dash.processed:>8,}")
    print(f"  Enriched:   {C.GREEN}{dash.extracted:>8,}{C.RESET}")
    print(f"  Loc issues: {dash.loc_mismatches:>8,}")
    print(f"  Errors:     {dash.errors:>8,}")
    print(f"  Elapsed:    {_eta(elapsed):>8}")
    print(f"  Rate:       {dash.processed/elapsed:>8.1f}/s")
    print(f"  {C.DIM}{'_'*50}{C.RESET}")
    print(f"  {C.BOLD}Fields extracted:{C.RESET}")
    for field, n in dash.fields_found.most_common():
        print(f"    {field:<18} {n:>6,}")
    print(f"  {C.DIM}{'_'*50}{C.RESET}")
    print(f"  {C.YELLOW}Results: {EXTRACT_CSV}{C.RESET}")
    print(f"  {C.YELLOW}Apply:   python run_enrich.py --apply{C.RESET}")
    print(f"  {C.DIM}Log:     {LOG_FILE}{C.RESET}")
    print()

    conn.close()


# ============================================================
# Apply cached extractions to DB
# ============================================================

def apply_extractions(db_path=DB_PATH):
    """Apply cached LLM extractions from CSV to the database (non-destructive: NULL fields only)."""
    if not os.path.exists(EXTRACT_CSV):
        print(f"  No cached extractions at {EXTRACT_CSV}")
        print(f"  Run: python run_enrich.py --limit 5000")
        return 0

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    with open(EXTRACT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"\n=== Applying {len(rows):,} cached extractions ===\n")

    applied = 0
    field_counts = Counter()

    for row in rows:
        sid = int(row["sighting_id"])
        updates = []
        params = []

        for field in ("shape", "color", "sound", "direction"):
            val = row.get(field, "").strip()
            if val:
                updates.append(f"{field} = COALESCE({field}, ?)")
                params.append(val)
                field_counts[field] += 1

        for field in ("duration_seconds", "num_witnesses"):
            val = row.get(field, "").strip()
            if val:
                try:
                    v = int(float(val))
                    updates.append(f"{field} = COALESCE({field}, ?)")
                    params.append(v)
                    field_counts[field] += 1
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
    print(f"  Applied to {applied:,} rows (NULL fields only — existing values preserved)")
    print(f"  Fields filled:")
    for field, n in field_counts.most_common():
        print(f"    {field:<20} {n:>6,}")

    conn.close()
    return applied


def print_stats(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print(f"\n=== LLM Extraction Status ===\n")

    cur.execute("SELECT COUNT(*) FROM sighting WHERE audit_status = 'extracted'")
    print(f"  Already extracted:   {cur.fetchone()[0]:>10,}")

    cur.execute("""SELECT COUNT(*) FROM sighting
        WHERE LENGTH(description) > 100
        AND ((shape IS NULL) + (color IS NULL) + (num_witnesses IS NULL) +
             (duration_seconds IS NULL) + (sound IS NULL) + (direction IS NULL)) >= 2
        AND (audit_status IS NULL OR audit_status != 'extracted')""")
    print(f"  Remaining targets:   {cur.fetchone()[0]:>10,}")

    if os.path.exists(EXTRACT_CSV):
        with open(EXTRACT_CSV, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f) - 1  # minus header
        size_mb = os.path.getsize(EXTRACT_CSV) / (1024 * 1024)
        print(f"  Cached CSV:          {line_count:>10,} rows ({size_mb:.1f} MB)")

    conn.close()


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="UFOSINT LLM Field Extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=5000, help="Max records (default: 5000)")
    parser.add_argument("--batch-size", type=int, default=25, help="Records per LLM call")
    parser.add_argument("--workers", type=int, default=10, help="Parallel workers")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--min-missing", type=int, default=2, help="Min missing fields to target")
    parser.add_argument("--apply", action="store_true", help="Apply cached extractions to DB")
    parser.add_argument("--stats", action="store_true", help="Show extraction status")
    args = parser.parse_args()

    if args.stats:
        print_stats()
    elif args.apply:
        apply_extractions()
    else:
        if not OPENROUTER_API_KEY:
            print(f"\n  {C.RED}ERROR: OPENROUTER_API_KEY not set.{C.RESET}")
            sys.exit(1)
        run_extraction(
            limit=args.limit, batch_size=args.batch_size,
            workers=args.workers, model=args.model,
            min_missing=args.min_missing,
        )
