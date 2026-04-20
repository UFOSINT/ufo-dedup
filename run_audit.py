#!/usr/bin/env python3
"""
  _______ _________ ______   _______  ______  _________
 (  ___  )\__   __/(  __  \ (  ___  )(  __  \ \__   __/
 | (   ) |   ) (   | (  \  )| (   ) || (  \  )   ) (
 | |   | |   | |   | |   ) || |   | || |   ) |   | |
 | |   | |   | |   | |   | || |   | || |   | |   | |
 | |   | |   | |   | |   ) || |   | || |   ) |   | |
 | (___) |___) (___| (__/  )| (___) || (__/  )___) (___
 (_______)\_______/(______/ (_______)(______/ \_______/
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint audit b --workers 15
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

  UFO Sighting Database — LLM Audit Pipeline
  Location normalization via Gemini Flash on OpenRouter

Usage:
    python run_audit.py                          # run all (default 120K, 15 workers)
    python run_audit.py --limit 1000             # first 1000 only
    python run_audit.py --workers 20             # more parallel
    python run_audit.py --resume                 # skip already-audited strings
"""

import json
import os
import sqlite3
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# --- Import audit module ---
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import audit

DB_PATH = audit.DB_PATH
LOG_FILE = os.path.join(os.path.dirname(__file__), "audit_tier_b.log")

# ============================================================
# ANSI colors
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
    BG_BLUE = "\033[44m"
    UP      = "\033[A"
    CLEAR   = "\033[2K"


def log(msg, logf=None):
    """Write to log file (always) and return the message."""
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    if logf:
        logf.write(line + "\n")
        logf.flush()
    return line


def format_bar(pct, width=40):
    """Render a progress bar."""
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    return bar


def format_duration(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def format_eta(seconds):
    if seconds <= 0:
        return "done"
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m {s}s"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


# ============================================================
# Live dashboard
# ============================================================

class Dashboard:
    def __init__(self, total_strings, total_sightings, workers, logf):
        self.total_strings = total_strings
        self.total_sightings = total_sightings
        self.workers = workers
        self.logf = logf
        self.lock = threading.Lock()

        # Counters
        self.processed = 0
        self.improved = 0
        self.no_change = 0
        self.errors = 0
        self.sightings_touched = 0
        self.batches_done = 0
        self.batches_total = 0

        # Recent fixes (ring buffer)
        self.recent_fixes = []
        self.max_recent = 8

        # Timing
        self.t0 = time.time()
        self.last_render = 0

        # Active workers
        self.active_workers = 0

    def update(self, batch_results):
        """Called after each batch completes. batch_results is a list of dicts."""
        with self.lock:
            self.batches_done += 1
            for r in batch_results:
                self.processed += 1
                if r["status"] == "improved":
                    self.improved += 1
                    self.sightings_touched += r.get("sightings", 0)
                    self.recent_fixes.append(r)
                    if len(self.recent_fixes) > self.max_recent:
                        self.recent_fixes.pop(0)
                    log(f"FIX: \"{r['original']}\" -> city={r.get('city')}, st={r.get('state')}, ctry={r.get('country')} (x{r.get('sightings',0)})", self.logf)
                elif r["status"] == "no_change":
                    self.no_change += 1
                elif r["status"] == "error":
                    self.errors += 1

    def render(self):
        """Render the dashboard to stdout."""
        now = time.time()
        if now - self.last_render < 0.3:
            return  # throttle
        self.last_render = now

        with self.lock:
            elapsed = now - self.t0
            pct = self.processed / self.total_strings if self.total_strings > 0 else 0
            rate = self.processed / elapsed if elapsed > 0 else 0
            eta = (self.total_strings - self.processed) / rate if rate > 0 else 0

            lines = []
            lines.append("")
            lines.append(f"  {C.BOLD}{C.CYAN}{'=' * 62}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.WHITE}  UFOSINT Audit Pipeline — Tier B Location Normalization{C.RESET}")
            lines.append(f"  {C.BOLD}{C.CYAN}{'=' * 62}{C.RESET}")
            lines.append("")

            # Progress bar
            bar = format_bar(pct, 44)
            lines.append(f"  {C.BOLD}Progress:{C.RESET}  {bar}  {C.BOLD}{pct*100:5.1f}%{C.RESET}")
            lines.append("")

            # Stats grid
            lines.append(f"  {C.DIM}Strings:{C.RESET}   {C.BOLD}{self.processed:>8,}{C.RESET} / {self.total_strings:,}      {C.DIM}Rate:{C.RESET}    {C.BOLD}{rate:>6.1f}{C.RESET}/s")
            lines.append(f"  {C.GREEN}Improved:{C.RESET}  {C.BOLD}{C.GREEN}{self.improved:>8,}{C.RESET}                   {C.DIM}Elapsed:{C.RESET} {C.BOLD}{format_duration(elapsed):>6}{C.RESET}")
            lines.append(f"  {C.DIM}No change:{C.RESET} {self.no_change:>8,}                   {C.DIM}ETA:{C.RESET}     {C.BOLD}{format_eta(eta):>6}{C.RESET}")
            lines.append(f"  {C.RED}Errors:{C.RESET}    {self.errors:>8,}                   {C.DIM}Workers:{C.RESET} {C.BOLD}{self.workers:>6}{C.RESET}")
            lines.append(f"  {C.MAGENTA}Sightings:{C.RESET} {C.BOLD}{self.sightings_touched:>8,}{C.RESET} affected          {C.DIM}Batches:{C.RESET} {self.batches_done}/{self.batches_total}")
            lines.append("")

            # Recent fixes
            if self.recent_fixes:
                lines.append(f"  {C.BOLD}{C.YELLOW}Recent fixes:{C.RESET}")
                for fix in self.recent_fixes[-6:]:
                    orig = fix.get("original", "?")[:35]
                    city = fix.get("city") or "-"
                    state = fix.get("state") or "-"
                    country = fix.get("country") or "-"
                    n = fix.get("sightings", 0)
                    lines.append(
                        f"    {C.DIM}{orig:<35}{C.RESET} {C.GREEN}->{C.RESET} "
                        f"{city}, {state}, {country}  {C.DIM}(x{n}){C.RESET}"
                    )
            else:
                lines.append(f"  {C.DIM}Waiting for first results...{C.RESET}")

            lines.append("")
            lines.append(f"  {C.DIM}Log: {LOG_FILE}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.CYAN}{'=' * 62}{C.RESET}")

            # Move cursor up and redraw
            output = "\033[H\033[J" + "\n".join(lines)
            sys.stdout.write(output)
            sys.stdout.flush()


# ============================================================
# Parallel batch processor
# ============================================================

def process_batch_for_dashboard(batch_items, model, batch_idx):
    """Process a batch and return structured results for the dashboard."""
    results = audit._process_tier_b_batch(batch_items, model)
    structured = []
    for j, (raw, old_city, old_state, old_country, n, loc_ids_str) in enumerate(batch_items):
        llm_result = results[j][1] if j < len(results) else None
        if llm_result is None:
            structured.append({"status": "error", "original": raw})
            continue

        new_city = llm_result.get("city")
        new_state = llm_result.get("state")
        new_country = llm_result.get("country")
        confidence = llm_result.get("confidence", "low")

        changed = (new_city != old_city or new_state != old_state or new_country != old_country)
        has_improvement = new_city or (new_state and new_state != old_state) or (new_country and new_country != old_country)

        if changed and has_improvement and confidence in ("high", "medium"):
            structured.append({
                "status": "improved",
                "original": raw,
                "city": new_city, "state": new_state, "country": new_country,
                "confidence": confidence, "notes": llm_result.get("notes"),
                "old_city": old_city, "old_state": old_state, "old_country": old_country,
                "sightings": n,
                "loc_ids_str": loc_ids_str,
            })
        else:
            structured.append({
                "status": "no_change",
                "original": raw,
                "loc_ids_str": loc_ids_str,
            })
    return batch_idx, structured, batch_items


def run_tier_b_with_dashboard(limit=120000, batch_size=50, workers=15, model=audit.DEFAULT_MODEL):
    """Full Tier B run with live dashboard and file logging."""

    # Clear screen
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()

    logf = open(LOG_FILE, "a", encoding="utf-8")
    log("=" * 60, logf)
    log(f"TIER B RUN START — limit={limit}, batch_size={batch_size}, workers={workers}, model={model}", logf)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Fetch all target rows
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
        print("  No un-audited location strings found.")
        logf.close()
        conn.close()
        return

    total_sightings = sum(r[4] for r in rows)
    log(f"Loaded {len(rows):,} unique strings ({total_sightings:,} sightings)", logf)

    # Create batch tracker
    batch_id = audit._create_batch(conn, "location_normalize", model, len(rows),
                                    {"limit": limit, "batch_size": batch_size, "workers": workers})

    # Build batches
    batches = []
    for i in range(0, len(rows), batch_size):
        batches.append(rows[i:i + batch_size])

    # Init dashboard
    dash = Dashboard(len(rows), total_sightings, workers, logf)
    dash.batches_total = len(batches)

    # Fire all batches
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_batch_for_dashboard, batch, model, idx): idx
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            try:
                batch_idx, structured, batch_items = future.result()
            except Exception as e:
                log(f"FATAL: batch error: {e}", logf)
                continue

            # Write to DB (single-threaded)
            for j, result in enumerate(structured):
                raw, old_city, old_state, old_country, n, loc_ids_str = batch_items[j]
                loc_ids = [int(x) for x in loc_ids_str.split(",")]

                if result["status"] == "improved":
                    fix_json = json.dumps({
                        "city": result["city"], "state": result["state"],
                        "country": result["country"],
                        "confidence": result["confidence"], "notes": result.get("notes"),
                        "original": {"city": old_city, "state": old_state, "country": old_country},
                    })
                    for lid in loc_ids:
                        cur.execute("""
                            UPDATE location SET city = ?, state = ?, country = ?
                            WHERE id = ? AND (city IS NULL OR city = ?)
                        """, (result["city"], result["state"], result["country"], lid, old_city))
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
                else:
                    check_val = "no_improvement" if result["status"] == "no_change" else "error"
                    cur.execute(f"""
                        UPDATE sighting SET
                            audit_location_check = ?,
                            audit_batch_id = ?,
                            audit_timestamp = datetime('now')
                        WHERE location_id IN ({','.join('?' * len(loc_ids))})
                        AND audit_location_check IS NULL
                    """, [check_val, batch_id] + loc_ids)

            conn.commit()
            dash.update(structured)
            dash.render()

    # Final render
    dash.render()
    elapsed = time.time() - dash.t0

    # Summary
    summary = {
        "processed": dash.processed, "improved": dash.improved,
        "no_change": dash.no_change, "errors": dash.errors,
        "sightings_touched": dash.sightings_touched,
        "elapsed_s": round(elapsed, 1),
        "rate": round(dash.processed / elapsed, 1) if elapsed > 0 else 0,
    }
    audit._complete_batch(conn, batch_id, summary)

    log(f"COMPLETE: {json.dumps(summary)}", logf)

    # Print final summary below dashboard
    print("\n\n")
    print(f"  {C.BOLD}{C.GREEN}COMPLETE{C.RESET}")
    print(f"  {C.DIM}{'─' * 50}{C.RESET}")
    print(f"  Processed:  {dash.processed:>8,} unique location strings")
    print(f"  Improved:   {C.GREEN}{dash.improved:>8,}{C.RESET}")
    print(f"  No change:  {dash.no_change:>8,}")
    print(f"  Errors:     {dash.errors:>8,}")
    print(f"  Sightings:  {dash.sightings_touched:>8,} affected")
    print(f"  Elapsed:    {format_duration(elapsed):>8}")
    print(f"  Rate:       {dash.processed/elapsed:>8.1f}/s")
    print(f"  {C.DIM}{'─' * 50}{C.RESET}")
    print(f"  {C.YELLOW}Next: python geocode.py  (re-geocode improved locations){C.RESET}")
    print(f"  {C.DIM}Log:  {LOG_FILE}{C.RESET}")
    print()

    logf.close()
    conn.close()


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="UFOSINT Audit Pipeline — Tier B Location Normalization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=120000,
                        help="Max unique location strings to process (default: 120000 = all)")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Locations per LLM call (default: 50)")
    parser.add_argument("--workers", type=int, default=15,
                        help="Parallel LLM workers (default: 15)")
    parser.add_argument("--model", default=audit.DEFAULT_MODEL,
                        help="OpenRouter model ID")
    args = parser.parse_args()

    if not os.environ.get("OPENROUTER_API_KEY"):
        print(f"\n  {C.RED}ERROR: OPENROUTER_API_KEY not set.{C.RESET}")
        print(f"  export OPENROUTER_API_KEY='sk-or-v1-...'")
        sys.exit(1)

    run_tier_b_with_dashboard(
        limit=args.limit,
        batch_size=args.batch_size,
        workers=args.workers,
        model=args.model,
    )
