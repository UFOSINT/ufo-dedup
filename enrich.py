"""
Enrich NUFORC sightings with metadata from skipped UFOCAT records.

Reads the enrichment sidecar (ufocat_enrichment.jsonl) produced by
import_ufocat.py and transfers Hynek classification, Vallee classification,
and shape data to matching NUFORC records.

Matching uses date + normalized city + state (same logic as dedup Tier 1b).

Run AFTER all imports are complete:
    python enrich.py
"""
import sqlite3
import json
import os
import re
import time
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
ENRICHMENT_PATH = os.path.join(os.path.dirname(__file__), "ufocat_enrichment.jsonl")


def normalize_city(city_str):
    """Normalize a city name for matching (same logic as dedup.py)."""
    if not city_str:
        return ''
    c = city_str.strip().upper()
    c = re.sub(r'\s*\(.*\)\s*$', '', c)
    c = re.sub(r'[\?\.\!]+$', '', c)
    c = re.sub(r'\s+', ' ', c).strip()
    return c


def run_enrichment():
    if not os.path.exists(ENRICHMENT_PATH):
        print(f"No enrichment file found at {ENRICHMENT_PATH}")
        print("Run import_ufocat.py first to generate it.")
        return

    t0 = time.time()

    # ---- Load enrichment data, keyed by (date, city_norm, state_norm) ----
    print(f"Loading enrichment data from {ENRICHMENT_PATH}...")
    enrich_groups = defaultdict(list)
    loaded = 0
    with open(ENRICHMENT_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            date = rec.get('date')
            if not date:
                continue
            date_10 = date[:10]
            city_norm = normalize_city(rec.get('location', ''))
            state_norm = (rec.get('state') or '').strip().upper()
            if not city_norm:
                continue
            key = (date_10, city_norm, state_norm)
            enrich_groups[key].append(rec)
            loaded += 1

    print(f"  {loaded:,} enrichment records in {len(enrich_groups):,} groups")

    # ---- Load NUFORC sightings keyed the same way ----
    print("Loading NUFORC sightings from database...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    cur.execute("SELECT id FROM source_database WHERE name='NUFORC'")
    nuforc_id = cur.fetchone()[0]

    cur.execute("""
        SELECT s.id, SUBSTR(s.date_event, 1, 10) as d,
               l.city, l.state,
               s.hynek, s.vallee, s.shape
        FROM sighting s
        JOIN location l ON s.location_id = l.id
        WHERE s.source_db_id = ?
          AND s.date_event IS NOT NULL
    """, (nuforc_id,))

    nuforc_groups = defaultdict(list)
    nuforc_count = 0
    for sid, d, city, state, hynek, vallee, shape in cur:
        city_n = normalize_city(city)
        state_n = (state or '').strip().upper()
        if not d or not city_n:
            continue
        key = (d, city_n, state_n)
        nuforc_groups[key].append({
            'id': sid,
            'hynek': hynek,
            'vallee': vallee,
            'shape': shape,
        })
        nuforc_count += 1

    print(f"  {nuforc_count:,} NUFORC records in {len(nuforc_groups):,} groups")

    # ---- Match and enrich ----
    print("Matching and enriching...")
    overlap_keys = set(enrich_groups.keys()) & set(nuforc_groups.keys())
    print(f"  Overlapping groups: {len(overlap_keys):,}")

    updates_hynek = 0
    updates_vallee = 0
    updates_shape = 0
    matched_nuforc = 0
    unmatched_enrich = 0

    update_batch = []  # (hynek, vallee, shape, sighting_id)

    for key in overlap_keys:
        enrich_recs = enrich_groups[key]
        nuforc_recs = nuforc_groups[key]

        # For each NUFORC record in this group, find the best enrichment source
        for nr in nuforc_recs:
            # Pick the first enrichment record that has metadata to offer
            best = None
            for er in enrich_recs:
                if er.get('hynek') or er.get('vallee') or er.get('shape'):
                    best = er
                    break
            if not best:
                continue

            new_hynek = best.get('hynek') if not nr['hynek'] else None
            new_vallee = best.get('vallee') if not nr['vallee'] else None
            new_shape = best.get('shape') if not nr['shape'] else None

            if new_hynek or new_vallee or new_shape:
                update_batch.append((
                    new_hynek or nr['hynek'],
                    new_vallee or nr['vallee'],
                    new_shape or nr['shape'],
                    nr['id']
                ))
                if new_hynek:
                    updates_hynek += 1
                if new_vallee:
                    updates_vallee += 1
                if new_shape:
                    updates_shape += 1
                matched_nuforc += 1

    # Count unmatched enrichment records
    for key in enrich_groups:
        if key not in nuforc_groups:
            unmatched_enrich += len(enrich_groups[key])

    # ---- Apply updates ----
    print(f"  Applying {len(update_batch):,} updates...")
    cur.executemany("""
        UPDATE sighting SET hynek = ?, vallee = ?, shape = ?
        WHERE id = ?
    """, update_batch)
    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\nEnrichment complete in {elapsed:.1f}s:")
    print(f"  NUFORC records enriched: {matched_nuforc:,}")
    print(f"  Hynek classifications added: {updates_hynek:,}")
    print(f"  Vallee classifications added: {updates_vallee:,}")
    print(f"  Shape values added: {updates_shape:,}")
    print(f"  Unmatched enrichment records: {unmatched_enrich:,}")


if __name__ == "__main__":
    run_enrichment()
