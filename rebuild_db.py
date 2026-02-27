"""
Master rebuild script for the Unified UFO Sightings Database.

Orchestrates the full pipeline:
  1. Create fresh schema
  2. Import all 5 sources (UFOCAT skips UFOReportCtr)
  3. Apply data quality fixes (coordinates, city fields, country codes)
  4. Enrich NUFORC records with UFOCAT metadata
  5. Run deduplication
  6. Copy to explorer

Usage:
    python rebuild_db.py              # Full rebuild
    python rebuild_db.py --skip-dedup # Skip dedup (faster for testing)
"""
import os
import sys
import time
import sqlite3
import argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "ufo_unified.db")
EXPLORER_DB = os.path.join(BASE_DIR, "ufo-explorer", "ufo_unified.db")

# US states + Canadian provinces for longitude fix
US_CA_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU', 'AS', 'MP',
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
}


def step(num, desc):
    """Print a step header."""
    print(f"\n{'='*60}")
    print(f"  STEP {num}: {desc}")
    print(f"{'='*60}\n")


def run_script(name):
    """Import and run a script's main function."""
    import importlib
    mod = importlib.import_module(name)
    if hasattr(mod, 'run_import'):
        mod.run_import()
    elif hasattr(mod, 'run_enrichment'):
        mod.run_enrichment()
    elif hasattr(mod, 'create_schema'):
        mod.create_schema()
    elif hasattr(mod, 'main'):
        # For dedup, override sys.argv to avoid argparse issues
        old_argv = sys.argv
        sys.argv = [name + '.py']
        mod.main()
        sys.argv = old_argv


def apply_data_fixes():
    """Apply post-import data quality fixes."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Fix 1a: UFOCAT longitude sign (US/CA locations with positive longitude)
    # UFOCAT stored ALL longitudes with inverted signs. US/CA should be negative.
    print("  Fixing UFOCAT longitude signs (US/CA -> negative)...")
    state_list = ','.join(f"'{s}'" for s in US_CA_STATES)
    cur.execute(f"""
        UPDATE location SET longitude = -longitude
        WHERE longitude > 0
        AND state IN ({state_list})
        AND id IN (
            SELECT location_id FROM sighting
            WHERE source_db_id = (SELECT id FROM source_database WHERE name='UFOCAT')
        )
    """)
    print(f"    Fixed {cur.rowcount:,} US/CA longitude signs")

    # Fix 1b: UFOCAT longitude sign (all OTHER locations — rest of world)
    # Same sign inversion: Eastern Hemisphere countries have negative lons (should be
    # positive), and Western Hemisphere countries outside US/CA have positive lons
    # (should be negative). Fix: negate ALL non-US/CA UFOCAT longitudes.
    print("  Fixing UFOCAT longitude signs (rest of world)...")
    cur.execute(f"""
        UPDATE location SET longitude = -longitude
        WHERE longitude IS NOT NULL
        AND (state IS NULL OR state NOT IN ({state_list}))
        AND id IN (
            SELECT location_id FROM sighting
            WHERE source_db_id = (SELECT id FROM source_database WHERE name='UFOCAT')
        )
    """)
    print(f"    Fixed {cur.rowcount:,} non-US/CA longitude signs")

    # Fix 2: UFOCAT city field (copy from raw_text where city is NULL)
    print("  Copying UFOCAT city from raw_text...")
    cur.execute("""
        UPDATE location SET city = raw_text
        WHERE city IS NULL AND raw_text IS NOT NULL
        AND id IN (
            SELECT location_id FROM sighting
            WHERE source_db_id = (SELECT id FROM source_database WHERE name='UFOCAT')
        )
    """)
    print(f"    Copied {cur.rowcount:,} city values")

    # Fix 3: Country code normalization
    print("  Normalizing country codes...")
    country_map = {
        'USA': 'US', 'United States': 'US', 'United States of America': 'US',
        'United Kingdom': 'GB', 'UK': 'GB', 'England': 'GB',
        'Canada': 'CA', 'Australia': 'AU',
    }
    for old, new in country_map.items():
        cur.execute("UPDATE location SET country = ? WHERE country = ?", (new, old))

    # Fix 4: MUFON date normalization (strip \n artifacts)
    print("  Fixing MUFON date artifacts...")
    cur.execute(r"""
        UPDATE sighting SET date_event_raw = REPLACE(date_event_raw, '\n', ' ')
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='MUFON')
        AND date_event_raw LIKE '%\n%'
    """)

    conn.commit()
    conn.close()
    print("  Data fixes applied.")


def copy_to_explorer():
    """Copy DB to explorer using sqlite3.backup (WAL-safe)."""
    if not os.path.isdir(os.path.join(BASE_DIR, "ufo-explorer")):
        print("  ufo-explorer/ directory not found, skipping copy.")
        return

    print(f"  Copying to {EXPLORER_DB}...")
    src = sqlite3.connect(DB_PATH)
    # Remove old explorer DB if exists
    if os.path.exists(EXPLORER_DB):
        os.remove(EXPLORER_DB)
    dst = sqlite3.connect(EXPLORER_DB)
    src.backup(dst)
    src.close()
    dst.close()
    size_mb = os.path.getsize(EXPLORER_DB) / (1024 * 1024)
    print(f"  Explorer DB copied ({size_mb:.0f} MB)")


def main():
    parser = argparse.ArgumentParser(description="Rebuild Unified UFO Database")
    parser.add_argument('--skip-dedup', action='store_true', help='Skip deduplication step')
    parser.add_argument('--skip-explorer', action='store_true', help='Skip explorer DB copy')
    args = parser.parse_args()

    overall_t0 = time.time()

    # Remove old DB
    if os.path.exists(DB_PATH):
        print(f"Removing old database: {DB_PATH}")
        os.remove(DB_PATH)
        # Also remove WAL/SHM files
        for ext in ('.db-wal', '.db-shm'):
            p = DB_PATH.replace('.db', ext)
            if os.path.exists(p):
                os.remove(p)

    step(1, "Create schema")
    run_script('create_schema')

    step(2, "Import UFOCAT (skips UFOReportCtr)")
    run_script('import_ufocat')

    step(3, "Import NUFORC")
    run_script('import_nuforc')

    step(4, "Import MUFON")
    run_script('import_mufon')

    step(5, "Import UPDB (skips MUFON/NUFORC)")
    run_script('import_updb')

    step(6, "Import UFO-search (was Geldreich)")
    run_script('import_geldreich')

    step(7, "Apply data quality fixes")
    apply_data_fixes()

    step(8, "Enrich NUFORC with UFOCAT metadata")
    run_script('enrich')

    if not args.skip_dedup:
        step(9, "Deduplication")
        run_script('dedup')
    else:
        print("\n  Skipping deduplication (--skip-dedup)")

    if not args.skip_explorer:
        step(10, "Copy to explorer")
        copy_to_explorer()
    else:
        print("\n  Skipping explorer copy (--skip-explorer)")

    # Final stats
    print(f"\n{'='*60}")
    print(f"  REBUILD COMPLETE")
    print(f"{'='*60}")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT sd.name, COUNT(*) FROM sighting s
        JOIN source_database sd ON s.source_db_id = sd.id
        GROUP BY sd.name ORDER BY COUNT(*) DESC
    """)
    total = 0
    for name, count in cur.fetchall():
        print(f"  {name:15s} {count:>10,}")
        total += count
    print(f"  {'TOTAL':15s} {total:>10,}")

    cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
    dups = cur.fetchone()[0]
    print(f"\n  Duplicate candidates: {dups:,}")

    # Check enrichment results
    cur.execute("""
        SELECT COUNT(*) FROM sighting
        WHERE source_db_id = (SELECT id FROM source_database WHERE name='NUFORC')
        AND hynek IS NOT NULL
    """)
    enriched = cur.fetchone()[0]
    print(f"  NUFORC records with Hynek (enriched): {enriched:,}")

    conn.close()

    elapsed = time.time() - overall_t0
    print(f"\n  Total elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"  Database size: {db_size:.0f} MB")


if __name__ == "__main__":
    main()
