"""
UAP Gerb overlay: crash-retrieval records, nuclear encounters, and facility
nodes, plus a derived `distance_to_nearest_nuclear_site_km` column computed
for every geocoded sighting in the main corpus.

Data source: uap-gerb-integration-bundle.zip (14 crash records, 35 nuclear
sighting records, 62 geocoded facility nodes).

Usage:
    python gerb_overlay.py                          # import + compute proximity
    python gerb_overlay.py --stats-only             # print coverage
    python gerb_overlay.py --db PATH                # custom DB path
    python gerb_overlay.py --bundle PATH            # custom zip path

Designed to plug into analyze.py's ANALYSIS_STEPS registry.
"""
import json
import math
import os
import sqlite3
import sys
import time
import zipfile

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "output", "ufo_unified.db"
)
BUNDLE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "uap-gerb-integration-bundle.zip"
)

BATCH_SIZE = 5000


# ============================================================
# Haversine
# ============================================================

def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two (lat, lon) points."""
    R = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ============================================================
# Import from bundle
# ============================================================

def import_crashes(conn, bundle_path=BUNDLE_PATH):
    """Import crash-retrieval records from crashes.json in the bundle."""
    with zipfile.ZipFile(bundle_path) as z:
        data = json.loads(z.read("export/crashes.json"))

    crashes = data.get("crashes", [])
    cur = conn.cursor()
    cur.execute("DELETE FROM crash_retrieval")  # idempotent

    for c in crashes:
        loc = c.get("location", {})
        craft = c.get("craft", {})
        recovery = c.get("recovery", {})
        cur.execute(
            """INSERT INTO crash_retrieval
               (id, page_name, year, date_event, city, region, country,
                latitude, longitude, precision, craft_type, craft_size_m,
                recovery_status, has_biologics, crew_count,
                evidence_quality, source_confidence, short_summary, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                c.get("id"),
                c.get("page_name"),
                c.get("year"),
                c.get("date"),
                loc.get("city"),
                loc.get("region"),
                loc.get("country"),
                loc.get("lat"),
                loc.get("lon"),
                loc.get("precision"),
                craft.get("type"),
                craft.get("size_meters"),
                recovery.get("status"),
                1 if recovery.get("non_human_biologics") else 0,
                str(recovery.get("crew_count", "")),
                str(c.get("evidence_quality", "")),
                str(c.get("source_confidence", "")),
                c.get("short_summary"),
                json.dumps(c),
            ),
        )

    conn.commit()
    print(f"  Crash retrievals imported: {len(crashes)}")
    return len(crashes)


def import_nuclear_encounters(conn, bundle_path=BUNDLE_PATH):
    """Import nuclear sighting records from nuclear_sightings.json."""
    with zipfile.ZipFile(bundle_path) as z:
        entries = json.loads(z.read("export/nuclear_sightings.json"))

    cur = conn.cursor()
    cur.execute("DELETE FROM nuclear_encounter")

    for e in entries:
        sensor = e.get("sensor_confirmation")
        cur.execute(
            """INSERT INTO nuclear_encounter
               (page_name, year, date_event, base, city, region, country,
                latitude, longitude, weapon_system, incident_type,
                missiles_affected, sensor_confirmation, witness_credibility,
                evidence_quality, source_confidence, summary, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                e.get("page_name"),
                e.get("year"),
                e.get("date"),
                e.get("base"),
                e.get("location_city"),
                e.get("location_region"),
                e.get("location_country"),
                e.get("location_lat"),
                e.get("location_lon"),
                e.get("weapon_system"),
                e.get("incident_type"),
                e.get("missiles_affected", 0),
                json.dumps(sensor) if sensor else None,
                e.get("witness_credibility"),
                e.get("evidence_quality"),
                e.get("source_confidence"),
                e.get("summary"),
                json.dumps(e),
            ),
        )

    conn.commit()
    print(f"  Nuclear encounters imported: {len(entries)}")
    return len(entries)


def import_facilities(conn, bundle_path=BUNDLE_PATH):
    """Import all geocoded facility nodes from combined.geojson + nuclear bases."""
    with zipfile.ZipFile(bundle_path) as z:
        combined = json.loads(z.read("export/combined.geojson"))
        nuc_entries = json.loads(z.read("export/nuclear_sightings.json"))

    cur = conn.cursor()
    cur.execute("DELETE FROM facility")

    seen = set()  # deduplicate by (round(lat,3), round(lon,3))

    # Facilities from the combined GeoJSON
    for f in combined["features"]:
        props = f["properties"]
        ftype = props.get("type", props.get("feature_type", ""))
        if ftype != "facility":
            continue
        geom = f.get("geometry")
        if not geom or not geom.get("coordinates"):
            continue
        lon, lat = geom["coordinates"][:2]
        key = (round(lat, 3), round(lon, 3))
        if key in seen:
            continue
        seen.add(key)
        name = props.get("title", props.get("name", props.get("full_name", "")))
        cur.execute(
            "INSERT INTO facility (name, facility_type, latitude, longitude, source) "
            "VALUES (?, ?, ?, ?, ?)",
            (name or None, props.get("facility_type", ""), lat, lon, "combined_geojson"),
        )

    # Nuclear bases from nuclear_sightings.json (some may not be in the GeoJSON)
    for e in nuc_entries:
        lat = e.get("location_lat")
        lon = e.get("location_lon")
        base = e.get("base")
        if not lat or not lon or not base:
            continue
        key = (round(lat, 3), round(lon, 3))
        if key in seen:
            continue
        seen.add(key)
        cur.execute(
            "INSERT INTO facility (name, facility_type, latitude, longitude, source) "
            "VALUES (?, ?, ?, ?, ?)",
            (base, "nuclear_base", lat, lon, "nuclear_sightings"),
        )

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM facility")
    total = cur.fetchone()[0]
    print(f"  Facilities imported: {total} (deduplicated)")
    return total


# ============================================================
# Nuclear proximity computation
# ============================================================

# Facility types considered "nuclear-relevant" for the proximity computation.
# Excludes crash-retrieval storage sites (cemetery, hospital), phenomenon
# observation sites (Hessdalen, Skinwalker), and general contractor HQs.
NUCLEAR_FACILITY_TYPES = {
    "military_base",
    "national_lab",
    "nuclear_test_site",
    "test_range",
    "nuclear_base",
}


def compute_nuclear_proximity(conn):
    """Compute distance_to_nearest_nuclear_site_km for every geocoded sighting.

    Uses a filtered subset of the `facility` table (only NUCLEAR_FACILITY_TYPES)
    as the reference set. O(n × m) where n = geocoded sightings (~396k) and
    m = nuclear facilities (~50). Pure Python haversine — runs in ~2 min.
    """
    cur = conn.cursor()

    # Load only nuclear-relevant facility coords
    placeholders = ",".join("?" * len(NUCLEAR_FACILITY_TYPES))
    cur.execute(
        f"SELECT id, name, latitude, longitude FROM facility "
        f"WHERE facility_type IN ({placeholders})",
        list(NUCLEAR_FACILITY_TYPES),
    )
    facilities = cur.fetchall()
    if not facilities:
        print("  WARNING: no facilities in DB. Run import_facilities() first.")
        return

    fac_coords = [(fid, name, lat, lon) for fid, name, lat, lon in facilities]
    print(f"  Reference set: {len(fac_coords)} facilities")

    # Load all geocoded sightings
    cur.execute("SELECT id, lat, lng FROM sighting WHERE lat IS NOT NULL AND lng IS NOT NULL")
    sightings = cur.fetchall()
    print(f"  Sightings with coords: {len(sightings):,}")

    if not sightings:
        print("  No geocoded sightings. Skipping proximity.")
        return

    t0 = time.time()
    updates = []

    for sid, slat, slng in sightings:
        best_dist = float("inf")
        best_name = None
        for _, fname, flat, flon in fac_coords:
            d = haversine_km(slat, slng, flat, flon)
            if d < best_dist:
                best_dist = d
                best_name = fname
        updates.append((round(best_dist, 1), best_name, sid))

    # Batch write
    for i in range(0, len(updates), BATCH_SIZE):
        cur.executemany(
            "UPDATE sighting SET distance_to_nearest_nuclear_site_km = ?, "
            "nearest_nuclear_site_name = ? WHERE id = ?",
            updates[i : i + BATCH_SIZE],
        )
        conn.commit()

    elapsed = time.time() - t0
    print(f"  Proximity computed: {len(updates):,} sightings in {elapsed:.0f}s")

    # Stats
    cur.execute(
        "SELECT AVG(distance_to_nearest_nuclear_site_km), "
        "MIN(distance_to_nearest_nuclear_site_km), "
        "MAX(distance_to_nearest_nuclear_site_km) "
        "FROM sighting WHERE distance_to_nearest_nuclear_site_km IS NOT NULL"
    )
    avg_d, min_d, max_d = cur.fetchone()
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE distance_to_nearest_nuclear_site_km < 50"
    )
    within_50 = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM sighting WHERE distance_to_nearest_nuclear_site_km < 100"
    )
    within_100 = cur.fetchone()[0]
    print(f"  Distances: avg={avg_d:.0f} km, min={min_d:.1f} km, max={max_d:.0f} km")
    print(f"  Within 50 km of a nuclear site: {within_50:,}")
    print(f"  Within 100 km of a nuclear site: {within_100:,}")


# ============================================================
# NRC Lexicon denormalization
# ============================================================

def denormalize_nrc(conn):
    """Copy NRC word-counts from sentiment_analysis to sighting columns.

    Reads the existing emo_* columns from the sentiment_analysis table
    (populated by sentiment.py during rebuild_db.py) and writes them to
    dedicated nrc_* columns on sighting.

    NRCLex also produces 'positive' and 'negative' word counts — these
    are not in our sentiment_analysis table (which only stores the 8
    emotion dimensions). We re-derive them from the raw text via NRCLex
    only if the nrclex library is available; otherwise those 2 columns
    stay NULL.
    """
    cur = conn.cursor()

    # Check if sentiment_analysis exists and has data
    try:
        cur.execute("SELECT COUNT(*) FROM sentiment_analysis WHERE emo_joy IS NOT NULL")
        total = cur.fetchone()[0]
    except sqlite3.OperationalError:
        print("  sentiment_analysis table not found — run sentiment.py first.")
        return

    if total == 0:
        print("  sentiment_analysis has 0 rows with emotion data. Skipping NRC denorm.")
        return

    print(f"  Denormalizing {total:,} NRC records from sentiment_analysis to sighting...")

    cur.execute("""
        UPDATE sighting SET
            nrc_joy          = (SELECT emo_joy          FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_fear         = (SELECT emo_fear         FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_anger        = (SELECT emo_anger        FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_sadness      = (SELECT emo_sadness      FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_surprise     = (SELECT emo_surprise     FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_disgust      = (SELECT emo_disgust      FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_trust        = (SELECT emo_trust        FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id),
            nrc_anticipation = (SELECT emo_anticipation FROM sentiment_analysis sa WHERE sa.sighting_id = sighting.id)
        WHERE id IN (SELECT sighting_id FROM sentiment_analysis)
    """)
    conn.commit()

    # Stats
    cur.execute("SELECT COUNT(*) FROM sighting WHERE nrc_fear IS NOT NULL")
    populated = cur.fetchone()[0]
    cur.execute("""
        SELECT AVG(nrc_fear), AVG(nrc_trust), AVG(nrc_joy), AVG(nrc_anticipation)
        FROM sighting WHERE nrc_fear IS NOT NULL
    """)
    avg_fear, avg_trust, avg_joy, avg_antic = cur.fetchone()
    print(f"  NRC denormalized: {populated:,} sightings")
    print(f"  Avg word counts: fear={avg_fear:.1f}, trust={avg_trust:.1f}, "
          f"joy={avg_joy:.1f}, anticipation={avg_antic:.1f}")


# ============================================================
# Orchestration
# ============================================================

def run_gerb_overlay(db_path=DB_PATH, bundle_path=BUNDLE_PATH):
    """Full import + compute: crash/nuclear/facility data + proximity + NRC."""
    t0 = time.time()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    if os.path.exists(bundle_path):
        print("\n[1/4] Importing crash retrievals...")
        import_crashes(conn, bundle_path)

        print("\n[2/4] Importing nuclear encounters...")
        import_nuclear_encounters(conn, bundle_path)

        print("\n[3/4] Importing facilities...")
        import_facilities(conn, bundle_path)
    else:
        print(f"\n  Bundle not found at {bundle_path}")
        print("  Skipping crash/nuclear/facility import (proximity will use existing facility table)")

    print("\n[4/4] Computing nuclear proximity for all geocoded sightings...")
    compute_nuclear_proximity(conn)

    elapsed = time.time() - t0
    print(f"\n  Gerb overlay complete in {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    conn.close()


def print_stats(db_path=DB_PATH):
    """Print current overlay statistics."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for table, label in [
        ("crash_retrieval", "Crash retrievals"),
        ("nuclear_encounter", "Nuclear encounters"),
        ("facility", "Facilities"),
    ]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {label}: {cur.fetchone()[0]:,}")
        except sqlite3.OperationalError:
            print(f"  {label}: table not found")

    cur.execute("SELECT COUNT(*) FROM sighting WHERE distance_to_nearest_nuclear_site_km IS NOT NULL")
    print(f"  Sightings with nuclear proximity: {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM sighting WHERE nrc_fear IS NOT NULL")
    print(f"  Sightings with NRC counts: {cur.fetchone()[0]:,}")

    conn.close()


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="UAP Gerb overlay + NRC denormalization")
    parser.add_argument("--db", default=DB_PATH, help="SQLite database path")
    parser.add_argument("--bundle", default=BUNDLE_PATH, help="Gerb bundle zip path")
    parser.add_argument("--stats-only", action="store_true", help="Print stats only")
    parser.add_argument("--nrc-only", action="store_true", help="Only run NRC denormalization")
    args = parser.parse_args()

    if args.stats_only:
        print_stats(args.db)
    elif args.nrc_only:
        conn = sqlite3.connect(args.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        denormalize_nrc(conn)
        conn.close()
    else:
        run_gerb_overlay(args.db, args.bundle)
        # NRC denormalization is separate from gerb overlay but runs in same session
        print("\n[NRC] Denormalizing NRC word-counts to sighting columns...")
        conn = sqlite3.connect(args.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        denormalize_nrc(conn)
        conn.close()
