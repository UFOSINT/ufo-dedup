"""
Fix corrupted coordinates in the unified UFO database.

Some locations have lat/lon values scaled by 100x or 1000x due to
decimal-point loss in the original source data. This script:
1. Finds all locations where |latitude| > 90 or |longitude| > 180
2. Auto-repairs by dividing by 10/100/1000 to restore valid ranges
3. NULLs out any values that can't be reliably corrected
4. Prints a report of all changes
"""
import sqlite3
import os
import sys


def fix_coordinates(db_path):
    """Fix invalid coordinates in the given database."""
    if not os.path.exists(db_path):
        print(f"  SKIP: {db_path} not found")
        return

    size = os.path.getsize(db_path) / (1024 * 1024)
    print(f"\n{'='*60}")
    print(f"Fixing: {db_path} ({size:.0f} MB)")
    print(f"{'='*60}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Find all invalid coordinates
    cur.execute("""
        SELECT id, raw_text, city, state, country, latitude, longitude
        FROM location
        WHERE (latitude IS NOT NULL AND (latitude > 90 OR latitude < -90))
           OR (longitude IS NOT NULL AND (longitude > 180 OR longitude < -180))
    """)
    bad_rows = cur.fetchall()

    if not bad_rows:
        print("  No invalid coordinates found!")
        conn.close()
        return

    print(f"  Found {len(bad_rows)} locations with invalid coordinates\n")

    fixed = 0
    nulled = 0

    for row in bad_rows:
        loc_id, raw_text, city, state, country, lat, lon = row
        label = f"  [{loc_id}] {city or raw_text or '?'}, {state or ''} {country or ''}"
        old_lat, old_lon = lat, lon

        new_lat = lat
        new_lon = lon
        lat_action = ""
        lon_action = ""

        # Fix latitude if out of range
        if lat is not None and (lat > 90 or lat < -90):
            repaired = False
            for divisor in [10, 100, 1000, 10000]:
                candidate = lat / divisor
                if -90 <= candidate <= 90:
                    new_lat = round(candidate, 6)
                    lat_action = f"lat {lat} -> {new_lat} (/{divisor})"
                    repaired = True
                    break
            if not repaired:
                new_lat = None
                lat_action = f"lat {lat} -> NULL (unfixable)"

        # Fix longitude if out of range
        if lon is not None and (lon > 180 or lon < -180):
            repaired = False
            for divisor in [10, 100, 1000, 10000]:
                candidate = lon / divisor
                if -180 <= candidate <= 180:
                    new_lon = round(candidate, 6)
                    lon_action = f"lon {lon} -> {new_lon} (/{divisor})"
                    repaired = True
                    break
            if not repaired:
                new_lon = None
                lon_action = f"lon {lon} -> NULL (unfixable)"

        # Apply fix
        actions = " | ".join(filter(None, [lat_action, lon_action]))
        print(f"{label}")
        print(f"    {actions}")

        cur.execute("""
            UPDATE location SET latitude = ?, longitude = ?
            WHERE id = ?
        """, (new_lat, new_lon, loc_id))

        if new_lat is None or new_lon is None:
            nulled += 1
        else:
            fixed += 1

    conn.commit()
    conn.close()

    print(f"\n  Summary: {fixed} auto-repaired, {nulled} nulled, {len(bad_rows)} total processed")


if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))

    # Fix both databases
    databases = [
        os.path.join(base, "ufo_unified.db"),
        os.path.join(base, "ufo-explorer", "ufo_unified.db"),
    ]

    for db_path in databases:
        fix_coordinates(db_path)

    print("\nDone!")
