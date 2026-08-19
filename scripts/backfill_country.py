"""
Backfill location.country_iso2 from coordinates.

The `country` text column is unusable as a filter key: 715 distinct values
mixing ISO codes, country names, US/Canadian state codes, cities, oceans and
CSV parse fragments. Coordinates are unambiguous, so this derives a clean
ISO-3166-1 alpha-2 code from lat/lng via the GeoNames gazetteer.

Offshore and border sightings are deliberately left NULL rather than being
assigned a best guess — see ufosint/processors/country.py for the rules.
A NULL here means "we decline to say", which is a different and more honest
claim than "unknown".

Usage:
    python scripts/backfill_country.py                    # dry run, report only
    python scripts/backfill_country.py --apply            # write to SQLite
    python scripts/backfill_country.py --csv out.csv      # emit id,iso2 for PG
    python scripts/backfill_country.py --db path/to.db

The CSV is the handoff to the web app's Postgres: location ids are copied
verbatim by migrate_sqlite_to_pg.py, so the same file loads cleanly there.
"""
import argparse
import csv
import os
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ufosint.config import Config                       # noqa: E402
from ufosint.processors.country import (                # noqa: E402
    BORDER_MARGIN_KM,
    OFFSHORE_KM,
    Gazetteer,
)


def compute(db_path):
    """Return (rows, stats) without touching the database.

    rows is a list of (location_id, iso2) for locations we are confident
    about. Everything else is counted in stats and left alone.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = conn.execute(
        "SELECT id, latitude, longitude FROM location "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    )
    gaz = Gazetteer().load()

    rows = []
    stats = Counter()
    countries = Counter()
    for loc_id, lat, lng in cur:
        iso2, reason = gaz.lookup(lat, lng)
        stats[reason] += 1
        if iso2:
            rows.append((loc_id, iso2))
            countries[iso2] += 1
    conn.close()
    return rows, stats, countries


def apply_to_sqlite(db_path, rows):
    """Add country_iso2 if missing and write the resolved codes."""
    conn = sqlite3.connect(db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(location)")]
    if "country_iso2" not in cols:
        conn.execute("ALTER TABLE location ADD COLUMN country_iso2 TEXT")
        print("  added column location.country_iso2")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_location_country_iso2 "
        "ON location(country_iso2)"
    )
    conn.executemany(
        "UPDATE location SET country_iso2 = ? WHERE id = ?",
        [(iso2, loc_id) for loc_id, iso2 in rows],
    )
    conn.commit()
    conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=None, help="SQLite path (default: Config.db_path())")
    ap.add_argument("--apply", action="store_true", help="write country_iso2 to the DB")
    ap.add_argument("--csv", default=None, help="write id,country_iso2 to this path")
    args = ap.parse_args()

    db_path = args.db or Config.db_path()
    if not os.path.exists(db_path):
        print(f"ERROR: {db_path} not found")
        return 1

    print(f"Database : {db_path}")
    print(f"Rules    : offshore > {OFFSHORE_KM:.0f} km, border margin < {BORDER_MARGIN_KM:.0f} km\n")

    rows, stats, countries = compute(db_path)
    total = sum(stats.values())

    print(f"Locations with coordinates : {total:,}")
    print(f"  coded                    : {stats['ok']:,} ({100.0*stats['ok']/total:.1f}%)")
    print(f"  left NULL — offshore     : {stats['offshore']:,}")
    print(f"  left NULL — border       : {stats['border']:,}")
    if stats["bad_coords"]:
        print(f"  left NULL — bad coords   : {stats['bad_coords']:,}")
    print(f"\nDistinct countries resolved : {len(countries)}")
    print("Top 12:")
    for cc, n in countries.most_common(12):
        print(f"   {cc}  {n:>7,}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["location_id", "country_iso2"])
            w.writerows(rows)
        print(f"\nwrote {len(rows):,} rows to {args.csv}")

    if args.apply:
        apply_to_sqlite(db_path, rows)
        print(f"\napplied {len(rows):,} country codes to {db_path}")
    else:
        print("\n(dry run — pass --apply to write)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
