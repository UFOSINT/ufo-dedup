"""
Purge the mufon.csv import and all r/UFOs sightings from the SQLite databases.

Mirrors the purge already applied to production Postgres in v0.16
(ufosint-explorer/scripts/purge_mufon_csv_and_reddit.sql). Until this runs,
the SQLite files are *older* than production for row membership, and any
reload via migrate_sqlite_to_pg.py or reload_from_public_db.py would
resurrect both sources.

Scope, deliberately narrow:
    source_database 'MUFON'  in collection 'PUBLIUS' — the mufon.csv import
    source_database 'r/UFOs' in collection 'Reddit'  — the r/UFOs ingest

MUFON records that reached the corpus through other catalogues are NOT
touched. Those carry source_db_id = UFOCAT (or UFO-search) with MUFON named
in source_ref; every predicate here keys on source_db_id and never on
source_ref or origin_id.

The two databases carry different table sets — ufo_public.db is the stripped
export and has no duplicate_candidate or sentiment_analysis — so child tables
are handled only where present.

Usage:
    python scripts/purge_mufon_csv_and_reddit.py                 # dry run
    python scripts/purge_mufon_csv_and_reddit.py --apply
    python scripts/purge_mufon_csv_and_reddit.py --apply --vacuum
"""
import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ufosint.config import Config  # noqa: E402

# (table, column) pairs to clear before deleting the sightings themselves.
# duplicate_candidate appears twice on purpose: a pair dies if EITHER side does.
CHILD_TABLES = [
    ("attachment", "sighting_id"),
    ("sighting_reference", "sighting_id"),
    ("sentiment_analysis", "sighting_id"),
    ("sighting_analysis", "sighting_id"),
    ("duplicate_candidate", "sighting_id_a"),
    ("duplicate_candidate", "sighting_id_b"),
]

EXPECTED_MIN = 400_000
EXPECTED_MAX = 550_000


def _tables(conn):
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}


def purge(db_path, apply_changes, vacuum):
    if not os.path.exists(db_path):
        print(f"  SKIP: {db_path} not found")
        return

    size_before = os.path.getsize(db_path) / (1024 * 1024)
    print(f"\n{'='*64}\n{db_path}  ({size_before:,.0f} MB)\n{'='*64}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = OFF")
    present = _tables(conn)

    # Matched by source name only. The collections differ between stores:
    # here r/UFOs sits in PUBLIUS, because the separate 'Reddit' collection
    # was never created on this side — it existed only in Postgres, seeded by
    # add_v013_reddit_columns.sql. MUFON's collection is asserted separately
    # below so a future MUFON arriving under a different collection is caught.
    targets = conn.execute("""
        SELECT sd.id, sd.name, COALESCE(sc.name,'?')
        FROM source_database sd
        LEFT JOIN source_collection sc ON sc.id = sd.collection_id
        WHERE sd.name IN ('MUFON', 'r/UFOs')
    """).fetchall()

    if len(targets) != 2:
        print(f"  targets found: {targets}")
        print("  Expected exactly 2 (MUFON, r/UFOs) — refusing.")
        conn.close()
        return

    mufon = [t for t in targets if t[1] == "MUFON"]
    if mufon and mufon[0][2] != "PUBLIUS":
        print(f"  MUFON is in collection {mufon[0][2]!r}, expected 'PUBLIUS' — refusing.")
        conn.close()
        return

    ids = [t[0] for t in targets]
    print("  targets: " + ", ".join(f"{n} ({c}, id={i})" for i, n, c in targets))

    ph = ",".join("?" * len(ids))
    doomed = [r[0] for r in conn.execute(
        f"SELECT id FROM sighting WHERE source_db_id IN ({ph})", ids)]
    total_before = conn.execute("SELECT COUNT(*) FROM sighting").fetchone()[0]
    remaining = total_before - len(doomed)

    print(f"  sightings: {total_before:,} -> {remaining:,} "
          f"(deleting {len(doomed):,})")

    if not (EXPECTED_MIN <= remaining <= EXPECTED_MAX):
        print(f"  Survivor count {remaining:,} outside the expected "
              f"{EXPECTED_MIN:,}-{EXPECTED_MAX:,} band — refusing.")
        conn.close()
        return

    conn.execute("CREATE TEMP TABLE doomed(id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO doomed(id) VALUES (?)", [(d,) for d in doomed])

    plan = []
    for table, col in CHILD_TABLES:
        if table not in present:
            continue
        n = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IN (SELECT id FROM doomed)"
        ).fetchone()[0]
        if n:
            plan.append((table, col, n))

    # Locations the purged rows pointed at, that nothing surviving still uses.
    orphan_locs = conn.execute("""
        SELECT COUNT(*) FROM location l
        WHERE l.id IN (SELECT location_id FROM sighting
                       WHERE id IN (SELECT id FROM doomed)
                         AND location_id IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM sighting s
                          WHERE s.location_id = l.id
                            AND s.id NOT IN (SELECT id FROM doomed))
    """).fetchone()[0]

    for table, col, n in plan:
        print(f"    {table}.{col:14} -{n:,}")
    print(f"    orphaned locations       -{orphan_locs:,}")

    if not apply_changes:
        print("\n  (dry run — pass --apply to write)")
        conn.close()
        return

    for table, col, _ in plan:
        conn.execute(
            f"DELETE FROM {table} WHERE {col} IN (SELECT id FROM doomed)")

    conn.execute("""
        DELETE FROM location WHERE id IN (
            SELECT l.id FROM location l
            WHERE l.id IN (SELECT location_id FROM sighting
                           WHERE id IN (SELECT id FROM doomed)
                             AND location_id IS NOT NULL)
              AND NOT EXISTS (SELECT 1 FROM sighting s
                              WHERE s.location_id = l.id
                                AND s.id NOT IN (SELECT id FROM doomed)))
    """)
    conn.execute("DELETE FROM sighting WHERE id IN (SELECT id FROM doomed)")
    conn.execute(f"DELETE FROM source_database WHERE id IN ({ph})", ids)
    conn.execute("""
        DELETE FROM source_collection
        WHERE name = 'Reddit'
          AND id NOT IN (SELECT collection_id FROM source_database
                         WHERE collection_id IS NOT NULL)
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM sighting").fetchone()[0]
    srcs = conn.execute("""
        SELECT sd.name, COUNT(s.id) FROM source_database sd
        LEFT JOIN sighting s ON s.source_db_id = sd.id
        GROUP BY sd.name ORDER BY 2 DESC
    """).fetchall()
    print(f"\n  sightings now: {after:,}")
    print(f"  sources now:   {srcs}")

    if vacuum:
        print("  VACUUM (reclaiming space, this takes a while) …")
        conn.isolation_level = None
        conn.execute("VACUUM")

    conn.close()
    size_after = os.path.getsize(db_path) / (1024 * 1024)
    print(f"  file: {size_before:,.0f} MB -> {size_after:,.0f} MB")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", action="append", default=None,
                    help="database to purge (repeatable; default: both)")
    ap.add_argument("--apply", action="store_true", help="actually delete")
    ap.add_argument("--vacuum", action="store_true",
                    help="VACUUM afterwards to reclaim file space")
    args = ap.parse_args()

    paths = args.db or [Config.db_path(), Config.public_db_path()]
    for p in paths:
        purge(p, args.apply, args.vacuum)
    return 0


if __name__ == "__main__":
    sys.exit(main())
