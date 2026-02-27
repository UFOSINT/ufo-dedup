"""Print final summary of the unified UFO database."""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("=" * 70)
print("  UNIFIED UFO DATABASE - FINAL SUMMARY")
print("=" * 70)

cur.execute("SELECT COUNT(*) FROM sighting")
total = cur.fetchone()[0]
print(f"\n  Total sightings:      {total:,}")

cur.execute("SELECT COUNT(*) FROM location")
print(f"  Total locations:      {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM source_database")
print(f"  Source databases:     {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM source_origin")
print(f"  Source origins:       {cur.fetchone()[0]:,}")

print(f"\n  Records by collection:")
cur.execute("""
    SELECT sc.name, COUNT(s.id)
    FROM source_collection sc
    JOIN source_database sd ON sd.collection_id = sc.id
    LEFT JOIN sighting s ON s.source_db_id = sd.id
    GROUP BY sc.id ORDER BY COUNT(s.id) DESC
""")
for name, cnt in cur.fetchall():
    bar = "#" * int(cnt / total * 50)
    print(f"    {name:<15} {cnt:>10,}  {bar}")

print(f"\n  Records by source:")
cur.execute("""
    SELECT sd.name, sc.name as collection, COUNT(s.id)
    FROM source_database sd
    JOIN source_collection sc ON sd.collection_id = sc.id
    LEFT JOIN sighting s ON s.source_db_id = sd.id
    GROUP BY sd.id ORDER BY COUNT(s.id) DESC
""")
for name, collection, cnt in cur.fetchall():
    bar = "#" * int(cnt / total * 50)
    print(f"    {name:<15} [{collection:<10}] {cnt:>10,}  {bar}")

print(f"\n  Field coverage:")
fields = [
    ("date_event", "Parsed date"),
    ("description", "Description text"),
    ("shape", "Object shape"),
    ("hynek", "Hynek class"),
    ("vallee", "Vallee class"),
    ("event_type", "Event type"),
    ("duration", "Duration"),
    ("num_witnesses", "Witness count"),
    ("color", "Color"),
    ("explanation", "Explanation"),
]
for field, label in fields:
    cur.execute(
        f"SELECT COUNT(*) FROM sighting WHERE {field} IS NOT NULL AND {field} != ''"
    )
    cnt = cur.fetchone()[0]
    pct = cnt / total * 100
    print(f"    {label:<20} {cnt:>10,}  ({pct:5.1f}%)")

cur.execute(
    "SELECT COUNT(*) FROM location WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
)
geo = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM location")
total_loc = cur.fetchone()[0]
print(f"\n  Geocoded locations:   {geo:,} / {total_loc:,} ({geo/total_loc*100:.1f}%)")

# Top 10 shapes
print(f"\n  Top 10 shapes:")
cur.execute("""
    SELECT shape, COUNT(*) as cnt
    FROM sighting WHERE shape IS NOT NULL AND shape != ''
    GROUP BY shape ORDER BY cnt DESC LIMIT 10
""")
for shape, cnt in cur.fetchall():
    print(f"    {shape:<20} {cnt:>8,}")

# Top 10 Hynek classifications
print(f"\n  Top 10 Hynek classifications:")
cur.execute("""
    SELECT hynek, COUNT(*) as cnt
    FROM sighting WHERE hynek IS NOT NULL AND hynek != ''
    GROUP BY hynek ORDER BY cnt DESC LIMIT 10
""")
for h, cnt in cur.fetchall():
    print(f"    {h:<20} {cnt:>8,}")

# Date distribution by decade
print(f"\n  Sightings by decade:")
cur.execute("""
    SELECT
        SUBSTR(date_event, 1, 3) || '0s' as decade,
        COUNT(*) as cnt
    FROM sighting
    WHERE date_event IS NOT NULL
      AND LENGTH(date_event) >= 4
      AND CAST(SUBSTR(date_event, 1, 4) AS INTEGER) >= 1900
    GROUP BY SUBSTR(date_event, 1, 3)
    ORDER BY decade
""")
for decade, cnt in cur.fetchall():
    bar = "#" * int(cnt / 1000)
    print(f"    {decade:<10} {cnt:>8,}  {bar}")

size = os.path.getsize(DB_PATH)
print(f"\n  Database size: {size / (1024*1024):.1f} MB")
print(f"  Location: {DB_PATH}")
print("=" * 70)

conn.close()
