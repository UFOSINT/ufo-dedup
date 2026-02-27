"""
Geocode locations in the unified database using the GeoNames gazetteer.

Downloads and parses cities15000.txt (free, offline, no API limits) and
matches locations by city+state+country with decreasing specificity.

Usage:
    python geocode.py                    # Geocode all NULL lat/lng locations
    python geocode.py --download         # Download gazetteer first, then geocode
    python geocode.py --stats-only       # Just print current geocoding stats
"""
import sqlite3
import os
import sys
import csv
import zipfile
import urllib.request
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
GEODATA_DIR = os.path.join(os.path.dirname(__file__), "geodata")
GAZETTEER_PATH = os.path.join(GEODATA_DIR, "cities15000.txt")
GAZETTEER_URL = "http://download.geonames.org/export/dump/cities15000.zip"

BATCH_SIZE = 5000

# Country name/code normalization
COUNTRY_NORMALIZE = {
    "USA": "US", "UNITED STATES": "US", "AMERICA": "US",
    "UK": "GB", "UNITED KINGDOM": "GB", "ENGLAND": "GB", "SCOTLAND": "GB", "WALES": "GB",
    "CANADA": "CA", "AUSTRALIA": "AU",
    "GERMANY": "DE", "FRANCE": "FR", "SPAIN": "ES", "ITALY": "IT",
    "BRAZIL": "BR", "MEXICO": "MX", "JAPAN": "JP", "CHINA": "CN",
    "INDIA": "IN", "RUSSIA": "RU", "SOUTH AFRICA": "ZA",
    "NETHERLANDS": "NL", "BELGIUM": "BE", "SWEDEN": "SE", "NORWAY": "NO",
    "DENMARK": "DK", "FINLAND": "FI", "POLAND": "PL", "IRELAND": "IE",
    "NEW ZEALAND": "NZ", "ARGENTINA": "AR", "CHILE": "CL",
    "PORTUGAL": "PT", "GREECE": "GR", "TURKEY": "TR", "ISRAEL": "IL",
    "PHILIPPINES": "PH", "INDONESIA": "ID", "MALAYSIA": "MY",
    "SOUTH KOREA": "KR", "COLOMBIA": "CO", "PERU": "PE",
    "PUERTO RICO": "PR", "AUSTRIA": "AT", "SWITZERLAND": "CH",
    "CZECH REPUBLIC": "CZ", "ROMANIA": "RO", "HUNGARY": "HU",
    "UKRAINE": "UA", "THAILAND": "TH", "VIETNAM": "VN",
    "SINGAPORE": "SG", "EGYPT": "EG", "PAKISTAN": "PK",
    "NIGERIA": "NG", "KENYA": "KE", "COSTA RICA": "CR",
    "PANAMA": "PA", "CUBA": "CU", "JAMAICA": "JM",
}

# Canadian province abbreviations to GeoNames admin1 codes
CA_PROVINCE_MAP = {
    "AB": "01", "BC": "02", "MB": "03", "NB": "04", "NL": "05",
    "NS": "07", "NT": "13", "NU": "14", "ON": "08", "PE": "09",
    "QC": "10", "SK": "11", "YT": "12",
}


def download_gazetteer():
    """Download and extract the GeoNames gazetteer."""
    os.makedirs(GEODATA_DIR, exist_ok=True)
    zip_path = os.path.join(GEODATA_DIR, "cities15000.zip")

    if os.path.exists(GAZETTEER_PATH):
        print(f"Gazetteer already exists: {GAZETTEER_PATH}")
        return

    print(f"Downloading {GAZETTEER_URL}...")
    urllib.request.urlretrieve(GAZETTEER_URL, zip_path)
    print(f"Extracting to {GEODATA_DIR}...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(GEODATA_DIR)
    os.remove(zip_path)
    print(f"Gazetteer ready: {GAZETTEER_PATH}")


def load_gazetteer():
    """Load GeoNames gazetteer into lookup dictionaries.

    Returns three dicts for decreasing specificity:
        exact:   (CITY, STATE, COUNTRY) -> (lat, lng, population)
        nostate: (CITY, COUNTRY) -> [(lat, lng, population), ...]
        cityonly: CITY -> [(lat, lng, population, country), ...]
    """
    if not os.path.exists(GAZETTEER_PATH):
        print(f"ERROR: Gazetteer not found at {GAZETTEER_PATH}")
        print("  Run: python geocode.py --download")
        sys.exit(1)

    exact = {}      # (city, admin1, country) -> (lat, lng, pop)
    nostate = defaultdict(list)   # (city, country) -> [(lat, lng, pop), ...]
    cityonly = defaultdict(list)  # city -> [(lat, lng, pop, country), ...]

    with open(GAZETTEER_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) < 15:
                continue

            name = row[1].strip().upper()       # name
            asciiname = row[2].strip().upper()   # asciiname
            lat = float(row[4])
            lng = float(row[5])
            country = row[8].strip().upper()     # country_code
            admin1 = row[10].strip().upper()     # admin1_code (state)
            try:
                pop = int(row[14])
            except (ValueError, IndexError):
                pop = 0

            # Also index alternate names for better matching
            alt_names = set()
            alt_names.add(name)
            alt_names.add(asciiname)
            if row[3]:  # alternatenames
                for alt in row[3].split(","):
                    alt = alt.strip().upper()
                    if alt and len(alt) > 1 and alt.isascii():
                        alt_names.add(alt)

            for city_name in alt_names:
                key_exact = (city_name, admin1, country)
                if key_exact not in exact or pop > exact[key_exact][2]:
                    exact[key_exact] = (lat, lng, pop)

                nostate[(city_name, country)].append((lat, lng, pop))
                cityonly[city_name].append((lat, lng, pop, country))

    # Sort nostate and cityonly by population (largest first)
    for key in nostate:
        nostate[key].sort(key=lambda x: x[2], reverse=True)
    for key in cityonly:
        cityonly[key].sort(key=lambda x: x[2], reverse=True)

    print(f"Gazetteer loaded: {len(exact):,} exact keys, "
          f"{len(nostate):,} city+country keys, {len(cityonly):,} city-only keys")
    return exact, nostate, cityonly


def normalize_country(raw):
    """Normalize country name/code to 2-letter ISO code."""
    if not raw:
        return None
    raw = raw.strip().upper()
    if len(raw) == 2:
        return raw
    return COUNTRY_NORMALIZE.get(raw, raw)


def normalize_state(state, country):
    """Normalize state/admin1 code for gazetteer lookup."""
    if not state:
        return None
    state = state.strip().upper()
    # For Canada, convert province abbreviations to GeoNames admin1 codes
    if country == "CA" and state in CA_PROVINCE_MAP:
        return CA_PROVINCE_MAP[state]
    return state


def geocode_location(city, state, country, raw_text, exact, nostate, cityonly):
    """Try to geocode a location with decreasing specificity.

    Returns (lat, lng, method) or (None, None, None).
    """
    if not city and not raw_text:
        return None, None, None

    city_upper = city.strip().upper() if city else None
    country_code = normalize_country(country)
    state_code = normalize_state(state, country_code)

    # Strategy 1: Exact match (city + state + country)
    if city_upper and state_code and country_code:
        key = (city_upper, state_code, country_code)
        if key in exact:
            lat, lng, _ = exact[key]
            return lat, lng, "exact"

    # Strategy 2: City + country (no state)
    if city_upper and country_code:
        key = (city_upper, country_code)
        if key in nostate:
            lat, lng, _ = nostate[key][0]  # Largest city by population
            return lat, lng, "city_country"

    # Strategy 3: City only (pick largest globally)
    if city_upper and city_upper not in ("UNKNOWN", "UNKNOWN CITY", "N/A", ""):
        if city_upper in cityonly:
            lat, lng, _, _ = cityonly[city_upper][0]  # Largest by population
            return lat, lng, "city_only"

    # Strategy 4: Parse raw_text for locations like "ITALY, ROME" or "City, ST"
    if raw_text and not city_upper:
        parsed_city, parsed_state, parsed_country = parse_raw_location(raw_text)
        if parsed_city:
            return geocode_location(
                parsed_city, parsed_state, parsed_country, None,
                exact, nostate, cityonly
            )

    return None, None, None


def parse_raw_location(raw_text):
    """Try to parse a raw location string into (city, state, country).

    Handles formats like:
        "ITALY, ROME"  -> city=ROME, country=IT
        "Rome, Italy"  -> city=ROME, country=IT
        "Houston, TX"  -> city=HOUSTON, state=TX
        "China"        -> country=CN
    """
    import re

    if not raw_text or not raw_text.strip():
        return None, None, None

    text = raw_text.strip()
    parts = [p.strip() for p in text.split(",")]

    if len(parts) == 1:
        # Single word — might be a country
        country = normalize_country(parts[0])
        if country and len(country) == 2:
            return None, None, country
        return None, None, None

    if len(parts) == 2:
        a, b = parts[0].strip(), parts[1].strip()

        # Check if first part is a country (e.g., "ITALY, ROME")
        country_a = normalize_country(a)
        if country_a and len(country_a) == 2 and len(a) > 2:
            return b, None, country_a

        # Check if second part is a country (e.g., "Rome, Italy")
        country_b = normalize_country(b)
        if country_b and len(country_b) == 2 and len(b) > 2:
            return a, None, country_b

        # Check if second part is a US state code (e.g., "Houston, TX")
        if re.match(r'^[A-Z]{2}$', b.upper()):
            return a, b.upper(), "US"

        # Check if second part is a 2-letter country code
        if len(b) == 2:
            return a, None, b.upper()

        return a, None, None

    if len(parts) == 3:
        # city, state, country
        return parts[0], parts[1], normalize_country(parts[2])

    return None, None, None


def run_geocoding(db_path=DB_PATH):
    """Geocode all locations with NULL coordinates."""
    exact, nostate, cityonly = load_gazetteer()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # Get all locations needing geocoding
    cur.execute("""
        SELECT id, city, state, country, raw_text
        FROM location
        WHERE latitude IS NULL AND longitude IS NULL
    """)
    locations = cur.fetchall()
    total = len(locations)
    print(f"\nLocations to geocode: {total:,}")

    geocoded = 0
    by_method = defaultdict(int)
    updates = []

    for i, (loc_id, city, state, country, raw_text) in enumerate(locations):
        lat, lng, method = geocode_location(
            city, state, country, raw_text,
            exact, nostate, cityonly
        )

        if lat is not None:
            geocode_src = f"geonames_{method}"
            updates.append((lat, lng, geocode_src, loc_id))
            geocoded += 1
            by_method[method] += 1

        if len(updates) >= BATCH_SIZE:
            cur.executemany(
                "UPDATE location SET latitude = ?, longitude = ?, geocode_src = ? WHERE id = ?",
                updates
            )
            conn.commit()
            updates = []
            print(f"  ... {i + 1:,}/{total:,} processed, {geocoded:,} geocoded", end="\r")

    # Final batch
    if updates:
        cur.executemany(
            "UPDATE location SET latitude = ?, longitude = ?, geocode_src = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print(f"\n\nGeocoding complete:")
    print(f"  Total locations processed: {total:,}")
    print(f"  Successfully geocoded:     {geocoded:,} ({100 * geocoded / total:.1f}%)")
    print(f"  Not matched:               {total - geocoded:,}")
    print(f"\n  By method:")
    for method, count in sorted(by_method.items(), key=lambda x: -x[1]):
        print(f"    {method:20s} {count:>8,}")

    # Print per-source stats
    print(f"\n  Geocoded sightings by source:")
    cur.execute("""
        SELECT sd.name,
               COUNT(*) as total,
               SUM(CASE WHEN l.latitude IS NOT NULL THEN 1 ELSE 0 END) as geocoded
        FROM sighting s
        JOIN source_database sd ON s.source_db_id = sd.id
        JOIN location l ON s.location_id = l.id
        GROUP BY sd.name
        ORDER BY total DESC
    """)
    for name, total_s, geo_s in cur.fetchall():
        pct = 100 * geo_s / total_s if total_s > 0 else 0
        print(f"    {name:12s}  {geo_s:>8,} / {total_s:>8,}  ({pct:.1f}%)")

    conn.close()


def print_stats(db_path=DB_PATH):
    """Print current geocoding statistics."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM location")
    total_locs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM location WHERE latitude IS NOT NULL")
    geocoded_locs = cur.fetchone()[0]

    print(f"Locations: {geocoded_locs:,} / {total_locs:,} geocoded "
          f"({100 * geocoded_locs / total_locs:.1f}%)")

    print(f"\nBy source:")
    cur.execute("""
        SELECT sd.name,
               COUNT(*) as total,
               SUM(CASE WHEN l.latitude IS NOT NULL THEN 1 ELSE 0 END) as geocoded
        FROM sighting s
        JOIN source_database sd ON s.source_db_id = sd.id
        JOIN location l ON s.location_id = l.id
        GROUP BY sd.name
        ORDER BY total DESC
    """)
    for name, total_s, geo_s in cur.fetchall():
        pct = 100 * geo_s / total_s if total_s > 0 else 0
        print(f"  {name:12s}  {geo_s:>8,} / {total_s:>8,}  ({pct:.1f}%)")

    conn.close()


if __name__ == "__main__":
    if "--download" in sys.argv:
        download_gazetteer()

    if "--stats-only" in sys.argv:
        print_stats()
    else:
        run_geocoding()
