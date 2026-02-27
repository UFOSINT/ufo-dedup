"""
Deduplication script for the unified UFO sightings database.

Identifies duplicate sighting pairs across sources using a tiered strategy:
  Tier 1: MUFON <-> NUFORC (date + city + state)
  Tier 2: All remaining cross-source pairs (date + location)
  Tier 3: Description fuzzy matching for same-date cross-source records

Note: Tier 1b (UFOCAT-UFOReportCtr <-> NUFORC) was removed because
import_ufocat.py now skips UFOReportCtr records at import time.

Usage:
  python dedup.py              # Run all tiers
  python dedup.py --tier 1     # Run only tier 1
  python dedup.py --tier verify # Just print the verification report
"""
import sqlite3
import difflib
import re
import time
import argparse
import os
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ufo_unified.db")

# Source DB IDs (from source_database table)
SRC_MUFON = 1
SRC_NUFORC = 2
SRC_UFOCAT = 3
SRC_UPDB = 4
SRC_UFOSEARCH = 5  # was Geldreich, renamed to UFO-search

BATCH_SIZE = 5000

# US state abbreviations for UFO-search location parsing
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU', 'AS', 'MP',
    # Canadian provinces
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
}


# ============================================================
# SIMILARITY / NORMALIZATION FUNCTIONS
# ============================================================

def strip_nuforc_prefix(desc):
    """Remove 'NUFORC UFO Sighting NNNNN' prefix from NUFORC descriptions."""
    if not desc:
        return desc
    if desc.startswith('NUFORC UFO Sighting'):
        return re.sub(r'^NUFORC UFO Sighting \d+\s*', '', desc).strip()
    return desc


def strip_mufon_boilerplate(desc):
    """Remove common MUFON submission template text."""
    if not desc:
        return desc
    if 'Submitted by razor via e-mail' in desc[:60]:
        m = re.search(r'Investigators?\s*Not(?:es?)?[.:,]?\s*(.+)', desc, re.DOTALL)
        return m.group(1).strip() if m else desc
    return desc


def token_jaccard(a, b):
    """Fast token-level Jaccard similarity. Returns 0.0-1.0."""
    if not a or not b:
        return 0.0
    a_tokens = set(re.findall(r'\w+', a.lower()))
    b_tokens = set(re.findall(r'\w+', b.lower()))
    if not a_tokens or not b_tokens:
        return 0.0
    intersection = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return intersection / union


def compute_similarity(desc_a, desc_b, source_a=None, source_b=None):
    """
    Compute similarity score between two descriptions.
    Handles source-specific preprocessing.
    Returns float 0.0-1.0.
    """
    if not desc_a or not desc_b:
        return 0.0

    a = desc_a
    b = desc_b

    # Source-specific preprocessing
    if source_a == SRC_NUFORC:
        a = strip_nuforc_prefix(a)
    if source_b == SRC_NUFORC:
        b = strip_nuforc_prefix(b)
    if source_a == SRC_MUFON:
        a = strip_mufon_boilerplate(a)
    if source_b == SRC_MUFON:
        b = strip_mufon_boilerplate(b)

    if not a or not b:
        return 0.0

    # Quick "starts with" check (common for UFOCAT<->NUFORC copied descriptions)
    a_norm = a.strip().lower()
    b_norm = b.strip().lower()
    shorter = min(len(a_norm), len(b_norm))
    if shorter >= 20:  # Need at least 20 chars for meaningful starts-with
        if b_norm.startswith(a_norm[:shorter]) or a_norm.startswith(b_norm[:shorter]):
            return 0.95

    # Token Jaccard as fast filter
    jaccard = token_jaccard(a, b)
    if jaccard < 0.03:
        return jaccard

    # Full SequenceMatcher for decent candidates
    return difflib.SequenceMatcher(None, a[:1000], b[:1000]).ratio()


def normalize_city(city_str):
    """Normalize a city name for matching."""
    if not city_str:
        return ''
    c = city_str.strip().upper()
    # Remove parenthetical qualifiers
    c = re.sub(r'\s*\(.*\)\s*$', '', c)
    # Remove trailing punctuation/question marks
    c = re.sub(r'[\?\.\!]+$', '', c)
    # Collapse whitespace
    c = re.sub(r'\s+', ' ', c).strip()
    return c


def parse_ufosearch_city_state(raw_text):
    """Extract (city, state) from UFO-search free-text locations."""
    if not raw_text:
        return None, None
    m = re.match(r'^(.+?),\s*([A-Z]{2})\s*\??$', raw_text.strip(), re.I)
    if m and m.group(2).upper() in US_STATES:
        return m.group(1).strip().upper(), m.group(2).upper()
    return None, None


# ============================================================
# DATA LOADING HELPERS
# ============================================================

def load_source_sightings(conn, source_db_id, use_raw_text_as_city=False,
                          source_ref_filter=None, extra_where=None):
    """
    Load sightings with location data for a given source.
    Returns dict: {(date, city_norm, state_norm): [(sighting_id, description)]}

    For UFOCAT (source_db_id=3), city comes from raw_text.
    For others, city comes from the city column.
    """
    where_clauses = ["s.source_db_id = ?", "s.date_event IS NOT NULL"]
    params = [source_db_id]

    if source_ref_filter:
        where_clauses.append("s.source_ref = ?")
        params.append(source_ref_filter)

    if extra_where:
        where_clauses.append(extra_where)

    city_col = "l.raw_text" if use_raw_text_as_city else "l.city"

    sql = f"""
        SELECT s.id, SUBSTR(s.date_event, 1, 10) as d,
               {city_col}, l.state, s.description
        FROM sighting s
        JOIN location l ON s.location_id = l.id
        WHERE {' AND '.join(where_clauses)}
    """

    cur = conn.cursor()
    cur.execute(sql, params)

    groups = defaultdict(list)
    count = 0
    for sid, d, city, state, desc in cur:
        city_n = normalize_city(city)
        state_n = (state or '').strip().upper()
        if not d or not city_n:
            continue
        key = (d, city_n, state_n)
        groups[key].append((sid, desc))
        count += 1

    return groups, count


def load_source_sightings_city_only(conn, source_db_id, country_filter=None):
    """
    Load sightings keyed by (date, city_norm) only — no state.
    Used for UPDB which has no state field.
    """
    where_clauses = ["s.source_db_id = ?", "s.date_event IS NOT NULL"]
    params = [source_db_id]

    if country_filter:
        where_clauses.append("l.country = ?")
        params.append(country_filter)

    sql = f"""
        SELECT s.id, SUBSTR(s.date_event, 1, 10) as d,
               l.city, s.description, s.source_db_id
        FROM sighting s
        JOIN location l ON s.location_id = l.id
        WHERE {' AND '.join(where_clauses)}
          AND l.city IS NOT NULL AND TRIM(l.city) != ''
    """
    cur = conn.cursor()
    cur.execute(sql, params)

    groups = defaultdict(list)
    count = 0
    for sid, d, city, desc, src_id in cur:
        city_n = normalize_city(city)
        if not d or not city_n:
            continue
        key = (d, city_n)
        groups[key].append((sid, desc, src_id))
        count += 1

    return groups, count


def insert_candidates(conn, candidates):
    """Batch insert duplicate candidates. candidates is list of tuples:
    (sighting_id_a, sighting_id_b, similarity_score, match_method, status)
    Always ensures a < b for the UNIQUE constraint.
    """
    if not candidates:
        return 0

    # Normalize: ensure a < b
    normalized = []
    for a, b, score, method, status in candidates:
        if a == b:
            continue
        lo, hi = min(a, b), max(a, b)
        normalized.append((lo, hi, score, method, status))

    cur = conn.cursor()
    cur.executemany("""
        INSERT OR IGNORE INTO duplicate_candidate
        (sighting_id_a, sighting_id_b, similarity_score, match_method, status)
        VALUES (?, ?, ?, ?, ?)
    """, normalized)
    conn.commit()
    return cur.rowcount


# ============================================================
# TIER IMPLEMENTATIONS
# ============================================================

def create_indexes(conn):
    """Create composite indexes for efficient matching."""
    print("Creating composite indexes...")
    t0 = time.time()
    cur = conn.cursor()
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_sighting_source_date ON sighting(source_db_id, date_event)",
        "CREATE INDEX IF NOT EXISTS idx_sighting_source_ref ON sighting(source_ref)",
        "CREATE INDEX IF NOT EXISTS idx_location_city_state ON location(city, state)",
    ]
    for sql in indexes:
        cur.execute(sql)
    conn.commit()
    print(f"  Indexes created in {time.time() - t0:.1f}s")


def tier_1a(conn):
    """Tier 1a: MUFON <-> NUFORC — date + city + state exact match."""
    print("\n" + "=" * 60)
    print("TIER 1a: MUFON <-> NUFORC (date + city + state)")
    print("=" * 60)
    t0 = time.time()

    # Load MUFON
    print("  Loading MUFON sightings...")
    mufon, mufon_count = load_source_sightings(conn, SRC_MUFON)
    print(f"    {mufon_count:,} records in {len(mufon):,} groups")

    # Load NUFORC
    print("  Loading NUFORC sightings...")
    nuforc, nuforc_count = load_source_sightings(conn, SRC_NUFORC)
    print(f"    {nuforc_count:,} records in {len(nuforc):,} groups")

    # Find overlapping keys
    overlap_keys = set(mufon.keys()) & set(nuforc.keys())
    print(f"  Overlapping (date, city, state) groups: {len(overlap_keys):,}")

    # Generate candidate pairs with scores
    candidates = []
    for key in overlap_keys:
        for m_id, m_desc in mufon[key]:
            for n_id, n_desc in nuforc[key]:
                score = compute_similarity(m_desc, n_desc, SRC_MUFON, SRC_NUFORC)
                candidates.append((m_id, n_id, score, 'tier1a_mufon_nuforc', 'pending'))

    print(f"  Total candidate pairs: {len(candidates):,}")

    # Insert
    inserted = insert_candidates(conn, candidates)
    elapsed = time.time() - t0
    print(f"  Inserted {inserted:,} new candidates in {elapsed:.1f}s")

    # Score summary
    if candidates:
        scores = [c[2] for c in candidates]
        high = sum(1 for s in scores if s >= 0.7)
        med = sum(1 for s in scores if 0.3 <= s < 0.7)
        low = sum(1 for s in scores if s < 0.3)
        print(f"  Score distribution: {high:,} high (>=0.7), {med:,} medium (0.3-0.7), {low:,} low (<0.3)")


def tier_2(conn):
    """Tier 2: All remaining cross-source pairs via date + location."""
    print("\n" + "=" * 60)
    print("TIER 2: All remaining cross-source pairs (date + location)")
    print("=" * 60)
    t0 = time.time()

    total_candidates = 0

    # --- 2a: MUFON <-> UFOCAT (city vs raw_text, both have state) ---
    print("\n  --- 2a: MUFON <-> UFOCAT ---")
    mufon, mc = load_source_sightings(conn, SRC_MUFON)
    print(f"    MUFON: {mc:,} records")
    # UFOReportCtr records no longer imported, so no exclusion needed
    ufocat, uc = load_source_sightings(
        conn, SRC_UFOCAT, use_raw_text_as_city=True
    )
    print(f"    UFOCAT: {uc:,} records")

    overlap = set(mufon.keys()) & set(ufocat.keys())
    print(f"    Overlapping groups: {len(overlap):,}")
    candidates = []
    for key in overlap:
        for m_id, m_desc in mufon[key]:
            for u_id, u_desc in ufocat[key]:
                score = compute_similarity(m_desc, u_desc, SRC_MUFON, SRC_UFOCAT)
                candidates.append((m_id, u_id, score, 'tier2a_mufon_ufocat', 'pending'))
    ins = insert_candidates(conn, candidates)
    total_candidates += len(candidates)
    print(f"    Pairs: {len(candidates):,}, inserted: {ins:,}")

    # Free memory
    del mufon, ufocat

    # --- 2b: NUFORC <-> UFOCAT ---
    print("\n  --- 2b: NUFORC <-> UFOCAT ---")
    nuforc, nc = load_source_sightings(conn, SRC_NUFORC)
    print(f"    NUFORC: {nc:,} records")
    # UFOReportCtr records no longer imported, so no exclusion needed
    ufocat2, uc2 = load_source_sightings(
        conn, SRC_UFOCAT, use_raw_text_as_city=True
    )
    print(f"    UFOCAT: {uc2:,} records")

    overlap = set(nuforc.keys()) & set(ufocat2.keys())
    print(f"    Overlapping groups: {len(overlap):,}")
    candidates = []
    for key in overlap:
        for n_id, n_desc in nuforc[key]:
            for u_id, u_desc in ufocat2[key]:
                score = compute_similarity(n_desc, u_desc, SRC_NUFORC, SRC_UFOCAT)
                candidates.append((n_id, u_id, score, 'tier2b_nuforc_ufocat', 'pending'))

        if len(candidates) >= 50000:
            insert_candidates(conn, candidates)
            candidates = []

    ins = insert_candidates(conn, candidates)
    total_candidates += len(candidates)
    print(f"    Pairs: {len(candidates):,}, inserted: {ins:,}")
    del nuforc, ufocat2

    # --- 2c: UPDB <-> other sources (city-only matching, no state) ---
    print("\n  --- 2c: UPDB <-> MUFON/NUFORC/UFOCAT (city only) ---")

    # UPDB uses 'US' for country, NUFORC uses 'USA', MUFON uses 'US'
    # Load UPDB for US records
    updb_us, updb_count = load_source_sightings_city_only(conn, SRC_UPDB, country_filter='US')
    print(f"    UPDB (US): {updb_count:,} records in {len(updb_us):,} groups")

    # Load other sources keyed by (date, city) only for matching
    for other_src, other_name, other_raw in [
        (SRC_MUFON, 'MUFON', False),
        (SRC_NUFORC, 'NUFORC', False),
    ]:
        other, oc = load_source_sightings(conn, other_src, use_raw_text_as_city=other_raw)
        # Convert to (date, city) keys (dropping state)
        other_city = defaultdict(list)
        for (d, city, state), items in other.items():
            other_city[(d, city)].extend(items)

        overlap = set(updb_us.keys()) & set(other_city.keys())
        candidates = []
        for key in overlap:
            for u_id, u_desc, _ in updb_us[key]:
                for o_id, o_desc in other_city[key]:
                    score = compute_similarity(u_desc, o_desc, SRC_UPDB, other_src)
                    candidates.append((u_id, o_id, score, f'tier2c_updb_{other_name.lower()}', 'pending'))

        ins = insert_candidates(conn, candidates)
        total_candidates += len(candidates)
        print(f"    UPDB <-> {other_name}: {len(candidates):,} pairs, {ins:,} inserted")
        del other, other_city

    # UPDB <-> UFOCAT (city only)
    ufocat_city = defaultdict(list)
    ufocat3, uc3 = load_source_sightings(conn, SRC_UFOCAT, use_raw_text_as_city=True)
    for (d, city, state), items in ufocat3.items():
        ufocat_city[(d, city)].extend(items)

    overlap = set(updb_us.keys()) & set(ufocat_city.keys())
    candidates = []
    for key in overlap:
        for u_id, u_desc, _ in updb_us[key]:
            for o_id, o_desc in ufocat_city[key]:
                score = compute_similarity(u_desc, o_desc, SRC_UPDB, SRC_UFOCAT)
                candidates.append((u_id, o_id, score, 'tier2c_updb_ufocat', 'pending'))

        if len(candidates) >= 50000:
            insert_candidates(conn, candidates)
            candidates = []

    ins = insert_candidates(conn, candidates)
    total_candidates += len(candidates)
    print(f"    UPDB <-> UFOCAT: {len(candidates):,} pairs, {ins:,} inserted")
    del updb_us, ufocat3, ufocat_city

    # --- 2d: UFO-search <-> other sources ---
    print("\n  --- 2d: UFO-search <-> other sources ---")
    # Load UFO-search and parse city/state from raw_text
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id, SUBSTR(s.date_event, 1, 10) as d,
               l.raw_text, s.description
        FROM sighting s
        JOIN location l ON s.location_id = l.id
        WHERE s.source_db_id = ? AND s.date_event IS NOT NULL
    """, (SRC_UFOSEARCH,))

    geld_groups = defaultdict(list)
    geld_count = 0
    for sid, d, raw, desc in cur:
        city, state = parse_ufosearch_city_state(raw or '')
        if city and d:
            geld_groups[(d, city, state or '')].append((sid, desc))
            geld_count += 1
    print(f"    UFO-search (parseable locations): {geld_count:,} in {len(geld_groups):,} groups")

    # Match against each other source
    for other_src, other_name, other_raw in [
        (SRC_MUFON, 'MUFON', False),
        (SRC_NUFORC, 'NUFORC', False),
        (SRC_UFOCAT, 'UFOCAT', True),
    ]:
        other, oc = load_source_sightings(conn, other_src, use_raw_text_as_city=other_raw)
        overlap = set(geld_groups.keys()) & set(other.keys())
        candidates = []
        for key in overlap:
            for g_id, g_desc in geld_groups[key]:
                for o_id, o_desc in other[key]:
                    score = compute_similarity(g_desc, o_desc, SRC_UFOSEARCH, other_src)
                    candidates.append((g_id, o_id, score, f'tier2d_ufosearch_{other_name.lower()}', 'pending'))

        ins = insert_candidates(conn, candidates)
        total_candidates += len(candidates)
        print(f"    UFO-search <-> {other_name}: {len(candidates):,} pairs, {ins:,} inserted")
        del other

    elapsed = time.time() - t0
    print(f"\n  Tier 2 total: {total_candidates:,} candidate pairs in {elapsed:.1f}s")


def tier_3(conn):
    """Tier 3: Description fuzzy matching for same-date cross-source records
    not already caught by location matching.

    Optimized: only processes dates with <= 20 total records to keep
    the pair space manageable. Loads all data in bulk first.
    """
    print("\n" + "=" * 60)
    print("TIER 3: Description fuzzy matching (date-only, cross-source)")
    print("=" * 60)
    t0 = time.time()

    cur = conn.cursor()

    # Find target dates: multi-source, small groups only (<=20 records)
    cur.execute("""
        SELECT SUBSTR(date_event, 1, 10) as d,
               COUNT(*) as cnt,
               COUNT(DISTINCT source_db_id) as src_cnt
        FROM sighting
        WHERE date_event IS NOT NULL
          AND LENGTH(date_event) >= 10
        GROUP BY d
        HAVING src_cnt >= 2 AND cnt <= 20
        ORDER BY d
    """)
    target_dates = {d for d, cnt, sc in cur.fetchall()}
    print(f"  Target dates (multi-source, <= 20 records): {len(target_dates):,}")

    # Load existing candidate pairs to skip
    cur.execute("SELECT sighting_id_a, sighting_id_b FROM duplicate_candidate")
    existing_pairs = set()
    for a, b in cur:
        existing_pairs.add((min(a, b), max(a, b)))
    print(f"  Existing pairs to skip: {len(existing_pairs):,}")

    # Bulk load all sightings for target dates into memory
    print("  Loading sightings for target dates...")
    cur.execute("""
        SELECT s.id, s.source_db_id, SUBSTR(s.date_event, 1, 10) as d, s.description
        FROM sighting s
        WHERE s.date_event IS NOT NULL
          AND LENGTH(s.date_event) >= 10
    """)

    # Group by (date, source)
    date_source_groups = defaultdict(lambda: defaultdict(list))
    loaded = 0
    for sid, src_id, d, desc in cur:
        if d in target_dates:
            date_source_groups[d][src_id].append((sid, desc))
            loaded += 1
    print(f"  Loaded {loaded:,} sightings across {len(date_source_groups):,} dates")

    candidates = []
    dates_processed = 0
    pairs_compared = 0
    found = 0

    for d, by_source in date_source_groups.items():
        source_ids = sorted(by_source.keys())

        # Cross-source pairs only
        for i in range(len(source_ids)):
            for j in range(i + 1, len(source_ids)):
                src_a = source_ids[i]
                src_b = source_ids[j]
                for a_id, a_desc in by_source[src_a]:
                    for b_id, b_desc in by_source[src_b]:
                        lo, hi = min(a_id, b_id), max(a_id, b_id)
                        if (lo, hi) in existing_pairs:
                            continue

                        pairs_compared += 1

                        # Preprocess
                        a_clean = strip_nuforc_prefix(strip_mufon_boilerplate(a_desc or ''))
                        b_clean = strip_nuforc_prefix(strip_mufon_boilerplate(b_desc or ''))

                        # Quick Jaccard filter
                        jac = token_jaccard(a_clean, b_clean)
                        if jac < 0.25:
                            continue

                        score = compute_similarity(a_desc, b_desc, src_a, src_b)
                        if score >= 0.5:
                            candidates.append((a_id, b_id, score, 'tier3_desc_fuzzy', 'pending'))
                            found += 1

        dates_processed += 1
        if dates_processed % 5000 == 0:
            if candidates:
                insert_candidates(conn, candidates)
                candidates = []
            print(f"    ... {dates_processed:,}/{len(date_source_groups):,} dates, "
                  f"{pairs_compared:,} pairs, {found:,} found", end='\r')

    # Final insert
    if candidates:
        insert_candidates(conn, candidates)

    elapsed = time.time() - t0
    print(f"\n  Dates processed: {dates_processed:,}")
    print(f"  Cross-source pairs compared: {pairs_compared:,}")
    print(f"  Tier 3 candidates found: {found:,}")
    print(f"  Completed in {elapsed:.1f}s")


def verify(conn):
    """Print verification report."""
    print("\n" + "=" * 60)
    print("VERIFICATION REPORT")
    print("=" * 60)

    cur = conn.cursor()

    # Total candidates
    cur.execute("SELECT COUNT(*) FROM duplicate_candidate")
    total = cur.fetchone()[0]
    print(f"\n  Total duplicate candidates: {total:,}")

    # By method
    print("\n  By match method:")
    cur.execute("""
        SELECT match_method, COUNT(*),
               ROUND(AVG(similarity_score), 3),
               ROUND(MIN(similarity_score), 3),
               ROUND(MAX(similarity_score), 3)
        FROM duplicate_candidate
        GROUP BY match_method
        ORDER BY COUNT(*) DESC
    """)
    print(f"    {'Method':<30} {'Count':>8}  {'Avg':>6}  {'Min':>6}  {'Max':>6}")
    print(f"    {'-'*30} {'-'*8}  {'-'*6}  {'-'*6}  {'-'*6}")
    for method, cnt, avg, mn, mx in cur.fetchall():
        print(f"    {method:<30} {cnt:>8,}  {avg:>6.3f}  {mn:>6.3f}  {mx:>6.3f}")

    # Score distribution
    print("\n  Score distribution:")
    buckets = [
        ("0.9 - 1.0  (certain duplicates)", 0.9, 1.01),
        ("0.7 - 0.9  (likely duplicates)", 0.7, 0.9),
        ("0.5 - 0.7  (possible duplicates)", 0.5, 0.7),
        ("0.3 - 0.5  (weak matches)", 0.3, 0.5),
        ("0.0 - 0.3  (unlikely matches)", 0.0, 0.3),
    ]
    for label, lo, hi in buckets:
        cur.execute(
            "SELECT COUNT(*) FROM duplicate_candidate WHERE similarity_score >= ? AND similarity_score < ?",
            (lo, hi)
        )
        cnt = cur.fetchone()[0]
        pct = cnt / total * 100 if total else 0
        bar = "#" * int(pct / 2)
        print(f"    {label}  {cnt:>8,}  ({pct:5.1f}%)  {bar}")

    # Sample high-confidence pairs
    print("\n  Top 10 highest-confidence pairs:")
    cur.execute("""
        SELECT dc.similarity_score, dc.match_method,
               sda.name, sa.date_event, la.raw_text,
               SUBSTR(sa.description, 1, 60),
               sdb.name, sb.date_event, lb.raw_text,
               SUBSTR(sb.description, 1, 60)
        FROM duplicate_candidate dc
        JOIN sighting sa ON dc.sighting_id_a = sa.id
        JOIN sighting sb ON dc.sighting_id_b = sb.id
        JOIN source_database sda ON sa.source_db_id = sda.id
        JOIN source_database sdb ON sb.source_db_id = sdb.id
        LEFT JOIN location la ON sa.location_id = la.id
        LEFT JOIN location lb ON sb.location_id = lb.id
        ORDER BY dc.similarity_score DESC
        LIMIT 10
    """)
    for i, row in enumerate(cur.fetchall()):
        score, method = row[0], row[1]
        src_a, date_a, loc_a, desc_a = row[2], row[3], row[4], row[5]
        src_b, date_b, loc_b, desc_b = row[6], row[7], row[8], row[9]
        print(f"\n    #{i+1} Score: {score:.3f} ({method})")
        print(f"      A [{src_a}] {date_a}  {(loc_a or '')[:40]}")
        print(f"        {(desc_a or '')[:70]}...")
        print(f"      B [{src_b}] {date_b}  {(loc_b or '')[:40]}")
        print(f"        {(desc_b or '')[:70]}...")

    # Records involved in duplicates
    cur.execute("""
        SELECT COUNT(DISTINCT id) FROM (
            SELECT sighting_id_a as id FROM duplicate_candidate
            UNION ALL
            SELECT sighting_id_b as id FROM duplicate_candidate
        )
    """)
    involved = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sighting")
    all_sightings = cur.fetchone()[0]
    print(f"\n  Unique sightings involved in duplicates: {involved:,} / {all_sightings:,} ({involved/all_sightings*100:.1f}%)")

    print("\n" + "=" * 60)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="UFO Database Deduplication")
    parser.add_argument('--tier', choices=['1', '2', '3', 'all', 'verify'],
                        default='all', help='Which tier to run (default: all)')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-200000")  # 200MB cache

    overall_t0 = time.time()

    if args.tier in ('1', '2', '3', 'all'):
        create_indexes(conn)

    if args.tier in ('1', 'all'):
        tier_1a(conn)
    if args.tier in ('2', 'all'):
        tier_2(conn)
    if args.tier in ('3', 'all'):
        tier_3(conn)
    if args.tier in ('verify', 'all'):
        verify(conn)

    if args.tier == 'all':
        print(f"\nTotal elapsed: {time.time() - overall_t0:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
