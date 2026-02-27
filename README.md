# Unified UFO Sightings Database

A unified SQLite database merging five major UFO/UAP sighting databases into a single, deduplicated repository of **614,505 sighting records** spanning from antiquity to 2026, with **126,730 duplicate candidate pairs** flagged for review.

## Source Databases

| Source | Format | Raw Records | Imported | Skipped | Columns | Description |
|--------|--------|-------------|----------|---------|---------|-------------|
| **UFOCAT** | CSV (87 MB) | 320,412 | 197,108 | 123,304 | 55 | CUFOS UFOCAT 2023 catalog. Richest metadata: Hynek/Vallee classifications, lat/lon, witness counts, durations. 123K records with `SOURCE=UFOReportCtr` (NUFORC-origin) are skipped at import and their metadata transferred to NUFORC via enrichment. |
| **NUFORC** | CSV (181 MB) | 159,320 | 159,320 | 0 | 18 | National UFO Reporting Center. Self-reported sightings with detailed free-text descriptions. **Enriched** post-import with 102K Hynek and 83K Vallee classifications from UFOCAT. |
| **MUFON** | CSV (162 MB) | 138,310 | 138,310 | 0 | 7 | Mutual UFO Network case reports. Short + long descriptions, investigator summaries. |
| **UPDB** | CSV (280 MB) | 1,885,757 | 65,016 | 1,820,741 | 9 | Unified Phenomena Database (phenomenAInon). **1.82M rows skipped** (MUFON/NUFORC already imported from richer originals). Remaining 65K come from UFODNA (38K), Blue Book (14K), NICAP (5.8K), and 7 other origins. |
| **UFO-search** | JSON (72 MB) | 54,751 | 54,751 | 0 | 20 | Majestic Timeline compilation from ufo-search.com. Historical records from 19 source compilations (Hatch, Eberhart, NICAP, Vallee, etc.). |

**Total raw records across all sources: ~2.56 million**
**After removing known overlaps at import time: 614,505**

### Why UFOCAT Skips UFOReportCtr Records

UFOCAT's `SOURCE` column identifies where each record originated. 123,304 records have `SOURCE=UFOReportCtr`, meaning they were copied from NUFORC. Since we import NUFORC directly (with richer descriptions), importing these again would create duplicates.

However, UFOCAT adds valuable metadata that NUFORC lacks:
- **Hynek classification**: 123,116 of the UFOReportCtr records (99.8%) have Hynek codes
- **Vallee classification**: 99,618 (80.8%) have Vallee codes
- **NUFORC has 0 Hynek/Vallee codes natively**

The `enrich.py` script transfers these classifications to the matching NUFORC records by date+city+state matching, preserving the metadata without creating duplicate sightings.

### UFOCAT Sub-Source Landscape

UFOCAT is itself an aggregator. Its top `SOURCE` values show the overlap with other databases:

| UFOCAT SOURCE | Records | Overlap With |
|---------------|---------|--------------|
| UFOReportCtr | 123,304 | NUFORC (skipped, enriched) |
| U (Hatch) | 17,184 | UFO-search Hatch (18K) |
| BlueBook1 | 13,101 | UPDB Blue Book (14K) |
| GEberhart1 | 11,643 | UFO-search Eberhart (7.9K) |
| CanadUFOSurv | 10,785 | — |
| NICAP | 2,315 | UPDB NICAP (5.8K), UFO-search NICAP (5.5K) |
| MUFONJournal + MUFON* | 2,861 | MUFON |

Only UFOReportCtr is skipped at import time. Other overlaps are handled by the deduplication engine.

## Database Schema

### Core Tables

**`sighting`** (614,505 rows, 42 columns) — The main table. Each row is one reported sighting event.

- **Provenance**: `source_db_id`, `source_record_id`, `origin_id`, `origin_record_id`
- **Dates**: `date_event` (ISO 8601), `date_event_raw`, `date_end`, `time_raw`, `timezone`, `date_reported`, `date_posted`
- **Location**: `location_id` (FK to `location` table)
- **Description**: `summary`, `description`
- **Observation**: `shape`, `color`, `size_estimated`, `angular_size`, `distance`, `duration`, `duration_seconds`, `num_objects`, `num_witnesses`, `sound`, `direction`, `elevation_angle`, `viewed_from`
- **Witness**: `witness_age`, `witness_sex`, `witness_names`
- **Classification**: `hynek`, `vallee`, `event_type`, `svp_rating`
- **Resolution**: `explanation`, `characteristics`
- **Context**: `weather`, `terrain`, `source_ref`, `page_volume`, `notes`
- **Preservation**: `raw_json` — complete original record as JSON

**`location`** — Deduplicated locations with `raw_text`, `city`, `county`, `state`, `country`, `region`, `latitude`, `longitude`.

**`source_database`** (5 rows) — UFOCAT, NUFORC, MUFON, UPDB, UFO-search.

**`source_origin`** (31 rows) — Upstream sources within aggregator databases (Blue Book, NICAP, Hatch, etc.).

**`duplicate_candidate`** (126,730 rows) — Flagged duplicate pairs with similarity scores.

## Import Methodology

Each source has a custom import script. Two sources (UFOCAT and UPDB) skip known-duplicate sub-sources at import time:

- **UFOCAT** skips `SOURCE=UFOReportCtr` (123K NUFORC-origin records)
- **UPDB** skips `name=MUFON` and `name=NUFORC` (1.82M records)

### Source-Specific Handling

**UFOCAT** (`import_ufocat.py`):
- 55-column CSV with split date fields (YEAR, MO, DAY, TIME)
- City stored in ALL CAPS in `raw_text`; copied to `city` column post-import
- Longitude negated for US/CA locations (stored as positive in source)
- Hynek/Vallee classifications mapped directly
- `SOURCE` column identifies upstream origin; `UFOReportCtr` records saved to enrichment sidecar file instead of being imported

**NUFORC** (`import_nuforc.py`):
- Multi-line CSV with quoted description fields
- Dates: `1995-02-02 23:00 Local` format
- Locations: `City, ST, Country`

**MUFON** (`import_mufon.py`):
- 7-column CSV with embedded `\n` in dates (`1992-08-19\n5:45AM`)
- Locations with escaped commas: `Newscandia\, MN\, US`

**UPDB** (`import_updb.py`):
- 1.9M rows; `name` column identifies sub-source
- 1,820,741 MUFON/NUFORC rows skipped
- Remaining 65,016 mapped to `source_origin` entries

**UFO-search** (`import_geldreich.py`):
- JSON array of 54,751 records from 19 historical compilations
- Variable date formats: "Summer 1947", "4/34", "0's", "6/24/1947"
- Regex-based date parser; free-text location parsing

### Enrichment

After all imports, `enrich.py` transfers UFOCAT metadata to NUFORC records:

1. Loads the enrichment sidecar (`ufocat_enrichment.jsonl`) produced by UFOCAT import
2. Matches to NUFORC sightings by date + normalized city + state
3. Transfers `hynek`, `vallee`, and `shape` where the NUFORC record has NULL values

**Result**: 102,554 NUFORC records gained Hynek classifications, 83,710 gained Vallee classifications, 1,697 gained shape data. 19,637 enrichment records had no matching NUFORC sighting.

### Data Quality Fixes

Applied automatically by `rebuild_db.py`:

- **UFOCAT longitude sign**: 30,822 Western Hemisphere locations had positive longitude; negated for US/CA
- **UFOCAT city field**: 73,766 locations had city only in `raw_text` — copied to `city` column
- **Country code normalization**: USA→US, United Kingdom→GB, Canada→CA, Australia→AU
- **MUFON dates**: Parsed `\n` separators and converted 12hr→24hr time

## Deduplication Methodology

Deduplication uses a **two-phase strategy**: known overlaps are eliminated at import time, then a three-tier matching engine flags remaining cross-source duplicates for review. **No records are deleted** — all 614,505 sightings remain in the database, with 126,730 candidate pairs stored in the `duplicate_candidate` table for downstream resolution.

### Phase 1: Import-Time Filtering

Before deduplication even runs, two aggregator sources skip sub-sources that would create known duplicates with higher-quality originals already imported:

| Source | Sub-Source Skipped | Records Skipped | Reason |
|--------|--------------------|-----------------|--------|
| **UFOCAT** | `SOURCE=UFOReportCtr` | 123,304 | Copies of NUFORC sightings (NUFORC imported directly with richer descriptions) |
| **UPDB** | `name=MUFON` | 131,506 | MUFON imported directly with richer descriptions |
| **UPDB** | `name=NUFORC` | 1,689,235 | NUFORC imported directly with richer descriptions |

This eliminates **1,944,045 known duplicates** before dedup begins, reducing the working set from ~2.56M raw records to 614,505. The UFOCAT skip also triggers enrichment (see below) to preserve valuable Hynek/Vallee metadata.

Other overlapping sub-sources (e.g. UFOCAT's Hatch records vs UFO-search's Hatch records) are kept and handled by the dedup engine, since both copies may carry unique metadata worth preserving.

### Phase 1.5: Metadata Enrichment

UFOCAT's 123K skipped UFOReportCtr records carry Hynek and Vallee classifications that NUFORC natively lacks. Rather than lose this data, `import_ufocat.py` writes skipped records to a sidecar file (`ufocat_enrichment.jsonl`), and `enrich.py` transfers the metadata to matching NUFORC sightings post-import.

**Matching**: Date (YYYY-MM-DD) + normalized UPPER(city) + UPPER(state). City normalization strips parenthetical qualifiers, trailing punctuation, and collapses whitespace.

**Transfer rules**: Only fills NULL fields — never overwrites existing NUFORC values.

| Field | NUFORC Records Enriched |
|-------|-------------------------|
| Hynek classification | 102,554 |
| Vallee classification | 83,710 |
| Shape | 1,697 |
| Unmatched (no NUFORC hit) | 19,637 |

### Phase 2: Three-Tier Cross-Source Matching (`dedup.py`)

After all imports and enrichment, the dedup engine compares records across different sources using progressively broader matching strategies. Each tier builds on the previous, skipping pairs already flagged.

#### Tier 1: MUFON ↔ NUFORC (7,694 pairs)

The highest-overlap pair. Both sources cover modern US sightings with reliable date/location data.

- **Match key**: Exact date (YYYY-MM-DD) + UPPER(city) + UPPER(state)
- **Loading**: MUFON city comes from `location.city`; NUFORC city from `location.city`
- **Scoring**: Full description similarity with source-specific preprocessing
- **Result**: 7,694 candidate pairs

#### Tier 2: All Remaining Cross-Source Pairs (101,879 pairs)

Four sub-tiers cover every remaining source combination, using the match key best suited to each source's location data quality:

| Sub-tier | Sources | Match Key | Why This Key | Pairs |
|----------|---------|-----------|--------------|-------|
| **2a** | MUFON ↔ UFOCAT | date + city + state | Both have structured state fields | 2,295 |
| **2b** | NUFORC ↔ UFOCAT | date + city + state | Both have structured state fields | 4,148 |
| **2c** | UPDB ↔ MUFON/NUFORC/UFOCAT | date + city **(no state)** | UPDB has inconsistent state data; city-only matching is more reliable | 63,459 |
| **2d** | UFO-search ↔ MUFON/NUFORC/UFOCAT | date + city + state | UFO-search locations parsed from free text via regex (`City, ST` format) | 31,977 |

**Source-specific notes**:
- UFOCAT cities are stored in `raw_text` (ALL CAPS), not `city` — the loader reads `raw_text` instead
- UFO-search locations are free-text strings parsed by regex to extract `(city, state)` pairs; only locations matching the `City, ST` pattern with a valid US/Canadian state code are matchable
- UPDB sub-tier (2c) filters to US records only (`country='US'`) to reduce false positives from city-only matching
- All candidate pairs are normalized so `sighting_id_a < sighting_id_b` to enforce the UNIQUE constraint and prevent directional duplicates

#### Tier 3: Description Fuzzy Matching (17,157 pairs)

Catches duplicates that Tiers 1-2 miss due to location data differences (misspellings, missing state, different geocoding).

- **Match key**: Date only (no location requirement)
- **Scope**: Only dates with records from 2+ sources AND ≤20 total records on that date. This keeps the pairwise comparison space manageable — a date with 100 records from 3 sources would generate thousands of pairs
- **Skip**: Pairs already found in Tiers 1-2 are excluded
- **Two-stage filtering**:
  1. **Token Jaccard > 0.25** — Fast set-intersection filter on lowercased word tokens. Eliminates obvious non-matches without expensive string alignment
  2. **SequenceMatcher ≥ 0.5** — Python's `difflib.SequenceMatcher` on the first 1,000 characters of each description. Only pairs passing the Jaccard gate reach this step
- **Result**: 17,157 candidates from cross-source pairs that share a date but weren't caught by location matching

### Similarity Scoring

Every candidate pair receives a similarity score (0.0–1.0) computed by `compute_similarity()`:

1. **Source-specific preprocessing**:
   - NUFORC: Strips `NUFORC UFO Sighting NNNNN` prefix
   - MUFON: Strips `Submitted by razor via e-mail` boilerplate, extracts investigator notes
2. **"Starts with" shortcut**: If both descriptions share the same first N characters (N ≥ 20), score = 0.95. This catches UFOCAT records that truncated or copied NUFORC descriptions
3. **Token Jaccard pre-filter**: If token Jaccard < 0.03, return that score immediately (no point running expensive alignment)
4. **Full alignment**: `difflib.SequenceMatcher` on first 1,000 characters of each description

Pairs with no description on either side receive score = 0.0 (these are still flagged as candidates based on location matching, just with a zero similarity score).

### Results

**126,730 duplicate candidate pairs** across 127,440 unique sightings (20.7% of all records).

| Confidence | Score Range | Pairs | Interpretation |
|------------|-------------|-------|----------------|
| Certain | 0.9 – 1.0 | 14,260 | Near-identical descriptions; safe to auto-merge |
| Likely | 0.7 – 0.9 | 9,567 | Strong match; minor wording differences |
| Possible | 0.5 – 0.7 | 13,303 | Same event reported differently across sources |
| Weak | 0.3 – 0.5 | 11,144 | Same date+location, descriptions partially overlap; needs manual review |
| Unlikely | 0.0 – 0.3 | 78,456 | Same date+location but likely different events (e.g. multiple sightings on busy nights) |

**By match method**:

| Method | Pairs | Avg Score |
|--------|-------|-----------|
| `tier2c_updb_ufocat` | 59,620 | 0.225 |
| `tier2d_ufosearch_ufocat` | 31,439 | 0.240 |
| `tier3_desc_fuzzy` | 17,157 | 0.768 |
| `tier1a_mufon_nuforc` | 7,694 | 0.226 |
| `tier2b_nuforc_ufocat` | 4,148 | 0.129 |
| `tier2c_updb_nuforc` | 3,519 | 0.234 |
| `tier2a_mufon_ufocat` | 2,295 | 0.072 |
| `tier2d_ufosearch_nuforc` | 397 | 0.044 |
| `tier2c_updb_mufon` | 320 | 0.012 |
| `tier2d_ufosearch_mufon` | 141 | 0.009 |

**Note**: The previous build flagged 242K duplicate candidates. The current build flags only 126K because the 123K UFOCAT-NUFORC duplicates (UFOReportCtr) are now prevented at import time rather than flagged after the fact. This is a cleaner approach — those weren't really "candidates" since they were known copies.

### What Dedup Does NOT Do

- **No records are deleted or merged**. The `duplicate_candidate` table is advisory. All 614,505 sightings remain queryable.
- **No within-source dedup**. The engine only flags cross-source pairs (different `source_db_id`). Duplicates within a single source (e.g. two NUFORC records for the same event) are not flagged.
- **No transitive closure**. If A↔B and B↔C are both flagged, A↔C is NOT automatically inferred. Each pair is independent.
- **Multiple witnesses are preserved**. If the same event has genuinely separate witness reports in different sources, both records remain. The similarity score helps distinguish true duplicates (high score) from independent reports of the same event (low score, different descriptions).

## UFO Explorer GUI

A self-contained web GUI in `ufo-explorer/`.

### Features

- **Interactive Map** — Leaflet.js with marker clustering, color-coded by source, bbox-based lazy loading
- **Timeline** — Chart.js stacked bar chart by year, click to drill down to monthly view
- **Search** — Full-text search across descriptions, filtered by shape/source/Hynek/date range
- **Detail View** — Full sighting record with mini-map, classification badges, raw JSON toggle

### Running

```bash
cd ufo-explorer
pip install flask
python app.py
# Open http://localhost:5000
```

## File Inventory

### Database & Pipeline
| File | Description |
|------|-------------|
| `ufo_unified.db` | Main unified database (~1.4 GB) |
| `create_schema.py` | Schema definition, indexes, seed data |
| `rebuild_db.py` | **Master rebuild script** — runs full pipeline end-to-end |

### Import Scripts
| File | Source | Imported | Skipped |
|------|--------|----------|---------|
| `import_ufocat.py` | UFOCAT 2023 CSV | 197,108 | 123,304 (UFOReportCtr) |
| `import_nuforc.py` | NUFORC CSV | 159,320 | — |
| `import_mufon.py` | MUFON CSV | 138,310 | — |
| `import_updb.py` | UPDB CSV | 65,016 | 1,820,741 (MUFON/NUFORC) |
| `import_geldreich.py` | UFO-search JSON | 54,751 | — |

### Analysis & Tools
| File | Description |
|------|-------------|
| `enrich.py` | Transfers UFOCAT Hynek/Vallee metadata to NUFORC records |
| `dedup.py` | Three-tier deduplication engine |
| `fix_coords.py` | Coordinate validation and auto-repair |
| `db_summary.py` | Database statistics report |

### GUI
| File | Description |
|------|-------------|
| `ufo-explorer/app.py` | Flask API server |
| `ufo-explorer/static/index.html` | Single-page app shell |
| `ufo-explorer/static/app.js` | Map/Timeline/Search logic |
| `ufo-explorer/static/style.css` | Dark theme styling |
| `ufo-explorer/ufo_unified.db` | Database copy for GUI |
