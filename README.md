# Unified UFO Sightings Database

A unified SQLite database merging five major UFO/UAP sighting databases into a single, deduplicated repository of **614,505 sighting records** spanning from antiquity to 2026, with **126,730 duplicate candidate pairs** flagged for review.

**For researchers reproducing this work**, jump to [`docs/`](docs/) (full index in [`docs/README.md`](docs/README.md)):

- 📦 **[`data/raw/README.md`](data/raw/README.md)** — where to obtain each source dataset (NUFORC, MUFON, UFOCAT, UPDB, UFO-search) and how to lay them out
- 🛠️ **[`docs/PIPELINE.md`](docs/PIPELINE.md)** — step-by-step rebuild tutorial with expected outputs at each stage
- 📊 **[`docs/SCHEMA.md`](docs/SCHEMA.md)** — column-by-column reference for `ufo_public.db` with real coverage numbers
- 🔍 **[`docs/QUERIES.md`](docs/QUERIES.md)** — ~25 tested SQL recipes for analysis
- 🧪 **[`docs/METHODOLOGY.md`](docs/METHODOLOGY.md)** — how the algorithms work: dedup, quality score, hoax detection, sentiment models
- 🔧 **[`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md)** — how to add a new source or new analysis step
- ⚠️ **[`docs/PITFALLS.md`](docs/PITFALLS.md)** — gotchas, silent failures, and surprising-but-intentional behaviors
- 📜 **[`CHANGELOG.md`](CHANGELOG.md)** — version history for citation

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

**`sighting`** (614,505 rows, 69 columns) — The main table. Each row is one reported sighting event.

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
- **Derived Analysis** (populated by `analyze.py`): `standardized_shape`, `primary_color`, `sentiment_score`, `dominant_emotion`, `quality_score`, `richness_score`, `hoax_likelihood`, `duration_bucket`, `movement_type`, `has_movement_mentioned`, `movement_categories` (JSON), `topic_id`
- **Public Dataset Fields** (populated by `analyze.py`): `lat`, `lng`, `sighting_datetime`, `has_description`, `has_media`
- **Emotion Classification** (populated by `emotions.py`): `emotion_28_dominant`, `emotion_28_group`, `emotion_7_dominant`, `vader_compound`, `roberta_sentiment`, `emotion_7_surprise`, `emotion_7_fear`, `emotion_7_neutral`, `emotion_7_anger`, `emotion_7_disgust`, `emotion_7_sadness`, `emotion_7_joy`

**`sighting_analysis`** (614,505 rows) — Richer JSON-encoded derived fields, one row per sighting:
- `behavior_tags` (JSON array), `color_list` (JSON array), `emotion_scores` (JSON object), `hoax_flags` (JSON array), `raw_shape_matched_via`

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

## Derived Analysis Pipeline

After import, enrichment, geocoding, and deduplication, `analyze.py` runs a 9-step derived-analysis pipeline that produces legally-safe, non-copyrighted features for the public site. The pipeline is defined as an ordered list of `(name, function, label)` tuples in `ANALYSIS_STEPS` — adding a new step (e.g. a future offline LLM enrichment) is one line.

### Steps

| # | Step | Output Columns | Description |
|---|------|----------------|-------------|
| 1 | Shape normalization | `standardized_shape` | Fuzzy-matches raw `shape` against 25 canonical values via rapidfuzz |
| 2 | Movement classification | `movement_type`, `has_movement_mentioned`, `movement_categories` (JSON), `behavior_tags` (JSON) | Regex-based taxonomy of 10 movement categories + 14 behavior tags |
| 3 | Color extraction | `primary_color`, `color_list` (JSON) | Word-boundary regex against a 21-color whitelist |
| 4 | Sentiment derivation | `sentiment_score`, `dominant_emotion`, `emotion_scores` (JSON) | Derived from the `sentiment_analysis` table (VADER + NRC) |
| 5 | Duration bucketing | `duration_bucket` | Maps `duration_seconds` to instant/seconds/minutes/hours/days |
| 6 | Public field derivation | `lat`, `lng`, `sighting_datetime`, `has_description`, `has_media` | Denormalizes coords, combines date+time, detects media mentions |
| 7 | Quality scoring | `quality_score` (0-100), `richness_score` | Weighted heuristic: description length, media, witnesses, movement, structured fields, coords. Unknown-date rows capped at 15 (relaxed to 35 for rich rows). |
| 8 | Hoax flagging | `hoax_likelihood` (0-1), `hoax_flags` (JSON) | Rule-based: short text, generic phrasing, duplicate phrasing, dramatic + no specifics, all-caps |
| 9 | Topic modeling | `topic_id` | STUB — reserved for v0.9 |

### Emotion Classification (`emotions.py`)

Runs three transformer models + VADER on GPU (CUDA) for every sighting with narrative text (502,985 rows, 81.9% coverage):

| Model | HuggingFace ID | Output |
|-------|----------------|--------|
| GoEmotions 28-class | `SamLowe/roberta-base-go_emotions` | `emotion_28_dominant`, `emotion_28_group` |
| 7-class RoBERTa | `j-hartmann/emotion-english-distilroberta-base` | `emotion_7_dominant`, 7 probability columns |
| RoBERTa sentiment | `cardiffnlp/twitter-roberta-base-sentiment-latest` | `roberta_sentiment` (-1 to +1) |
| VADER | vaderSentiment | `vader_compound` (-1 to +1) |

### Quality Score Formula (v0.8.3b)

```
description length (0 / <50 / <200 / 200+)    0 / 5 / 15 / 25
has_media = 1                                 +15
num_witnesses tier (0 / 1 / 2 / 3+)           0 / 5 / 10 / 15
has_movement_mentioned                        +10  (+5 if 2+ categories)
9 structured fields x 3 pts each              max 27
coords present                                +5
specificity bonus (time/direction/altitude)    +5
cap: min(100, score)
unknown-date cap: min(score, 15)  [relaxed to 35 if features>=8 + has_description]
```

### Public Export

`export_public.py` produces a clean, text-free SQLite (`ufo_public.db`) from the private analysis DB:

```bash
python export_public.py
# Source: 1.7 GB -> Public: 507 MB (71.6% reclaimed)
```

The export uses an **allowlist** (`PUBLIC_TABLES`): only explicitly listed tables survive. Raw text columns (`description`, `summary`, `notes`, `raw_json`) are dropped. Private tables (`sentiment_analysis`, `duplicate_candidate`, `reference`, `sighting_reference`, `attachment`) are dropped. The private DB is never modified.

## Rebuilding from Scratch

### Prerequisites

1. **Python 3.10+** (tested on 3.12) and `pip`.
2. **Source data files.** None are committed — see [`data/raw/README.md`](data/raw/README.md) for per-source acquisition notes (where to get NUFORC, MUFON, UFOCAT, UPDB, UFO-search). Files go either in `./data/raw/` (set `UFOSINT_DATA_DIR=$(pwd)/data/raw`) or in `../data/raw/` (the parent-directory layout this repo defaults to).
3. **For emotion classification (optional, v0.11+)**: an NVIDIA GPU with ≥6 GB VRAM, CUDA-enabled PyTorch, and the `transformers` library. CPU inference works but is ~50× slower.

### Quick verification before you start

```bash
# Confirm all 5 source files are reachable at the expected paths
python -c "
import os
from import_nuforc    import CSV_PATH as P1
from import_mufon     import CSV_PATH as P2
from import_ufocat    import CSV_PATH as P3
from import_updb      import CSV_PATH as P4
from import_geldreich import JSON_PATH as P5
for label, p in [('nuforc', P1), ('mufon', P2), ('ufocat', P3),
                 ('updb', P4), ('ufo-search', P5)]:
    ok = 'OK ' if os.path.exists(p) else 'MISS'
    size = f'{os.path.getsize(p)/1024/1024:>7.1f} MB' if os.path.exists(p) else '       --'
    print(f'  {ok}  {size}  {os.path.normpath(p)}')
"
```

Every line should print `OK` and a non-zero size.

### Full pipeline

```bash
# 1. Install ETL dependencies
pip install -r requirements-etl.txt

# 2. Download GeoNames gazetteer (one-time, ~10 MB)
python geocode.py --download

# 3. Full rebuild (~25 min: import + geocode + dedup + sentiment + analyze)
python rebuild_db.py

# 4. (Optional) Emotion classification — GPU-accelerated transformers
pip install torch transformers          # ~2 GB download for torch+CUDA
python emotions.py --db ufo_unified.db  # ~30 min on RTX 4060 Ti

# 5. Export public DB (strips raw text + private tables, VACUUMs)
python export_public.py \
    --source ufo_unified.db \
    --target ufo_public.db
```

### Configuring the source data path

The 5 importers all read `UFOSINT_DATA_DIR` if set, otherwise default to `../data/raw/` (relative to this directory). Override examples:

```bash
# bash / zsh
export UFOSINT_DATA_DIR=/path/to/your/data/raw
python rebuild_db.py

# PowerShell
$env:UFOSINT_DATA_DIR = "C:\path\to\your\data\raw"
python rebuild_db.py
```

For a standalone clone of just `ufo-dedup/`, the simplest setup is to put your data under `ufo-dedup/data/raw/` and set `UFOSINT_DATA_DIR=$(pwd)/data/raw` before running `rebuild_db.py`.

## Tests

```bash
pip install pytest
pytest tests/ -v   # 369 tests
```

Test suite covers: schema creation, ETL parsers, data quality fixes, deduplication, shape normalization, movement classification, color extraction, sentiment derivation, quality scoring, unknown-date capping, hoax flagging, duration bucketing, public-field derivation, idempotency.

## UFO Explorer GUI

> **Note**: The production explorer has moved to `ufosint-explorer/` (separate repo). The `ufo-explorer/` directory in this repo is a legacy prototype and is no longer maintained.

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

### Analysis & Derived Features
| File | Description |
|------|-------------|
| `analyze.py` | **Derived-insight pipeline** — 9-step plug-in-ready pipeline (`ANALYSIS_STEPS` registry). Produces standardized shapes, movement/behavior tags, color extraction, quality scores, hoax flags, sentiment derivation, duration bucketing, and public-dataset field denormalization. |
| `emotions.py` | **Transformer emotion classification** — GoEmotions 28-class, 7-class RoBERTa, RoBERTa sentiment, and VADER. GPU-accelerated (CUDA), batched, idempotent. Classifies 502,985 sightings with text. |
| `sentiment.py` | VADER + NRCLex sentiment/emotion analysis (legacy, replaced by `emotions.py` for production) |
| `export_public.py` | **Clean public export** — copies the private DB, strips raw text columns (`description`, `summary`, `notes`, `raw_json`) and private-only tables, VACUUMs. Produces `ufo_public.db` (507 MB) from `ufo_unified.db` (1.7 GB). Allowlist-driven: only tables in `PUBLIC_TABLES` survive. |
| `enrich.py` | Transfers UFOCAT Hynek/Vallee metadata to NUFORC records |
| `dedup.py` | Three-tier deduplication engine |
| `geocode.py` | Offline geocoding via GeoNames gazetteer (cities15000.txt, auto-downloaded) |
| `fix_coords.py` | Coordinate validation and auto-repair |
| `db_summary.py` | Database statistics report |

### Tests
| File | Description |
|------|-------------|
| `tests/test_analyze.py` | 39 tests for the derived-analysis pipeline |
| `tests/test_data_quality.py` | Data quality fix tests (shapes, dates, Hynek, Vallee) |
| `tests/test_dedup.py` | Deduplication engine tests |
| `tests/test_etl.py` | ETL parser and schema tests |
| `tests/conftest.py` | Shared fixtures: in-memory DB, `insert_test_sighting()` helper |
