# v0.14 Handoff — Dev Team

**Date:** 2026-04-19
**File:** `data/output/ufo_public.db` (541 MB, 618,316 sightings)

## What changed since v0.13

### Data quality overhaul
This release includes a major data quality improvement pass using LLM-powered analysis across the entire database.

| Improvement | Records affected | Method |
|---|---|---|
| Bad geocodes fixed (wrong country/hemisphere) | 29,029 pins removed | Code: state bounding-box validation |
| Location strings normalized for geocoding | 78,518 sightings | LLM: Gemini Flash via OpenRouter |
| New correct map pins | +48,503 net | Improved geocoder + LLM-cleaned locations |
| Shape extracted from descriptions | +125,236 | LLM field extraction |
| Color extracted from descriptions | +202,216 | LLM field extraction |
| Direction extracted from descriptions | +119,214 | LLM field extraction |
| Sound extracted from descriptions | +61,934 | LLM field extraction |
| Duration parsed from text | +200,835 | Regex parser (code) |
| Witnesses extracted from descriptions | +30,128 | LLM field extraction |
| Shapes remapped (Ovoid->Oval, etc.) | +14,016 | Extended shape alias map (code) |

### Key metrics

| Metric | v0.13 | v0.14 | Change |
|---|---|---|---|
| Total sightings | 618,316 | 618,316 | same |
| Geocoded (has map pin) | 398,135 (64.4%) | **418,077 (67.6%)** | +19,942 |
| Quality score >= 60 | 119,965 | **160,728** | **+40,763 (+34%)** |
| Avg quality score | 39.4 | **42.3** | +2.9 |
| Has standardized shape | 238,848 | **343,602** | +104,754 |
| Has color | ~62,198 | **264,414** | +202,216 |
| Has duration (seconds) | ~994 | **232,646** | +231,652 |
| Has sound | ~26,877 | **88,645** | +61,768 |
| Has direction | ~12,378 | **131,592** | +119,214 |
| Duration bucketed | 994 | **232,438** | +231,444 |

### New columns on sighting (v0.14)

```sql
-- Reddit / LLM fields (from v0.13 import_reddit.py, now in schema)
reddit_post_id      TEXT UNIQUE
reddit_url          TEXT
llm_confidence      TEXT        -- high|medium|low
llm_anomaly_assessment TEXT     -- anomalous|prosaic|ambiguous
llm_prosaic_candidate TEXT
llm_strangeness_rating INTEGER  -- 1-5
llm_model           TEXT
has_photo            INTEGER    -- 0 or 1
has_video            INTEGER    -- 0 or 1

-- Audit fields (metadata about data quality processing)
audit_status         TEXT       -- pending|audited|extracted|skipped|error
audit_location_check TEXT       -- match|mismatch|normalized|no_improvement
audit_location_fix   TEXT       -- JSON: corrected city/state/country
audit_geocode_check  TEXT       -- match|mismatch
audit_data_extracted TEXT       -- JSON: LLM-extracted fields
audit_quality_notes  TEXT
audit_batch_id       INTEGER
audit_model          TEXT
audit_timestamp      TEXT
```

### New canonical shapes (3 added)
`Crescent`, `Cloud`, `Dome` — in addition to the existing 25.

### Tables in public DB
- `sighting` — 618,316 rows, ~90 columns (raw text stripped)
- `location` — 218K rows
- `source_database` — 6 sources
- `source_collection` — 3 collections
- `source_origin` — upstream source tracking
- `sighting_analysis` — derived JSON fields (behavior_tags, color_list, hoax_flags)
- `crash_retrieval` — 14 UAP Gerb crash cases
- `nuclear_encounter` — 35 Hastings nuclear encounters
- `facility` — 75 nuclear/military facilities

**Dropped from public:** `sentiment_analysis`, `duplicate_candidate`, `reference`, `sighting_reference`, `attachment`, `audit_batch`

## Migration instructions

### 1. PG schema update

Run this SQL on Azure Postgres **before** migrating data:

```sql
-- v0.14 Reddit + LLM columns
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS reddit_post_id TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS reddit_url TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS llm_confidence TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS llm_anomaly_assessment TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS llm_prosaic_candidate TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS llm_strangeness_rating SMALLINT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS llm_model TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS has_photo SMALLINT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS has_video SMALLINT;

-- v0.14 Audit columns (optional — useful for data provenance)
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS audit_status TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS audit_location_check TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS audit_geocode_check TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS audit_model TEXT;
ALTER TABLE sighting ADD COLUMN IF NOT EXISTS audit_timestamp TEXT;

-- Unique index for Reddit dedup
CREATE UNIQUE INDEX IF NOT EXISTS idx_sighting_reddit_post_id
    ON sighting(reddit_post_id) WHERE reddit_post_id IS NOT NULL;

-- Audit indexes (optional)
CREATE INDEX IF NOT EXISTS idx_sighting_audit_status ON sighting(audit_status);
```

### 2. Data migration

```bash
cd ufosint-explorer/scripts
python migrate_sqlite_to_pg.py \
    --source /path/to/ufo_public.db
```

The column-probe in `migrate_sqlite_to_pg.py` handles new columns gracefully — it intersects with the PG schema and skips any columns that don't exist yet on PG. So it's safe to run even if you haven't applied the ALTER TABLEs above (you'll just miss the new columns).

### 3. Verify

```sql
-- Quick sanity checks after migration
SELECT COUNT(*) FROM sighting;                    -- 618,316
SELECT COUNT(*) FROM sighting WHERE lat IS NOT NULL;  -- 418,077
SELECT COUNT(*) FROM sighting WHERE quality_score >= 60;  -- 160,728
SELECT COUNT(*) FROM sighting WHERE color IS NOT NULL;    -- 264,414
SELECT COUNT(*) FROM sighting WHERE reddit_post_id IS NOT NULL;  -- 3,811
```

## What the public site gets

With this release, the map/explorer now has:
- **20K more correct map pins** (and 29K wrong ones removed)
- **40K more "high quality" records** visible when filtering QS >= 60
- **Color filtering** now works on 264K records (was ~62K)
- **Shape filtering** covers 344K records (was 239K)
- **Duration filtering** covers 232K records (was ~1K)
- **Sound data** on 89K records (was ~27K)
- **Direction of travel** on 132K records (was ~12K)
- **Reddit r/UFOs** sightings (3,811) with LLM anomaly assessments

## File locations

```
ufo-dedup/data/output/
├── ufo_public.db              541 MB   ← HAND THIS TO DEV TEAM
├── ufo_unified.db           1,808 MB   ← Private, stays on research machine
├── llm_field_extractions.csv    MB     ← LLM extraction cache (for rebuild replay)
├── audit_tier_b_fixes.csv     6.9 MB   ← Location normalization cache
└── audit_tier_b_results.csv    34 MB   ← Full audit backup
```

## Notes

- **Emotion columns** (`emotion_7_*`, `emotion_28_*`, `vader_compound`, `roberta_sentiment`): These require a GPU re-run of `emotions.py` on the new DB. They are NOT populated in this export. The `dominant_emotion` column (VADER+NRC derived) IS populated. If the site needs the transformer emotions, run `emotions.py` before export.
- **Description column**: NULL for all legacy sources (raw text stripped). Populated for Reddit rows only (LLM-generated summaries, safe to display).
- The `audit_*` columns are metadata about our processing — safe to expose but not useful for end users. The dev team can ignore them in the UI.
