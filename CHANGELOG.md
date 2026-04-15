# Changelog

Version provenance for the unified UFO sightings pipeline. Each version is identified by what shipped to the public Postgres DB, not by a code release tag.

## v0.11.0 — Transformer emotion classification (current)

GPU-accelerated transformer-based emotion classification on every sighting with narrative text (502,985 rows = 81.9% coverage). Adds 12 new columns to `sighting`.

**New columns**
- `emotion_28_dominant`, `emotion_28_group` — GoEmotions 28-class label + sentiment group (positive / negative / ambiguous / neutral)
- `emotion_7_dominant` — 7-class RoBERTa emotion (surprise / fear / neutral / anger / disgust / sadness / joy)
- `vader_compound`, `roberta_sentiment` — both -1.0 to +1.0 sentiment scores
- `emotion_7_surprise` … `emotion_7_joy` — full 7-class softmax probability vector

**New module**
- `emotions.py` — GPU-accelerated batched inference using HuggingFace transformers. Models: `SamLowe/roberta-base-go_emotions`, `j-hartmann/emotion-english-distilroberta-base`, `cardiffnlp/twitter-roberta-base-sentiment-latest`, plus VADER (CPU). Idempotent with `--reset` and `--stats-only` flags.

**New PG migration**
- `add_v011_emotion_columns.sql` (ships in `ufosint-explorer/scripts/`)

**Build environment**
- Tested on RTX 4060 Ti (8 GB VRAM, CUDA), float16 inference, batch size 64
- ~35 min wall time for 502,985 rows
- ~600 rows/sec sustained throughput

**Coverage**
- NUFORC: 159,319 / 159,320 (100.0%)
- MUFON: 137,385 / 138,310 (99.3%)
- UPDB: 58,228 / 65,016 (89.6%)
- UFO-search: 54,751 / 54,751 (100.0%)
- UFOCAT: 93,302 / 197,108 (47.3%)

## v0.8.5 — Live deployment of v0.8.3b (no data changes)

Frontend bump. Schema version label flipped from `v083-1` to `v085-1`. App-side packing changes only — same data on PG.

## v0.8.3b — Quality score rebalance + movement classification

Refined quality-score weighting after v0.8.3a was too aggressive (only 14.3% landed at QS≥60). v0.8.3b brings that to 19.3% (118,320 rows) by upweighting structured fields (2pts → 3pts each) and relaxing the unknown-date cap for text-rich rows.

**Quality score formula (current)**
```
description length (0 / <50 / <200 / 200+)    0 / 5 / 15 / 25
has_media = 1                                 +15
num_witnesses tier (0 / 1 / 2 / 3+)           0 / 5 / 10 / 15
has_movement_mentioned                        +10  (+5 if 2+ categories)
9 structured fields × 3 pts each              max 27
coords present                                +5
specificity bonus (time/direction/altitude)    +5
unknown-date cap                              min(score, 15)
                                              (relaxed to 35 if features>=8 + has_description)
final cap                                     min(100, score)
```

**New columns**
- `has_movement_mentioned` (SMALLINT, 0/1) — narrative mentions structured movement
- `movement_categories` (TEXT, JSON array) — 10 categories: hovering, linear, erratic, accelerating, rotating, ascending, descending, vanished, followed, landed

**Coverage**
- 249,217 sightings (40.6%) have ≥1 movement category
- 1,282 NULL-date rich rows promoted from QS=15 to QS=35 under the relaxed cap

## v0.8.3a — Initial v0.8.3 build (deprecated)

First pass at the v0.8.3 quality-score rewrite. Too aggressive — only 14.3% of rows passed QS≥60. Replaced by v0.8.3b. Not deployed to production.

## v0.8.2 — Derived public fields + clean export

The cutover that made the public dataset legally distributable. Adds 17 derived columns to `sighting` and the `export_public.py` clean-export step.

**New columns**
- `lat`, `lng` — denormalized from `location` for fast map queries
- `sighting_datetime` — combined ISO 8601 date+time
- `has_description`, `has_media` — boolean filters
- `quality_score`, `richness_score`, `hoax_likelihood` — derived analysis
- `standardized_shape`, `primary_color`, `dominant_emotion` — text-derived labels
- `topic_id` (reserved), `duration_bucket`, `movement_type`

**New modules**
- `analyze.py` — 9-step ANALYSIS_STEPS registry pipeline
- `export_public.py` — allowlist-driven clean SQLite export

**New PG migration**
- `add_v082_derived_columns.sql`

**NRC silent-failure bug discovered and fixed mid-deploy.** `sentiment.py` had been silently catching `MissingCorpusError` from NRCLex and producing all-zero emotion data on every prior build. Fixed by downloading the NLTK corpora; sentiment table re-run as part of v0.8.2 cutover.

## v0.8.1 — Sentiment analysis (legacy)

VADER + NRCLex sentiment scoring on a separate `sentiment_analysis` table (one row per sighting). 502,985 rows scored. Replaced in v0.8.2 by direct columns on `sighting` and in v0.11 by transformer-based scoring.

## v0.8.0 — GeoNames geocoding

Added offline GeoNames-based geocoding (`geocode.py`). Uses `cities15000.txt` gazetteer. 396,240 sightings geocoded (64.5% coverage).

## v0.7.x — Three-tier deduplication engine

`dedup.py` ships with three-tier cross-source matching: Tier 1 (MUFON↔NUFORC date+city+state), Tier 2 (4 sub-tiers covering all remaining cross-source pairs), Tier 3 (description fuzzy matching for date-only matches). 126,730 candidate pairs flagged across 127,440 sightings.

## v0.5.x — Initial 5-source ETL

Five importers (`import_nuforc.py`, `import_mufon.py`, `import_ufocat.py`, `import_updb.py`, `import_geldreich.py`), schema definition (`create_schema.py`), and `enrich.py` for transferring UFOCAT Hynek/Vallée classifications to NUFORC records. 614,505 sightings imported after import-time skip filtering.

---

## Build provenance for citation

If you cite a specific build, the git commit on `main` of `ufo-dedup` at the time of the build is the canonical reference. For the v0.11 build documented throughout these docs:

- **ufo-dedup commit**: see `git log` on the `main` branch around the v0.11 docs commits
- **Source datasets**: NUFORC scrape ~2024, MUFON CMS export ~2024, UFOCAT 2023 (CUFOS), PhenomAInon UPDB ~2024, UFO-search Majestic Timeline ~2024
- **GeoNames gazetteer**: `cities15000.txt`, downloaded at build time
- **Transformer model snapshots**: HuggingFace IDs above pin to specific commits in the model cards

Sighting IDs are **not stable across rebuilds** — the importer reassigns them in source-load order each time. Use `(source_db_id, source_record_id)` as a stable cross-rebuild identifier.
