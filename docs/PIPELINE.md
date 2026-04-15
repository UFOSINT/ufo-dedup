# Pipeline Tutorial — Reproducing v0.11 from Scratch

A complete walkthrough that takes you from a fresh clone of `ufo-dedup` and the 5 source datasets to your own copy of `ufo_public.db`. Each step includes the exact command, expected output (with real numbers from our build), wall-clock timing, and what to do if it goes wrong.

This is the **operational** companion to:
- [`README.md`](../README.md) — what the project is
- [`docs/METHODOLOGY.md`](METHODOLOGY.md) — how the algorithms work
- [`docs/SCHEMA.md`](SCHEMA.md) — what's in the output

> **Total wall time on a recent NVIDIA workstation**: ~60 minutes (25 min rebuild + 35 min emotion classification + 5 min export). Most of it is unattended.

---

## 0. Prerequisites

| Requirement | Why |
|---|---|
| Python 3.10+ (3.12 tested) | All modules |
| ~5 GB free disk for source data | NUFORC + MUFON + UFOCAT + UPDB + UFO-search ≈ ~1 GB raw, plus build intermediates |
| ~10 GB free disk for build artifacts | `ufo_unified.db` is ~1.7 GB; intermediate `.db-wal` files can spike to ~3 GB during the build |
| NVIDIA GPU with ≥6 GB VRAM (optional, recommended) | Emotion classification step. CPU works but is ~50× slower (~30 hours vs ~30 minutes) |
| Internet access | One-time GeoNames gazetteer download (~10 MB), HuggingFace model downloads (~2 GB total) |

Verify Python and check for an NVIDIA GPU:

```bash
python --version            # 3.10 or higher
nvidia-smi                  # if you have a CUDA GPU
```

---

## 1. Get the source data

Five files, none in this repo. See [`data/raw/README.md`](../data/raw/README.md) for per-source acquisition (free / membership / academic). Place them at:

```
data/raw/
├── nuforc.csv                                   ~181 MB
├── mufon.csv                                    ~162 MB
├── UFOCAT/ufocat2023.csv                         ~91 MB
├── UPDB.app/phenomenAInon_UPDB.csv              ~280 MB
└── UFO-search/majestic.json                      ~72 MB
```

Or set `UFOSINT_DATA_DIR=/your/path` if your data lives somewhere else.

### Verify the data is reachable

```bash
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

**Expected output:**
```
  OK     180.5 MB  data/raw/nuforc.csv
  OK     161.9 MB  data/raw/mufon.csv
  OK      87.1 MB  data/raw/UFOCAT/ufocat2023.csv
  OK     280.4 MB  data/raw/UPDB.app/phenomenAInon_UPDB.csv
  OK      71.5 MB  data/raw/UFO-search/majestic.json
```

If any line shows `MISS`, the rebuild will fail at the corresponding import step. Don't proceed until all 5 are `OK`.

---

## 2. Install dependencies

```bash
pip install -r requirements-etl.txt
```

**Expected**: installs `vaderSentiment`, `NRCLex`, `rapidfuzz`. Roughly 200 KB of pure Python, instant.

For emotion classification (step 7) you'll also need PyTorch + transformers — install separately to avoid forcing it on people who only want the structural pipeline:

```bash
pip install torch transformers       # adds ~2 GB if CUDA wheels
```

### Download NLTK corpora for NRCLex

`sentiment.py` uses NRCLex which needs NLTK data. **Critical** — without this, NRC silently produces all-zero emotion counts (this was the v0.8.2 bug):

```bash
python -m textblob.download_corpora
```

**Expected**: downloads `brown`, `punkt_tab`, `wordnet`, `averaged_perceptron_tagger_eng`, `conll2000`, `movie_reviews`. ~50 MB total.

### Verify NRC actually works

```bash
python -c "
from nrclex import NRCLex
n = NRCLex('I was terrified by the bright craft, filled with fear and panic.')
print(n.raw_emotion_scores)
"
```

**Expected**: `{'positive': 1, 'anger': 1, 'fear': 2, 'negative': 2}`. If you get `{}`, the corpora aren't installed correctly — re-run the download step.

---

## 3. Download the GeoNames gazetteer

One-time download. Powers the offline geocoder.

```bash
python geocode.py --download
```

**Expected output:**
```
Downloading http://download.geonames.org/export/dump/cities15000.zip...
Extracting to .../ufo-dedup/geodata...
Gazetteer ready: .../ufo-dedup/geodata/cities15000.txt
Gazetteer loaded: 191,181 exact keys, 186,847 city+country keys, 182,088 city-only keys
```

(There may be a traceback at the end — `geocode.py --download` also tries to run geocoding against an empty DB. Ignore the traceback; only the download mattered.)

**Wall time**: ~20 seconds (downloads ~10 MB, extracts to ~30 MB).

---

## 4. Run the structural rebuild

This is the long step. Imports all 5 sources, applies data quality fixes, geocodes, enriches NUFORC with UFOCAT metadata, runs deduplication, computes sentiment, runs the derived analysis pipeline.

```bash
python rebuild_db.py
```

**Wall time**: ~25 minutes total. Breakdown by step:

| Step | What | Wall time |
|---|---|---:|
| 1 | Create schema | <1s |
| 2 | Import UFOCAT (197K rows + enrichment sidecar) | ~30s |
| 3 | Import NUFORC (159K rows) | ~25s |
| 4 | Import MUFON (138K rows) | ~20s |
| 5 | Import UPDB (65K rows kept, 1.82M skipped) | ~50s |
| 6 | Import UFO-search (55K rows from 19 historical compilations) | ~10s |
| 7 | Apply data quality fixes (~30 SQL updates) | ~10s |
| 8 | Geocode locations via GeoNames | ~3 min |
| 9 | Enrich NUFORC with UFOCAT Hynek/Vallée metadata | ~10s |
| 10 | Three-tier deduplication | ~1 min |
| 11 | Sentiment analysis (VADER + NRC on 503K rows) | ~9 min |
| 12 | Derived analysis pipeline (9 ANALYSIS_STEPS) | ~7 min |
| 13 | Copy to explorer (skipped — directory absent in standalone clones) | <1s |

**Expected final output:**
```
============================================================
  REBUILD COMPLETE
============================================================
  UFOCAT             197,108
  NUFORC             159,320
  MUFON              138,310
  UPDB                65,016
  UFO-search          54,751
  TOTAL              614,505

  Duplicate candidates: 126,729
  NUFORC records with Hynek (enriched): 102,554
  Geocoded sightings: 396,240 (43,994 locations via GeoNames)
  Sentiment records: 502,985
  Derived analysis: 614,505 scored (>=60: ~118,000, 24 standardized shapes)
  Public fields:    coords=396,165, datetime=604,393, has_description=510,229, has_media=96,998
  Movement/dates:   has_movement=249,217, date-capped (unknown date): 8,830

  Total elapsed: ~1500s (~25 min)
  Database size: 1678 MB
```

The numbers should match exactly except for `quality_score >= 60` (depends slightly on data quality fixes that are deterministic but version-sensitive — should land within ±200 rows of 118,320).

### If something goes wrong

| Symptom | Most likely cause | Fix |
|---|---|---|
| `FileNotFoundError` on a CSV/JSON | Source data missing or wrong location | Re-run the verification snippet from Step 1 |
| `ModuleNotFoundError: vaderSentiment` | Dependencies not installed | `pip install -r requirements-etl.txt` |
| `MissingCorpusError` (NRCLex) | NLTK corpora not installed | `python -m textblob.download_corpora` |
| `ERROR: Gazetteer not found` | Skipped step 3 | `python geocode.py --download` |
| Sentiment runs but emotion counts are all 0 | Same NRC corpora issue, but silently caught | See [PITFALLS.md](PITFALLS.md) |
| `database is locked` partway through | Another process holds the DB open | Close DB Browser / sqlite3 CLI / etc. |

---

## 5. Verify the rebuild

```bash
python -c "
import sqlite3
conn = sqlite3.connect('ufo_unified.db')
cur = conn.cursor()
for q in [
    \"SELECT COUNT(*) FROM sighting\",
    \"SELECT COUNT(*) FROM location\",
    \"SELECT COUNT(*) FROM duplicate_candidate\",
    \"SELECT COUNT(*) FROM sentiment_analysis\",
    \"SELECT COUNT(*) FROM sighting WHERE quality_score IS NOT NULL\",
    \"SELECT COUNT(*) FROM sighting WHERE quality_score >= 60\",
    \"SELECT COUNT(*) FROM sighting WHERE has_movement_mentioned = 1\",
    \"SELECT COUNT(*) FROM sighting WHERE lat IS NOT NULL AND lng IS NOT NULL\",
]:
    cur.execute(q)
    print(f'{q[:60]:<62} {cur.fetchone()[0]:>10,}')
"
```

**Expected output:**
```
SELECT COUNT(*) FROM sighting                                          614,505
SELECT COUNT(*) FROM location                                          214,782
SELECT COUNT(*) FROM duplicate_candidate                               126,729
SELECT COUNT(*) FROM sentiment_analysis                                502,985
SELECT COUNT(*) FROM sighting WHERE quality_score IS NOT NULL          614,505
SELECT COUNT(*) FROM sighting WHERE quality_score >= 60                118,320
SELECT COUNT(*) FROM sighting WHERE has_movement_mentioned = 1         249,217
SELECT COUNT(*) FROM sighting WHERE lat IS NOT NULL AND lng IS NOT NULL 396,165
```

If your numbers don't match within ±100 rows on any line, something diverged. The most common cause is a different snapshot of the source data (NUFORC scrapes age, MUFON exports update). The pipeline is otherwise deterministic.

---

## 6. (Optional) Run emotion classification

This adds the 12 transformer-derived emotion columns. Skip this step if you only want the structural pipeline (you'll still get `dominant_emotion` from VADER+NRC, just not the 28-class GoEmotions or 7-class RoBERTa).

### First run downloads ~2 GB of model weights

```bash
python emotions.py --db ufo_unified.db
```

The first run downloads:
- `SamLowe/roberta-base-go_emotions` (~500 MB)
- `j-hartmann/emotion-english-distilroberta-base` (~330 MB)
- `cardiffnlp/twitter-roberta-base-sentiment-latest` (~500 MB)

Models are cached in `~/.cache/huggingface/hub/`. Subsequent runs skip the download.

### Expected output:

```
  Device: NVIDIA GeForce RTX 4060 Ti
  Batch size: 64
  Loading GoEmotions 28-class model...
  Loading 7-class RoBERTa emotion model...
  Loading RoBERTa sentiment model...

  Sightings to classify: 502,985
  502,985/502,985 (100.0%, 600/s, ~0m remaining)

  Emotion classification complete:
    Records classified: 502,985
    Elapsed: ~2100s (~35 min)
    Rate: ~600 rows/s

  Coverage (614,505 total sightings):
    emotion_28_dominant       502,985  (81.9%)
    emotion_28_group          502,985  (81.9%)
    emotion_7_dominant        502,985  (81.9%)
    vader_compound            502,985  (81.9%)
    roberta_sentiment         502,985  (81.9%)

  Top 10 GoEmotions 28-class labels:
    neutral            (neutral  )    435,934
    realization        (ambiguous)     18,342
    confusion          (ambiguous)     10,730
    surprise           (ambiguous)      8,112
    admiration         (positive )      6,389
    fear               (negative )      5,865
    ...

  7-class emotion distribution:
    neutral         158,470
    surprise        157,644
    fear            134,322
    disgust          29,955
    sadness          10,301
    anger             8,089
    joy               4,204

  Average VADER compound:      0.1333
  Average RoBERTa sentiment:   0.0089
```

**Wall time**: ~35 min on RTX 4060 Ti. Scales roughly linearly with VRAM × clock; a 4090 will be ~3× faster, an old 1080 Ti ~2× slower, CPU-only ~50× slower.

### Resume after interruption

`emotions.py` is **idempotent** — it skips rows where `emotion_28_dominant IS NOT NULL`. If your run gets killed mid-way, just re-run the same command and it picks up where it left off.

---

## 7. Export the public DB

Strips raw narrative columns and private tables. Produces a clean, distributable SQLite.

```bash
python export_public.py
```

**Expected output:**
```
Source: data/output/ufo_unified.db
Target: data/output/ufo_public.db

Source DB is valid. 614,505 sightings, 1.7 GB.
Removing existing target ...
Copying source -> target via sqlite3.backup...
  done in ~3s

Stripping raw text columns from sighting...
    DROP COLUMN description
    DROP COLUMN summary
    DROP COLUMN notes
    DROP COLUMN raw_json

Dropping private-only tables...
    DROP TABLE sentiment_analysis
    DROP TABLE attachment
    DROP TABLE sighting_reference
    DROP TABLE duplicate_candidate
    DROP TABLE reference

Running VACUUM to reclaim disk...
  done in ~5s

============================================================
  EXPORT COMPLETE
============================================================
  Source:        1.7 GB
  Public:      507.0 MB
  Delta:         1.2 GB  (71.6% reclaimed)
```

The public DB is ~507 MB with all 12 derived columns + 12 emotion columns intact. Safe to redistribute (modulo `witness_names` — see [PITFALLS.md](PITFALLS.md)).

### Custom paths

```bash
python export_public.py --source /path/to/private.db --target /path/to/public.db
```

---

## 8. Run a smoke-test query

Confirm the public DB is queryable:

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/output/ufo_public.db')
cur = conn.cursor()
cur.execute('''
    SELECT sd.name AS source, s.emotion_7_dominant, COUNT(*) AS n
    FROM sighting s JOIN source_database sd ON s.source_db_id = sd.id
    WHERE s.emotion_7_dominant IS NOT NULL
    GROUP BY 1, 2 ORDER BY 1, n DESC LIMIT 10
''')
for r in cur.fetchall():
    print(f'  {r[0]:<12} {r[1]:<10} {r[2]:>8,}')
"
```

**Expected output:**
```
  MUFON        surprise     58,354
  MUFON        fear         36,580
  MUFON        neutral      30,175
  ...
```

Welcome to your reproduction. From here, see [`docs/QUERIES.md`](QUERIES.md) for analysis recipes and [`docs/SCHEMA.md`](SCHEMA.md) for the full column reference.

---

## Total resource consumption summary

| Resource | Used |
|---|---|
| Source data on disk | ~1 GB |
| Build intermediates (peak) | ~3 GB |
| `ufo_unified.db` (private, with raw text) | ~1.7 GB |
| `ufo_public.db` (clean export) | ~507 MB |
| HuggingFace model cache | ~2 GB |
| GeoNames gazetteer | ~30 MB |
| Wall time without GPU emotions | ~25 min |
| Wall time with GPU emotions | ~60 min |
| Wall time with CPU emotions | ~30 hours |

Once the build is done and the `ufo_public.db` is in hand, the Postgres migration (for those running the public app) is documented in `ufosint-explorer/scripts/migrate_sqlite_to_pg.py` — but most reproducers stop at the SQLite and query it directly.
