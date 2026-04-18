# Source Data Setup

This directory holds the raw source datasets that `rebuild_db.py` ingests. **None of the actual data files are committed to this repo** — they're either too large, paid/subscription-gated, or have licensing restrictions that prevent redistribution. You'll need to obtain each file yourself and place it in the structure below.

## Expected file tree

```
data/raw/
├── nuforc.csv                                   (~181 MB,  159K rows)
├── mufon.csv                                    (~162 MB,  138K rows)
├── UFOCAT/
│   └── ufocat2023.csv                           (~91 MB,   320K rows)
├── UPDB.app/
│   └── phenomenAInon_UPDB.csv                   (~280 MB,  1.9M rows)
└── UFO-search/
    └── majestic.json                            (~72 MB,   55K records)
```

After all files are in place, run:
```bash
pip install -r requirements-etl.txt
python geocode.py --download         # one-time GeoNames gazetteer (~10 MB)
python rebuild_db.py                  # full pipeline, ~25 min
```

## Data location override

By default the importers look at `../data/raw/` relative to this `ufo-dedup/` directory (the layout in the parent UFOSINT repo). To use a different location — including a checkout where you've placed data inside `ufo-dedup/data/raw/` — set the env var:

```bash
# bash / zsh
export UFOSINT_DATA_DIR=/abs/path/to/your/data/raw

# PowerShell
$env:UFOSINT_DATA_DIR = "C:\path\to\your\data\raw"
```

The 5 importers (`import_nuforc.py`, `import_mufon.py`, `import_ufocat.py`, `import_updb.py`, `import_geldreich.py`) all read this variable.

---

## Per-source acquisition notes

The data ecosystem around UFO/UAP reports is fragmented — sources have different licenses, access models, and update cadences. We can't redistribute these files but we can point you at the right starting line for each one. Some are free, some are paid, some require a membership.

### 1. NUFORC — National UFO Reporting Center

- **What it is**: Self-reported sighting database maintained by NUFORC since 1974. ~159K records as of 2024. Detailed free-text descriptions.
- **Official site**: https://nuforc.org
- **Direct database access**: NUFORC does not publish a bulk download. Their website is browseable but scrape-protected.
- **Common community sources**:
  - Periodic scrapes published on Kaggle (search "NUFORC ufo sightings")
  - GitHub repos that mirror older snapshots
  - The Phenomenon archive (phenomenon.app) includes a NUFORC subset
- **Expected filename**: `nuforc.csv`
- **Expected schema**: 18 columns including `date_time`, `city`, `state`, `country`, `shape`, `duration`, `summary`, `text`, `posted`, `images`. UTF-8, may have multi-line fields with quoted descriptions.
- **License**: Reports are user-submitted to NUFORC. NUFORC's terms govern redistribution. Verify your scrape source has appropriate rights before public reuse.

### 2. MUFON — Mutual UFO Network

- **What it is**: ~138K case reports from the world's largest civilian UFO investigation organization. Includes investigator notes alongside witness descriptions.
- **Official site**: https://mufon.com
- **Access**: MUFON CMS access requires paid membership. The full case database is behind a member portal.
- **Bulk export path**: MUFON has historically allowed members to export case data. Check the current member portal terms.
- **Community sources**: Some older snapshots circulate publicly but are typically out of date and may have been obtained without explicit license.
- **Expected filename**: `mufon.csv`
- **Expected schema**: 7 columns, typically `date`, `location`, `short_desc`, `long_desc`, `summary`, `case_number`. Date format is `YYYY-MM-DD\nHH:MMam` (literal `\n` separator). Locations have escaped commas: `Newscandia\, MN\, US`.
- **License**: MUFON owns the case database. **Confirm membership terms before any redistribution.**

### 3. UFOCAT — CUFOS UFOCAT Catalog

- **What it is**: ~320K records from the Center for UFO Studies. Highly structured: Hynek and Vallée classifications, lat/lng, witness counts, durations, source attributions.
- **Official site**: https://cufos.org
- **Access**: UFOCAT is an academic/research dataset. CUFOS distributes it through their channels — contact CUFOS directly for current access terms.
- **Versions**: We use the 2023 build. Older builds (pre-2020) circulate publicly but have substantially fewer records.
- **Expected path**: `UFOCAT/ufocat2023.csv`
- **Expected schema**: 55 columns including split date fields (`YEAR`, `MO`, `DAY`, `TIME`), `LOCATION` in ALL CAPS in `raw_text`, `HYNEK`, `VALLEE`, `LAT`, `LON` (note: longitudes stored with inverted sign — `import_ufocat.py` corrects this), `SOURCE` (upstream attribution).
- **License**: CUFOS-owned. Academic use is generally permitted; verify before redistribution.

### 4. UPDB — PhenomAInon Unified Phenomena Database

- **What it is**: ~1.9M raw records from the PhenomAInon (Phenomenon.app) project — an aggregator that combines MUFON, NUFORC, NICAP, Blue Book, UFODNA, and ~9 other sources into a single denormalized table.
- **Official site**: https://phenomenon.app (or check current PhenomAInon project URL)
- **Access**: PhenomAInon publishes bulk data dumps on their site. Free download, attribution requested.
- **Expected path**: `UPDB.app/phenomenAInon_UPDB.csv`
- **Expected schema**: 9 columns including `name` (upstream source identifier), `date`, `location`, `short_desc`, `long_desc`, `case_number`. The `name` column drives import-time filtering — `import_updb.py` skips MUFON and NUFORC records (1.82M rows) since we already imported those from their original sources, retaining only the ~65K records from upstream aggregators not covered elsewhere.
- **License**: PhenomAInon's terms apply. Mostly redistributive of upstream public datasets.

### 5. UFO-search — Rich Geldreich's Majestic Timeline

- **What it is**: ~55K historical UFO/UAP records compiled by Rich Geldreich from 19+ source compilations (Hatch's UFO catalog, Eberhart's UFOI, NICAP archive, Vallée's Magonia, Wonders in the Sky, Blue Book NICAP, Brazilian/Canadian/UK government files, etc.).
- **Official site**: https://ufo-search.com
- **Access**: Free download from the site. The JSON dump is published openly.
- **Expected path**: `UFO-search/majestic.json`
- **Expected schema**: JSON array of 54,751 records. Each record has highly variable date formats ("Summer 1947", "4/34", "0's", "6/24/1947"). `import_geldreich.py` includes a regex-based date parser to normalize these. Free-text location strings parsed by regex to extract `(city, state)` pairs.
- **License**: Compilation by Geldreich; underlying records mostly from public-domain government / academic sources.

---

## What rebuild_db.py does with these files

| Step | Reads from | Output |
|---|---|---|
| 2. Import UFOCAT | `UFOCAT/ufocat2023.csv` | 197,108 rows + `ufocat_enrichment.jsonl` sidecar (123K skipped UFOReportCtr records → enriched into NUFORC) |
| 3. Import NUFORC | `nuforc.csv` | 159,320 rows |
| 4. Import MUFON | `mufon.csv` | 138,310 rows |
| 5. Import UPDB | `UPDB.app/phenomenAInon_UPDB.csv` | 65,016 rows imported, 1,820,741 MUFON/NUFORC duplicates skipped |
| 6. Import UFO-search | `UFO-search/majestic.json` | 54,751 rows |

**Total after import: 618,316 unique sightings** (614,505 from 5 legacy sources + 3,811 from r/UFOs Reddit ingest). See the main `README.md` for the full pipeline (geocoding, enrichment, deduplication, derived analysis, emotion classification, public export).

---

## Validation

Before kicking off a full rebuild, you can verify all 5 source files are reachable:

```bash
python -c "
import os
from import_nuforc   import CSV_PATH as P1
from import_mufon    import CSV_PATH as P2
from import_ufocat   import CSV_PATH as P3
from import_updb     import CSV_PATH as P4
from import_geldreich import JSON_PATH as P5
for label, p in [('nuforc', P1), ('mufon', P2), ('ufocat', P3), ('updb', P4), ('ufo-search', P5)]:
    ok = 'OK ' if os.path.exists(p) else 'MISS'
    size = f'{os.path.getsize(p)/1024/1024:>7.1f} MB' if os.path.exists(p) else '       --'
    print(f'  {ok}  {size}  {os.path.normpath(p)}')
"
```

Every line should print `OK` and a non-zero size before you run `python rebuild_db.py`.

---

## A note on legal / ethical use

The UFO/UAP data ecosystem includes:
- **Public-domain** government records (Project Blue Book, Canadian/Brazilian/UK declassified files)
- **Permissively licensed** academic / community compilations (UFOCAT, UFO-search/Geldreich, PhenomAInon)
- **Private/membership-gated** databases (MUFON CMS) where redistribution is restricted

This pipeline is designed for **research reproducibility** — derived analytics, deduplication studies, and structural metadata (the kind of work in `analyze.py` and `emotions.py`). The downstream public export at `data/output/ufo_public.db` (produced by `export_public.py`) **strips all raw narrative text** and ships only derived fields, specifically to avoid republishing source-owned text content.

If you're building something on top of this pipeline, please:
1. Verify your access to each source dataset under that source's terms
2. Use the public-export pattern (drop raw narratives) for anything you publish
3. Credit the original sources in any derived work

When in doubt, contact the source organization directly. The communities behind NUFORC, MUFON, CUFOS, etc. have spent decades building this data and generally welcome research use — but they appreciate being asked.
