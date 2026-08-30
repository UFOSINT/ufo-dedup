# CLAUDE.md — Agent handoff for `ufo-dedup`

If you're a Claude Code session starting cold in this repo, read this first.

## What this repo is

The **ETL and deduplication pipeline** that builds the unified UFO sightings
database. It ingests raw source datasets, deduplicates them, enriches them
(geocoding, emotion analysis, LLM extraction, derived scores), and exports
two SQLite files:

| File | What it is |
|------|------------|
| `data/output/ufo_unified.db` | Canonical working database. Includes raw narrative text. Never published. |
| `data/output/ufo_public.db` | Stripped export — derived fields only, no licensed raw text. This is what ships on GitHub Releases. |

**This repo is SQLite-only.** Nothing here connects to Postgres, and that's
deliberate — keep it that way. The live site's Postgres is loaded *from* these
files by the sibling repo's `scripts/migrate_sqlite_to_pg.py` and
`scripts/reload_from_public_db.py`. If you need to get a derived column into
production, emit a portable artifact (CSV keyed by the row id) and let the web
app apply it, rather than reaching into PG from here. Location and sighting ids
are copied verbatim across the boundary, so id-keyed files load cleanly.

## Ownership

Both this repo and [`ufosint-explorer`](https://github.com/UFOSINT/ufosint-explorer)
are owned by the same person (as of 2026-08-19). Earlier handoff notes described
a split where a separate agent owned each side and cross-repo edits needed
explicit permission — **that split no longer applies.** You may edit both.

The sibling repo's `CLAUDE.md` still carries the old two-owner wording and
Windows-era paths (`C:/dev/dg/UFOSINT/`); it needs the same correction.

## SQLite / production parity

Both SQLite files were purged to match production on 2026-08-19 and now agree
with it: **476,195 sightings across four sources** (UFOCAT 197,108 / NUFORC
159,320 / UPDB 65,016 / UFO-search 54,751). A reload via the sibling repo's
`migrate_sqlite_to_pg.py` or `reload_from_public_db.py` is safe again.

Re-runnable via `scripts/purge_mufon_csv_and_reddit.py` (dry run by default).
Pre-purge backups sit beside the databases as `*.db.pre-v016-purge`; they are
gitignored and can be deleted once you're confident.

**Still stale:** the published `ufo_public.db` release asset on GitHub is the
old 618,316-row build. Attach a rebuilt file to the next tagged release.

Two shape differences from the Postgres purge, both handled in that script:

- SQLite has a `sighting_analysis` table Postgres lacks, and `ufo_public.db`
  lacks `duplicate_candidate` and `sentiment_analysis`.
- **`r/UFOs` sits in the `PUBLIUS` collection here.** The separate `Reddit`
  collection only ever existed in Postgres, seeded by
  `add_v013_reddit_columns.sql`. Matching on collection name — as the SQL
  version does — finds only one of the two targets.

## Layout

```
ufosint/
  config.py            Paths + settings (ufosint.toml). Config.db_path() is canonical.
  db.py                SQLite connection manager. Single source of truth for connections.
  pipeline.py          The 15-step rebuild orchestration.
  importers/           nuforc, ufocat, updb, geldreich (+ mufon, reddit: retired,
                       registered but not in the default pipeline)
  processors/          Enrichment passes: geocoder, dedup, emotions, movement, colors,
                       duration, hoax, nuclear, country
  llm/                 LLM extraction + audit (cached; see data/output/*.csv)
  export/public_db.py  Builds the stripped public export
scripts/               One-shot operational scripts
geodata/               GeoNames gazetteers (cities1000 / 5000 / 15000)
tests/                 pytest; 499 tests, all offline
```

The root-level `import_*.py` and `analyze.py` files are thin wrappers kept for
backwards compatibility with the tests — the real code lives in `ufosint/`.

## Country coding

`location.country` is **not** a usable country field. It holds 715 distinct
values across the corpus: ISO-2 codes and full names for the same country
(India appears as both `IN` and `India`), US and Canadian state codes (`TX`,
`FL`, `NY`, `MB`), cities, oceans, placeholders (`Unspecified`,
`International Waters`), CSV parse fragments (`approximately)`, `@ Main St.)`),
and `Mars`. Only 41.2% of mapped sightings carry a usable one.

Use `location.country_iso2` instead, derived from coordinates by
`ufosint/processors/country.py` and backfilled with
`scripts/backfill_country.py`. That covers 95.7% of mapped rows.

**Offshore and border sightings are deliberately left NULL.** Nearest-city is
an approximation of point-in-polygon and is unreliable in exactly those two
cases, so the module declines to answer rather than inventing a country:

- **offshore** — nearest populated place further than `OFFSHORE_KM` (100 km)
- **border** — a place in another country within `BORDER_MARGIN_KM` (25 km)
  of the nearest one

A NULL from this path means *"we decline to say"*, which is a stronger and more
honest claim than *"unknown"*. `tests/test_country.py` pins the refusals; a
change that raises coverage by answering in those cases is a regression.

**`normalize_country()` in `processors/geocoder.py` has a known bug:** it
short-circuits on `if len(raw) == 2: return raw`, so any two-letter string is
treated as a valid ISO code and `TX`, `FL`, `NY` become "countries". It is only
safe on the text-fallback path after that is fixed to validate against real
ISO-3166. Don't derive that validation list from the gazetteer — it has no
cities in Antarctica and so wrongly rejects `AQ`.

## Aggregator origin retention

UPDB and UFOCAT are **aggregators** — they carry cases other bodies originally
reported, recorded in UPDB's `name` column and surfaced as
`sighting.origin_id` / `origin_record_id`.

Skipping an aggregator's row is only correct when we import that body's own
richer dataset. When we don't, the skip stops being deduplication and becomes
deletion.

That is what happened to MUFON: `mufon.csv` was retired in v0.16, but UPDB
kept skipping MUFON-origin rows on the old rationale, so MUFON coverage from
UPDB fell to zero instead of falling back to the aggregator copy.

**The rule:** an aggregator skips an origin **only if** that origin appears in
`DIRECTLY_IMPORTED_ORIGINS` (`ufosint/importers/base.py`), which must stay in
step with `STEPS` in `ufosint/pipeline.py`. `tests/test_origins.py` asserts
the two agree — the failure mode is silent, so it needs a test rather than
discipline.

Retained rows are labelled: `parse_row` sets `origin_name`, and
`Importer._flush_batch` resolves it to the `source_origin` FK. That is what
keeps "MUFON via UPDB" distinguishable from the retired `mufon.csv` import —
the distinction the v0.16 purge keyed on (`source_db_id`, never `origin_id`).

`mufon` and `reddit` are no longer default pipeline steps. Their importer
classes remain available via `get_importer()` for deliberate one-off use.

## Conventions

- **Comments explain _why_, not _what_.**
- Tests are offline and must stay that way — no network calls, no live DB.
  The gazetteer is committed; use it rather than downloading.
- Long-running passes write a `.log` at the repo root and are resumable;
  follow that pattern for anything that takes more than a few minutes.
- Processors go in `ufosint/processors/` and expose pure, testable functions
  separate from their DB-writing runner.
- One-shot operational work goes in `scripts/`, defaults to a dry run, and
  requires an explicit `--apply` to mutate anything.

## Before you act on a recalled memory

Memory may be stale — branch names, row counts and file lists all move. Verify
with `git status` / `git log` and a live query before repeating any claim about
current state. Row counts in particular have changed twice recently.
