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

## ⚠️ The SQLite files are out of sync with production

**Check this before running any reload.** In v0.16 the live Postgres was purged
of the `mufon.csv` import (138,310 sightings) and all r/UFOs records (3,811).
**Those rows are still present in both SQLite files here** — they still report
618,316 sightings across six sources; production serves 476,195 across four.

Consequences you must not walk into:

- Running `migrate_sqlite_to_pg.py` or `reload_from_public_db.py` against
  production **will resurrect MUFON and Reddit** and undo the purge.
- The published `ufo_public.db` release asset is likewise stale. The site's
  download card says so explicitly; keep that caveat accurate.

Until the same purge is applied here, treat the SQLite files as *older* than
production, not as the source of truth for row membership. The web app's
`scripts/purge_mufon_csv_and_reddit.sql` documents exactly what was removed and
on what predicate (`source_db_id`, never `origin_id`).

## Layout

```
ufosint/
  config.py            Paths + settings (ufosint.toml). Config.db_path() is canonical.
  db.py                SQLite connection manager. Single source of truth for connections.
  pipeline.py          The 17-step rebuild orchestration.
  importers/           One module per source: nuforc, ufocat, updb, geldreich, mufon, reddit
  processors/          Enrichment passes: geocoder, dedup, emotions, movement, colors,
                       duration, hoax, nuclear, country
  llm/                 LLM extraction + audit (cached; see data/output/*.csv)
  export/public_db.py  Builds the stripped public export
scripts/               One-shot operational scripts
geodata/               GeoNames gazetteers (cities1000 / 5000 / 15000)
tests/                 pytest; 477 tests, all offline
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
