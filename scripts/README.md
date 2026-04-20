# Standalone Scripts

Utility scripts that are NOT part of the core `ufosint` pipeline but are useful for specific tasks. These run independently and are not imported by the `ufosint/` package.

## Reddit Pipeline

| Script | Description |
|---|---|
| `scrape_reddit.py` | Pass 1: scrape r/UFOs posts via Reddit JSON API / PRAW |
| `extract_reddit.py` | Pass 2: LLM-extract structured fields from raw posts via OpenRouter |

The Reddit import itself (Pass 3) is part of the core pipeline: `ufosint import reddit`

## Data Quality Tools

| Script | Description |
|---|---|
| `enrich_free_wins.py` | Preview/staging tool for developing new duration parsers and shape mappings before merging into `ufosint/processors/` |
| `fix_coords.py` | One-off coordinate validation and repair (superseded by `ufosint audit`) |

## Utilities

| Script | Description |
|---|---|
| `db_summary.py` | Quick database statistics report (superseded by `ufosint status`) |
| `extract_historic.py` | Extract pre-1901 sightings to a separate DB for historical analysis |
