# Documentation Index

Reproducibility-first docs for the unified UFO sightings pipeline. Start here, then jump to whichever doc matches your goal.

## I want to…

| Goal | Read |
|---|---|
| Understand what this project is | [`../README.md`](../README.md) (top-level) |
| Reproduce the v0.13 build from scratch | [`PIPELINE.md`](PIPELINE.md) — step-by-step tutorial with expected outputs |
| Get the source data | [`../data/raw/README.md`](../data/raw/README.md) — per-source acquisition guide |
| Query the public DB | [`QUERIES.md`](QUERIES.md) — ~25 tested SQL recipes |
| Look up a column's meaning / coverage | [`SCHEMA.md`](SCHEMA.md) — column-by-column reference |
| Understand how the algorithms work | [`METHODOLOGY.md`](METHODOLOGY.md) — dedup, quality score, hoax detection, sentiment models |
| Add a new source / new analysis step | [`CONTRIBUTING.md`](CONTRIBUTING.md) — extension patterns |
| Avoid the gotchas we hit | [`PITFALLS.md`](PITFALLS.md) — known issues + workarounds |
| Cite a specific version | [`../CHANGELOG.md`](../CHANGELOG.md) — version history |

## Doc characteristics

These docs are deliberately:

- **Tested.** Every SQL query in `QUERIES.md` was run against the live `ufo_public.db`. Every command in `PIPELINE.md` was executed during a real rebuild. Every code sample in `CONTRIBUTING.md` matches the current codebase.
- **Numerically grounded.** Coverage percentages, row counts, and distribution histograms come from the actual v0.13 build (618,316 sightings, 6 sources) — not estimates. If your numbers diverge from these by more than a few hundred rows on any given metric, something probably differs in your source data snapshot.
- **Honest about gaps.** The empty columns (`topic_id`), the model artifacts (GoEmotions neutral skew, VADER positivity bias), and the privacy considerations (`witness_names`) are flagged in [`PITFALLS.md`](PITFALLS.md), not hidden.
- **Versioned.** All numbers are from v0.13. When the next version ships, these docs need re-validation.

## What's intentionally NOT in these docs

- **Per-record analysis findings.** This is a pipeline reference, not a UAP research paper. Conclusions about specific sightings, source comparisons, or temporal patterns belong in downstream work.
- **App / frontend docs.** The `ufosint-explorer` repo (separate) owns the public web app, the binary buffer format, and the chart renderers.
- **Legal / licensing guidance.** Each source dataset has its own terms — see [`../data/raw/README.md`](../data/raw/README.md) for what we know, and contact the source organization for definitive answers.
- **A LICENSE file.** Add one if you fork — that's a project-level decision the maintainers should make.

## Doc maintenance

If you make a code change that breaks a documented behavior:

| Affected doc | When to update |
|---|---|
| `SCHEMA.md` | Schema change — added/removed/renamed columns or tables |
| `QUERIES.md` | Schema change OR a recipe stops working / changes results meaningfully |
| `PIPELINE.md` | Step added/removed, command syntax changed, or expected output changed |
| `METHODOLOGY.md` | Algorithm change (rule weights, regex patterns, model swap, etc.) |
| `CONTRIBUTING.md` | Extension pattern changes (new ANALYSIS_STEPS shape, new importer convention) |
| `PITFALLS.md` | Add a new pitfall when one bites you (so the next person doesn't trip on the same thing) |
| `CHANGELOG.md` | Every version bump |

The docs are part of the code review surface — treat them as such.
