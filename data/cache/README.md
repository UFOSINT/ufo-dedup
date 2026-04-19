# LLM / ML Inference Cache

This directory stores cached inference results from expensive LLM or ML operations so they can be replayed on future rebuilds without re-calling APIs or re-running GPU inference.

## Current cache files

| Source | Cache location | Notes |
|---|---|---|
| Tier B location normalization | `../output/audit_tier_b_fixes.csv` | Stored in output/ because it's also a deliverable |
| Reddit LLM extraction | `../raw/reddit/extracted/*.json` | One JSON per post |
| Emotion classification | Stored in-DB (sighting columns) | `emotions.py` skips non-NULL rows |

## Future cache candidates

When adding new LLM/ML processing steps, follow this pattern:

1. **Run inference** and store results in the DB
2. **Export a replay CSV** to `data/output/` or `data/cache/`
3. **Add a replay function** in the module (see `audit.py:replay_tier_b()`)
4. **Wire replay into `rebuild_db.py`** so fresh builds use the cache

This ensures the pipeline is:
- **Reproducible** — anyone can rebuild from raw data + cached inference
- **Economical** — LLM costs are one-time, not per-rebuild
- **Transparent** — cached results are inspectable CSV/JSON files
