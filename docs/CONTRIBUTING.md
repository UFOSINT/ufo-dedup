# Contributing — How to Extend the Pipeline

Four common extension paths. The codebase uses the `ufosint` CLI package (`pip install -e .`) with a plugin architecture — adding new sources or analysis steps is one file + one registration line.

| You want to add... | Read section... | Approximate effort |
|---|---|---|
| A new source dataset | [§1](#1-add-a-new-source-dataset) | ~1 hour (scaffold + implement `parse_row`) |
| A new analysis step | [§2](#2-add-a-new-analysis-step) | ~30 min for the processor, ~30 min for tests |
| A new derived column | [§3](#3-add-a-new-derived-column) | Folds into either §1 or §2 above |
| A new LLM enrichment | [§4](#4-add-a-new-llm-enrichment) | ~1 hour (prompt + cache + CLI wiring) |

---

## 1. Add a new source dataset

Adding a new source (e.g., AARO) uses the `ufosint scaffold` command to generate boilerplate:

### a. Generate the importer

```bash
ufosint scaffold aaro
```

This creates `ufosint/importers/aaro.py` with a template `AaroImporter` class. Edit it:

```python
# ufosint/importers/aaro.py
class AaroImporter(Importer):
    source_name = "AARO"

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "AARO", "aaro_public.csv")

    def parse_row(self, raw):
        """Parse one CSV row into (location_dict, sighting_dict)."""
        location = {
            "raw_text": raw.get("location") or None,
            "city": raw.get("city") or None,
            "state": raw.get("state") or None,
            "country": raw.get("country") or None,
        }
        sighting = {
            "source_record_id": raw.get("case_id") or None,
            "date_event": raw.get("date") or None,
            "description": raw.get("description") or None,
            "shape": raw.get("shape") or None,
        }
        return location, sighting
```

The base class handles: reading the file, batch inserts, progress display, source_db_id lookup, location dedup, and error handling. You only implement `parse_row()`.

### b. Register the importer

Add one line to `ufosint/importers/__init__.py`:

```python
from ufosint.importers.aaro import AaroImporter
IMPORTERS["aaro"] = AaroImporter
```

### c. Register the source in the schema

Add to `create_schema.py`'s `sources` list:

```python
("AARO", coll_map["PUBLIUS"], "AARO public release", "https://aaro.mil", None),
```

### d. Place your data and run

```bash
# Place source file
cp aaro_public.csv <raw_data_dir>/AARO/aaro_public.csv

# Import
ufosint import aaro

# Verify
ufosint status
```

### e. Add a test

```python
# tests/test_ufosint_package.py (or a new test file)
class TestAaroParser:
    def test_parse_date(self):
        from ufosint.importers.aaro import parse_aaro_date
        assert parse_aaro_date("2023-08-15") == ("2023-08-15", "2023-08-15")
```

### f. Update dedup (if source overlaps with existing ones)

If your source overlaps with NUFORC/MUFON, add a Tier 2 sub-tier in `dedup.py`. Or use the enrichment-sidecar pattern from `ufosint/importers/ufocat.py` (override `should_skip_row()` and `on_skip()`).

---

## 2. Add a new analysis step

Adding a new processor (e.g., sky-condition extraction) uses the `Processor` base class:

### a. Add the column(s) to the schema

In `create_schema.py`, add to the `sighting` DDL:

```sql
sky_condition       TEXT,       -- clear|cloudy|partly_cloudy|stormy|unknown
```

### b. Create the processor

Create `ufosint/processors/sky.py`:

```python
from ufosint.processors.base import Processor, executemany_batched
import re

SKY_PATTERNS = {
    "clear":   [r"\bclear (?:sky|night|day)\b", r"\bcloudless\b"],
    "cloudy":  [r"\bovercast\b", r"\bcloudy\b"],
    "stormy":  [r"\bstorm\b", r"\bthunder\b", r"\blightning\b"],
}
_SKY_RE = {k: [re.compile(p, re.I) for p in v] for k, v in SKY_PATTERNS.items()}

class SkyClassifier(Processor):
    name = "sky"
    label = "Classifying sky conditions"

    def process(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT id, COALESCE(description, summary) FROM sighting WHERE description IS NOT NULL")
        updates = []
        for sid, text in cur.fetchall():
            condition = "unknown"
            for cond, patterns in _SKY_RE.items():
                if any(p.search(text) for p in patterns):
                    condition = cond
                    break
            updates.append((condition, sid))
        executemany_batched(conn, "UPDATE sighting SET sky_condition = ? WHERE id = ?", updates)
        print(f"  Sky conditions classified: {len(updates):,} rows")
```

### c. Register in the processor registry

One line in `ufosint/processors/__init__.py`:

```python
from ufosint.processors.sky import SkyClassifier
PROCESSORS["sky"] = SkyClassifier
```

### d. Add a test

```python
class TestSkyClassifier:
    def test_clear_sky(self):
        from ufosint.processors.sky import SkyClassifier
        # ... set up in-memory DB, insert row, run processor, assert
```

### e. Run it

```bash
ufosint analyze sky          # run just your new processor
ufosint analyze --list       # verify it shows up
ufosint analyze              # run all (yours included)
```

### Processor execution order

Position matters if your step depends on values another step writes:

```bash
$ ufosint analyze --list

  shapes               Normalizing shapes                  deps: none
  movement             Classifying movement/behavior       deps: none
  colors               Extracting colors                   deps: none
  sentiment_derive     Deriving sentiment summary          deps: none
  duration             Parsing durations and bucketing     deps: none
  public_fields        Deriving public fields              deps: none
  quality              Calculating quality score           deps: shapes, movement, ...
  hoax                 Flagging potential hoaxes           deps: quality
  topic                Topic modeling                      deps: none
```

If your processor writes a value the quality score should use, set `depends_on = []` and place it before `quality` in the registry. The `quality` processor's `depends_on` list determines what must run first.

---

## 3. Add a new derived column

Two scenarios:

### Scenario A: derived from raw data → add via §2 above

Most cases. Add a column, write an `analyze.py` function, register the step.

### Scenario B: derived from JSON in sighting_analysis (no schema change)

If you only need a JSON side-field, write to `sighting_analysis` instead:

```python
def my_new_jsonfield(conn):
    cur = conn.cursor()
    cur.execute("SELECT id FROM sighting")
    updates = []
    for (sid,) in cur.fetchall():
        # ... compute some structured object ...
        updates.append((json.dumps(my_obj), sid))
    _executemany_batched(
        conn,
        "UPDATE sighting_analysis SET my_field = ? WHERE sighting_id = ?",
        updates,
    )
```

Then add `my_field` to the `sighting_analysis` DDL in `create_schema.py`. Doesn't affect the public binary buffer (it's a side table, not on `sighting`), so the dev team's app changes are zero.

---

## 4. Add a new LLM enrichment

LLM enrichments follow the **cache + replay** pattern so expensive API calls are one-time:

### a. Write your enrichment logic

Use the shared `LLMClient` and `ResultCache`:

```python
from ufosint.llm import LLMClient, ResultCache
from ufosint.llm.prompts import MY_SYSTEM_PROMPT

cache = ResultCache("my_enrichment", columns=["sighting_id", "result_field"])
client = LLMClient(workers=15)

# Process records, cache results
results = client.batch_process(items, build_prompt, parse_response)
cache.append_rows([{"sighting_id": sid, "result_field": val} for ...])
```

### b. Add a replay function

```python
def replay_my_enrichment(db_path):
    rows = cache.load()
    # Apply rows to DB (NULL fields only)
```

### c. Wire into the pipeline

Add to `ufosint/pipeline.py` in `_step_replay()` — cached results are automatically replayed on rebuild.

### d. Add a CLI command (optional)

```python
# In ufosint/cli.py
@main.command("my-enrichment")
def my_enrichment():
    """Run my LLM enrichment."""
    ...
```

**Key principle:** the live LLM run produces a cache CSV. The cache CSV is replayed on every rebuild. No API key needed for reproduction.

---

## Wiring into Postgres for the public app

Local SQLite changes don't automatically appear on the Azure Postgres deployment. To get a new column live:

### a. Write a PG migration

In `ufosint-explorer/scripts/`, create `add_v012_sky_columns.sql`:

```sql
ALTER TABLE sighting
    ADD COLUMN IF NOT EXISTS sky_condition       TEXT,
    ADD COLUMN IF NOT EXISTS visibility_estimate SMALLINT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sighting_sky
    ON sighting(sky_condition);
```

Idempotent (`ADD COLUMN IF NOT EXISTS`) and safe to run on a live DB (`CREATE INDEX CONCURRENTLY`).

### b. Update migrate_sqlite_to_pg.py

Add the column names to the `sighting` row in the `TABLES` list. The column-probe in `copy_table()` handles missing columns gracefully — old PG schemas just skip the new fields.

### c. Hand off to dev team

Ping the dev team with: (1) the SQL migration to apply, (2) the updated `migrate_sqlite_to_pg.py`, (3) a fresh `ufo_public.db` from your rebuild. Same handoff pattern documented for the v0.8.3 and v0.11 releases.

---

## Code style

Match what's already there. The repo conventions are minimal but consistent:

- **No type annotations** in core ETL modules — kept readable by domain experts who aren't Python pros
- **Module-level constants in UPPER_SNAKE_CASE**, helpers in `_lowercase_underscore` if private
- **Docstrings for public functions** — explain *why*, not what (the code already shows what)
- **Print to stdout for progress**, no logging library — these are operator-run scripts, not services
- **Commit per-batch** in long-running loops — protects against mid-run kills (this was a v0.11 bug fix)
- **Tests are functional, not unit**: each test inserts data and asserts on derived output
- **Don't break idempotency**: every `analyze.py` step must produce the same output if re-run on a row that already has values

## Tests

Run before committing:

```bash
pytest tests/ -q
```

Should report `446 passed` (current count). Add tests for your new code.

## Things to NOT do

- **Don't put credentials in code.** Use env vars (`UFOSINT_DATA_DIR` precedent) or external secret stores.
- **Don't drop existing columns or rename them.** Add new ones; deprecate old ones. The PG migration script and the binary packer both depend on column stability.
- **Don't commit data files.** `.gitignore` excludes the obvious ones (`*.csv`, `*.json`, `*.db`); double-check before `git add`.
- **Don't change sighting IDs.** They're FK-referenced from `location`, `sighting_analysis`, `sentiment_analysis`, and (on PG) `date_correction`. ID assignment is import-order; don't shuffle.
- **Don't break existing tests.** If your change requires updating an existing test, write up *why* in the commit message — that's the audit trail for future contributors.
- **Don't add new heavy dependencies casually.** Each one increases the install footprint for everyone. `torch` was an explicit decision for v0.11; it's now optional and gated to the emotion classification step. Anything similar needs the same isolation.
