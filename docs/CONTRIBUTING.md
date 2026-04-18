# Contributing — How to Extend the Pipeline

Three common extension paths, with worked examples. Each one is tested against the actual codebase as of the v0.13 build (618,316 sightings, 6 sources).

| You want to add… | Read section… | Approximate effort |
|---|---|---|
| A new source dataset | [§1](#1-add-a-new-source-dataset) | ~2 hours (depends on source format) |
| A new analysis step | [§2](#2-add-a-new-analysis-step) | ~30 min for the function, ~30 min for tests |
| A new derived column | [§3](#3-add-a-new-derived-column) | Folds into either §1 or §2 above |

---

## 1. Add a new source dataset

Adding a 6th source (say, a hypothetical "AARO public release CSV") follows the same pattern as the existing 5 importers. Walk through:

### a. Create the importer

Copy `import_nuforc.py` as a template:

```bash
cp import_nuforc.py import_aaro.py
```

Edit:

```python
# import_aaro.py
DB_PATH = os.path.join(os.path.dirname(__file__), "ufo_unified.db")
DATA_DIR = os.environ.get(
    "UFOSINT_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw"),
)
CSV_PATH = os.path.join(DATA_DIR, "AARO", "aaro_public.csv")  # adjust filename

def parse_aaro_date(s):
    """Source-specific date parser. Return ISO 8601 or None."""
    # ... your logic here ...

def parse_aaro_location(s):
    """Source-specific location parser. Return (raw_text, city, state, country)."""
    # ... your logic here ...

def run_import():
    """Standard entry point. Called by rebuild_db.py via reflection."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # ... read CSV, insert into location and sighting tables ...
    conn.commit()
    conn.close()
```

The importer function MUST be named `run_import()` — `rebuild_db.py:run_script()` looks for that name via `getattr`.

### b. Register the source in the schema seed

Edit `create_schema.py` and add to the `sources` list:

```python
sources = [
    ("MUFON", coll_map["PUBLIUS"], ...),
    ("NUFORC", coll_map["PUBLIUS"], ...),
    ("UFOCAT", coll_map["UFOCAT"], ...),
    ("UPDB", coll_map["PUBLIUS"], ...),
    ("UFO-search", coll_map["GELDREICH"], ...),
    ("AARO", coll_map["PUBLIUS"], "AARO public release", "https://aaro.mil", None),
]
```

The source gets `id=6` automatically (autoincrement). Document this in your importer:

```python
SOURCE_DB_ID = 6  # AARO. Confirmed in create_schema.py:264-269
```

### c. Wire into rebuild_db.py

Add a step:

```python
step(7, "Import AARO public release")
run_script('import_aaro')
```

Renumber the subsequent steps. Update the final stats block to count AARO records.

### d. Update data/raw/README.md

Add an "AARO" section with acquisition instructions and expected schema.

### e. Add a test

Copy a test pattern from `tests/test_etl.py` — typically:

```python
def test_aaro_date_parsing():
    from import_aaro import parse_aaro_date
    assert parse_aaro_date("2023-08-15") == "2023-08-15"
    assert parse_aaro_date("Q3 2023") is None  # unparseable
```

### f. Update the dedup engine

If your new source overlaps with existing ones (very likely — most aggregators include NUFORC content), add a Tier 2 sub-tier in `dedup.py`:

```python
# Tier 2e: AARO ↔ MUFON/NUFORC/UFOCAT
match_pairs(SRC_AARO, SRC_MUFON, "tier2e_aaro_mufon",
            match_key=("date", "city", "state"))
# ... etc for other source pairs ...
```

The `match_pairs` helper handles candidate generation and similarity scoring; you only specify the match-key strategy.

If your source has known import-time duplicates with other sources (e.g. AARO republishes NUFORC records with an `aaro_origin=NUFORC` tag), use the same enrichment-sidecar pattern as `import_ufocat.py`:

```python
ENRICHMENT_PATH = os.path.join(os.path.dirname(__file__), "aaro_enrichment.jsonl")
# Skip rows where aaro_origin in {'NUFORC', 'MUFON'}, write to sidecar
# Then enrich.py picks up the sidecar and transfers any unique metadata
```

---

## 2. Add a new analysis step

Adding a new derived analysis (say, sky-condition extraction) leverages the `ANALYSIS_STEPS` plug-in registry:

### a. Add the column(s) to the schema

In `create_schema.py`, add to the `sighting` DDL:

```python
-- v0.12 sky conditions
sky_condition       TEXT,       -- clear|cloudy|partly_cloudy|stormy|unknown
visibility_estimate INTEGER,    -- 0-100, miles
```

And update the index list if you want it filterable:

```python
"CREATE INDEX IF NOT EXISTS idx_sighting_sky ON sighting(sky_condition)",
```

### b. Write the analysis function

In `analyze.py` (or a new module imported into it), add:

```python
SKY_PATTERNS = {
    "clear":         [r"\bclear (?:sky|night|day)\b", r"\bcloudless\b"],
    "cloudy":        [r"\bovercast\b", r"\bcloudy\b", r"\bgrey sky\b"],
    "partly_cloudy": [r"\bpartly cloudy\b", r"\bscattered clouds\b"],
    "stormy":        [r"\bstorm\b", r"\bthunder\b", r"\blightning\b", r"\brain\b"],
}
_SKY_RE = {k: [re.compile(p, re.IGNORECASE) for p in v]
           for k, v in SKY_PATTERNS.items()}


def classify_sky_conditions(conn):
    """v0.12: regex-extract sky condition from narrative text."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, COALESCE(description, summary)
        FROM sighting
        WHERE description IS NOT NULL OR summary IS NOT NULL
    """)
    rows = cur.fetchall()
    updates = []
    for sid, text in rows:
        condition = "unknown"
        for cond, patterns in _SKY_RE.items():
            if any(p.search(text) for p in patterns):
                condition = cond
                break
        updates.append((condition, sid))
    _executemany_batched(
        conn,
        "UPDATE sighting SET sky_condition = ? WHERE id = ?",
        updates,
    )
    print(f"  Sky conditions classified: {len(updates):,} rows")
```

### c. Register in ANALYSIS_STEPS

One line in `analyze.py`:

```python
ANALYSIS_STEPS = [
    ("shapes",        normalize_and_cluster_shapes, "Normalizing shapes"),
    ("movement",      classify_movement,            "Classifying movement/behavior"),
    # ... existing steps ...
    ("topic",         run_topic_modeling,           "Topic modeling"),
    ("sky",           classify_sky_conditions,      "Classifying sky conditions"),  # NEW
]
```

### d. Add the column to DERIVED_SIGHTING_COLUMNS

So `analyze.reset_analysis()` clears it on `--reset`:

```python
DERIVED_SIGHTING_COLUMNS = [
    # ... existing columns ...
    "emotion_7_joy",
    "sky_condition", "visibility_estimate",  # NEW
]
```

### e. Add a test

```python
class TestSkyConditions:
    def test_clear_sky(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="A clear night, no clouds.")
        analyze.classify_sky_conditions(conn)
        cur = conn.cursor()
        cur.execute("SELECT sky_condition FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "clear"

    def test_storm(self, analysis_db):
        conn, _ = analysis_db
        sid = _insert_sighting(conn, description="During a thunderstorm I saw...")
        analyze.classify_sky_conditions(conn)
        cur = conn.cursor()
        cur.execute("SELECT sky_condition FROM sighting WHERE id = ?", (sid,))
        assert cur.fetchone()[0] == "stormy"
```

Done. The next `python rebuild_db.py` will run your new step automatically as part of step 12.

### Where in the order should it go?

Position matters if your step depends on values another step writes. The current order:

```
1. shapes              (no deps)
2. movement            (no deps)
3. colors              (no deps)
4. sentiment           (depends on sentiment_analysis table)
5. duration            (no deps)
6. public_fields       (no deps — populates lat/lng/datetime/has_*)
7. quality             (depends on 1-6 — reads has_media, has_movement_mentioned, etc.)
8. hoax                (depends on 7 — reads richness_score)
9. topic               (no deps; STUB)
```

If your new step writes a value that the quality score should use, insert it before step 7 and add it to the quality formula. Otherwise append it at the end.

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

Should report `369 passed` (current count). Add tests for your new code.

## Things to NOT do

- **Don't put credentials in code.** Use env vars (`UFOSINT_DATA_DIR` precedent) or external secret stores.
- **Don't drop existing columns or rename them.** Add new ones; deprecate old ones. The PG migration script and the binary packer both depend on column stability.
- **Don't commit data files.** `.gitignore` excludes the obvious ones (`*.csv`, `*.json`, `*.db`); double-check before `git add`.
- **Don't change sighting IDs.** They're FK-referenced from `location`, `sighting_analysis`, `sentiment_analysis`, and (on PG) `date_correction`. ID assignment is import-order; don't shuffle.
- **Don't break existing tests.** If your change requires updating an existing test, write up *why* in the commit message — that's the audit trail for future contributors.
- **Don't add new heavy dependencies casually.** Each one increases the install footprint for everyone. `torch` was an explicit decision for v0.11; it's now optional and gated to the emotion classification step. Anything similar needs the same isolation.
