# Refactor Plan: `ufo-dedup` → `ufosint` CLI Package

**Status:** Planning
**Branch:** `refactor/ufosint-cli` (from `main` at `b289e75`)
**Goal:** Transform 26 standalone scripts into a single installable Python package with a unified CLI, shared infrastructure, and plugin architecture.

---

## Why

The current codebase works but has accumulated significant technical debt:

- **21 files** define their own `DB_PATH` — one path change requires editing 21 files
- **13 different CLIs** a user must discover (`rebuild_db.py`, `audit.py`, `run_audit.py`, `emotions.py`, `export_public.py`...)
- **6 different entry-point conventions** (`run_import`, `run_analysis`, `main`, `run_emotions`...)
- **Copy-pasted infrastructure** — LLM calling code in 3 places, progress bars in 2, CSV caching in 3
- **No configuration file** — paths, models, batch sizes all hardcoded as module constants
- **No shared DB connection management** — each script opens/closes its own connection
- **11,294 lines** with no package structure — everything flat in the repo root

## Principles

1. **Don't break the data.** All `data/` files (databases, caches, Reddit artifacts, geodata) must survive untouched.
2. **One command to rule them all.** `ufosint <verb>` replaces 13 separate scripts.
3. **Plugin architecture.** Adding a new source or analysis step = one file + one registration line.
4. **Config over constants.** Paths, models, batch sizes in `ufosint.toml`, overridable by env vars.
5. **Cache everything expensive.** LLM calls, GPU inference, geocoding results — all cached and replayable.
6. **Backward compatible.** The old scripts (`rebuild_db.py`, `analyze.py`, etc.) become thin wrappers during transition.

---

## Target Architecture

```
ufo-dedup/
├── ufosint/                      # Python package
│   ├── __init__.py               # version, package metadata
│   ├── cli.py                    # Click-based CLI: `ufosint <command>`
│   ├── config.py                 # Central config (ufosint.toml + env vars)
│   ├── db.py                     # Database manager (connection pool, schema, migrations)
│   ├── pipeline.py               # Step registry + DAG runner
│   │
│   ├── importers/                # One module per source
│   │   ├── __init__.py           # Importer registry
│   │   ├── base.py               # class Importer(ABC): parse_row, run
│   │   ├── nuforc.py             # class NuforcImporter(Importer)
│   │   ├── mufon.py
│   │   ├── ufocat.py
│   │   ├── updb.py
│   │   ├── geldreich.py
│   │   └── reddit.py
│   │
│   ├── processors/               # Analysis + enrichment steps
│   │   ├── __init__.py           # Processor registry
│   │   ├── base.py               # class Processor(ABC): run, reset, status
│   │   ├── geocoder.py           # GeoNames geocoding (both passes)
│   │   ├── dedup.py              # Three-tier deduplication
│   │   ├── sentiment.py          # VADER + NRCLex
│   │   ├── shapes.py             # Shape normalization (canonical list + aliases)
│   │   ├── movement.py           # Movement/behavior classification
│   │   ├── colors.py             # Color extraction
│   │   ├── duration.py           # Duration text parser + bucketing
│   │   ├── quality.py            # Quality score formula
│   │   ├── hoax.py               # Hoax flag detection
│   │   ├── public_fields.py      # Denormalization (lat/lng, datetime, has_*)
│   │   ├── emotions.py           # GPU transformer classification
│   │   ├── nuclear.py            # Gerb overlay + nuclear proximity
│   │   └── enrich_nuforc.py      # UFOCAT → NUFORC metadata transfer
│   │
│   ├── llm/                      # Shared LLM infrastructure
│   │   ├── __init__.py
│   │   ├── client.py             # OpenRouter client (retry, rate limit, parallel)
│   │   ├── cache.py              # CSV-based cache + replay mechanism
│   │   ├── prompts.py            # All system/user prompts in one place
│   │   ├── audit.py              # Tier A/B/C audit logic
│   │   └── extractor.py          # Field extraction from descriptions
│   │
│   ├── export/                   # Output generation
│   │   ├── __init__.py
│   │   ├── public_db.py          # Clean public SQLite export
│   │   ├── spreadsheet.py        # Excel/CSV exports
│   │   └── handoff.py            # Dev team handoff generator
│   │
│   └── display/                  # CLI display components (reusable)
│       ├── __init__.py
│       ├── dashboard.py          # Live-updating dashboard (ANSI)
│       ├── progress.py           # Progress bars
│       └── log.py                # Dual file+stdout logger
│
├── data/                         # Data directory (NOT in git, scaffolding only)
│   ├── README.md                 # ✓ tracked
│   ├── raw/                      # Source datasets
│   │   ├── README.md             # ✓ tracked
│   │   └── reddit/               # Reddit extraction artifacts
│   ├── output/                   # Pipeline outputs + caches
│   │   ├── README.md             # ✓ tracked
│   │   ├── ufo_unified.db        # ✗ gitignored
│   │   ├── ufo_public.db         # ✗ gitignored
│   │   └── *.csv                 # ✗ gitignored (LLM caches)
│   ├── cache/                    # Inference cache
│   │   └── README.md             # ✓ tracked
│   └── models/                   # Local model weights
│       └── README.md             # ✓ tracked
│
├── tests/                        # Test suite
│   ├── conftest.py               # Shared fixtures
│   ├── test_importers.py         # Replaces test_etl.py
│   ├── test_processors.py        # Replaces test_analyze.py
│   ├── test_quality.py           # Replaces test_data_quality.py
│   ├── test_dedup.py             # Stays
│   └── test_llm.py               # New: LLM cache/replay tests
│
├── docs/                         # Documentation
│   ├── PIPELINE.md
│   ├── SCHEMA.md
│   ├── METHODOLOGY.md
│   ├── QUERIES.md
│   ├── CONTRIBUTING.md
│   └── PITFALLS.md
│
├── ufosint.toml                  # Central configuration
├── pyproject.toml                # Package definition (pip installable)
├── requirements.txt              # Core deps
├── requirements-gpu.txt          # PyTorch + transformers (optional)
├── requirements-llm.txt          # requests (for OpenRouter)
├── CHANGELOG.md
├── README.md
└── .gitignore
```

---

## Key Design Decisions

### 1. Configuration: `ufosint.toml`

```toml
[paths]
db = "data/output/ufo_unified.db"
public_db = "data/output/ufo_public.db"
raw_data = "../data/raw"           # overridden by UFOSINT_DATA_DIR env var
geodata = "data/geodata"
cache = "data/output"

[llm]
model = "google/gemini-2.0-flash-001"
workers = 15
batch_size = 25
# API key from OPENROUTER_API_KEY env var (never in config file)

[gpu]
batch_size = 64
device = "auto"                    # auto|cuda|cpu

[pipeline]
unknown_date_cap = 15
unknown_date_cap_rich = 35
fuzzy_shape_cutoff = 85
```

### 2. Database manager: `db.py`

```python
class Database:
    """Single source of truth for DB connections and schema."""

    def __init__(self, path=None):
        self.path = path or Config.get("paths.db")

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def ensure_schema(self):
        """Create tables + indexes if missing (idempotent)."""

    def migrate_to(self, version):
        """Apply schema migrations up to version."""

    def status(self) -> dict:
        """Return coverage stats for all major columns."""
```

### 3. Pipeline step registry: `pipeline.py`

```python
class Step:
    name: str
    function: Callable
    depends_on: list[str]
    skip_flag: str              # --skip-{name}
    cacheable: bool             # has CSV cache for replay

PIPELINE = [
    Step("schema",    create_schema,       depends_on=[]),
    Step("ufocat",    UfocatImporter,      depends_on=["schema"]),
    Step("nuforc",    NuforcImporter,      depends_on=["schema"]),
    Step("mufon",     MufonImporter,       depends_on=["schema"]),
    Step("updb",      UpdbImporter,        depends_on=["schema"]),
    Step("geldreich", GeldreichImporter,   depends_on=["schema"]),
    Step("reddit",    RedditImporter,      depends_on=["schema"]),
    Step("fixes",     DataFixer,           depends_on=["ufocat","nuforc","mufon","updb","geldreich"]),
    Step("geocode1",  Geocoder,            depends_on=["fixes"]),
    Step("audit",     AuditPipeline,       depends_on=["geocode1"],  cacheable=True),
    Step("geocode2",  Geocoder,            depends_on=["audit"]),
    Step("enrich",    NuforcEnricher,      depends_on=["ufocat","nuforc"]),
    Step("dedup",     Deduplicator,        depends_on=["geocode2"]),
    Step("sentiment", SentimentAnalyzer,   depends_on=["fixes"]),
    Step("analyze",   DerivedAnalysis,     depends_on=["sentiment","geocode2","dedup"]),
    Step("replay",    CacheReplayer,       depends_on=["analyze"],   cacheable=True),
    Step("export",    PublicExporter,      depends_on=["replay"]),
]

def run_pipeline(from_step=None, skip=None):
    """Execute pipeline steps in dependency order."""
```

### 4. Base classes

```python
# importers/base.py
class Importer(ABC):
    name: str                    # e.g. "nuforc"
    source_db_name: str          # e.g. "NUFORC"

    @abstractmethod
    def parse_row(self, raw: dict) -> tuple[dict, dict]:
        """Parse one raw row into (sighting_dict, location_dict)."""

    def run(self, db: Database):
        """Standard import loop: read source, parse rows, batch insert."""
        # Shared logic: progress bar, batch commits, stats

# processors/base.py
class Processor(ABC):
    name: str
    depends_on: list[str]

    @abstractmethod
    def process(self, db: Database):
        """Run this processing step."""

    def status(self, db: Database) -> dict:
        """Return coverage stats for this processor's output."""

    def reset(self, db: Database):
        """Clear this processor's output columns."""
```

### 5. LLM client: `llm/client.py`

```python
class LLMClient:
    """Shared OpenRouter client with retry, rate limiting, parallel workers."""

    def __init__(self, model=None, workers=None):
        self.model = model or Config.get("llm.model")
        self.workers = workers or Config.get("llm.workers")
        self.api_key = os.environ["OPENROUTER_API_KEY"]

    def batch_process(self, items, prompt_fn, parse_fn, on_batch_done=None):
        """Process items in parallel batches. Thread-safe.

        Args:
            items: list of dicts to process
            prompt_fn: items -> messages list
            parse_fn: response_text -> parsed results
            on_batch_done: callback(batch_results) for dashboard updates
        """

    def _call(self, messages) -> str:
        """Single API call with retry."""
```

### 6. Cache system: `llm/cache.py`

```python
class ResultCache:
    """CSV-backed cache for expensive operations. Replay on rebuild."""

    def __init__(self, name, columns, path=None):
        self.name = name                    # e.g. "audit_tier_b"
        self.path = path or Config.cache_path(f"{name}.csv")

    def has_cache(self) -> bool: ...
    def load_seen_ids(self) -> set: ...
    def append(self, rows): ...
    def replay(self, db: Database): ...
    def export(self, db: Database): ...
```

---

## CLI Commands

```bash
# === Pipeline ===
ufosint rebuild                          # full 17-step pipeline
ufosint rebuild --from geocode           # resume from step
ufosint rebuild --skip emotions,dedup    # skip steps
ufosint rebuild --only analyze           # single step

# === Import ===
ufosint import nuforc                    # single source
ufosint import --all                     # all sources
ufosint import reddit --csv path/to/csv  # custom path

# === Processing ===
ufosint geocode                          # run geocoding
ufosint analyze                          # derived analysis
ufosint emotions                         # GPU emotion classification
ufosint sentiment                        # VADER + NRC

# === LLM Operations ===
ufosint audit --tier b --workers 15      # location normalization
ufosint enrich --limit 5000 --workers 10 # field extraction
ufosint spot-check --count 500           # quality grading

# === Export ===
ufosint export public                    # clean public DB
ufosint export handoff                   # dev team package
ufosint export csv sightings             # CSV dump

# === Status ===
ufosint status                           # full DB status dashboard
ufosint status --columns                 # column coverage report
ufosint status --cache                   # LLM cache inventory

# === Cache ===
ufosint cache list                       # show all cached results
ufosint cache replay                     # replay all caches
ufosint cache export emotions            # export emotion data to cache
```

---

## Migration Strategy (6 phases)

### Phase 1: Scaffold + config (no behavior change)
**Files:** `ufosint/__init__.py`, `config.py`, `db.py`, `pyproject.toml`, `ufosint.toml`
**Test:** `import ufosint` works, `Config.get("paths.db")` returns correct path
**Risk:** Zero — no existing code touched

### Phase 2: CLI shell + status command
**Files:** `cli.py`, `display/dashboard.py`, `display/log.py`
**Test:** `ufosint status` shows DB coverage stats
**Risk:** Zero — additive only, old scripts still work

### Phase 3: Migrate importers
**Files:** `importers/base.py`, one file per source
**Test:** `ufosint import nuforc` produces same row count as `python import_nuforc.py`
**Approach:** Extract logic from existing files into class methods. Old files become:
```python
# import_nuforc.py (backward compat wrapper)
from ufosint.importers.nuforc import NuforcImporter
from ufosint.db import Database
if __name__ == "__main__":
    NuforcImporter().run(Database())
```
**Risk:** Low — each importer is independent, testable in isolation

### Phase 4: Migrate processors
**Files:** `processors/base.py`, split `analyze.py` (1,317 lines) into 8 focused modules
**Test:** `ufosint analyze` produces same quality scores ±0
**Approach:** Each `ANALYSIS_STEPS` entry becomes its own `Processor` subclass
**Risk:** Medium — quality score formula must be exactly preserved. Test with diff.

### Phase 5: Migrate LLM tools
**Files:** `llm/client.py`, `llm/cache.py`, `llm/audit.py`, `llm/extractor.py`
**Test:** `ufosint audit --tier b --limit 10` produces same output format
**Approach:** Extract shared code from `audit.py` (1,148 lines), `run_audit.py`, `run_enrich.py`
**Risk:** Low — LLM prompts stay identical, only infrastructure changes

### Phase 6: Pipeline runner + cleanup
**Files:** `pipeline.py`, update `cli.py` with all commands
**Test:** `ufosint rebuild` produces identical DB to `python rebuild_db.py`
**Approach:** Wire all steps into the DAG runner. Delete old wrapper scripts.
**Risk:** Low if phases 3-5 are solid

---

## Data Protection Checklist

Files that must NEVER be committed and must survive the refactor:

```
# Databases (~2.6 GB)
data/output/ufo_unified.db
data/output/ufo_public.db

# LLM caches (~87 MB total — these ARE the expensive outputs)
data/output/audit_tier_b_fixes.csv        # $3 of LLM work
data/output/audit_tier_b_results.csv
data/output/emotion_classification_cache.csv  # 35 min of GPU
data/output/llm_field_extractions.csv     # $8 of LLM work
data/output/spot_check_results.csv

# Reddit pipeline artifacts
data/raw/reddit/raw/*.json               # 4,695 scraped posts
data/raw/reddit/extracted/*.json          # 4,695 LLM extractions
data/raw/reddit/reddit_sightings_extracted.csv
data/raw/reddit/work_queue.json
data/raw/reddit/extract_log.json
data/raw/reddit/scrape_log.json

# Geocoder data (~51 MB)
geodata/cities1000.txt
geodata/cities5000.txt
geodata/cities15000.txt

# UFOCAT enrichment sidecar
ufocat_enrichment.jsonl

# Build logs (historical record)
*.log
```

### .gitignore (updated for new structure)

```gitignore
# Data files
*.db
*.db-wal
*.db-shm
*.csv
*.jsonl
*.json
*.xlsx
*.accdb
*.zip
*.gz
*.parquet

# Keep README scaffolding everywhere
!**/README.md

# Geodata (downloaded at runtime)
data/geodata/

# Reddit raw data
data/raw/reddit/raw/
data/raw/reddit/extracted/

# Model weights
data/models/*.bin
data/models/*.pt
data/models/*.safetensors

# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
dist/
build/

# Environment
.env

# Logs
*.log

# Legacy
ufo-explorer/
geodata/
```

---

## pyproject.toml

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "ufosint"
version = "0.14.0"
description = "Unified UFO/UAP sighting analysis pipeline"
requires-python = ">=3.10"
dependencies = [
    "vaderSentiment",
    "NRCLex",
    "rapidfuzz>=3.0",
    "click>=8.0",
    "tomli>=2.0; python_version < '3.11'",
]

[project.optional-dependencies]
gpu = ["torch", "transformers"]
llm = ["requests"]
dev = ["pytest", "ruff"]

[project.scripts]
ufosint = "ufosint.cli:main"

[tool.setuptools.packages.find]
include = ["ufosint*"]
```

---

## Definition of Done

- [ ] `pip install -e .` installs the package
- [ ] `ufosint rebuild` produces identical DB to current `python rebuild_db.py`
- [ ] `ufosint status` shows full coverage dashboard
- [ ] `ufosint audit --tier b --limit 10` works with API key
- [ ] `ufosint enrich --limit 10` works with API key
- [ ] `ufosint export public` produces identical public DB
- [ ] All 369 existing tests pass (adapted to new import paths)
- [ ] New tests for config, db, pipeline, cache, LLM client
- [ ] No data files committed to git
- [ ] All data files survive the refactor
- [ ] Old scripts (`rebuild_db.py`, `analyze.py`, etc.) still work as thin wrappers
- [ ] README updated with new CLI usage
- [ ] CONTRIBUTING.md updated with new extension patterns

---

## Estimated Effort

| Phase | Scope | Effort |
|-------|-------|--------|
| 1. Scaffold + config | 4 new files | 1-2 hours |
| 2. CLI + status | 3 new files | 2-3 hours |
| 3. Importers | 8 files (6 sources + base + registry) | 3-4 hours |
| 4. Processors | 10 files (8 processors + base + registry) | 4-6 hours |
| 5. LLM tools | 5 files | 3-4 hours |
| 6. Pipeline + cleanup | 2 files + delete old scripts | 2-3 hours |
| **Total** | **~32 files** | **~15-22 hours** |

This can be done incrementally — each phase is independently deployable and testable. Phase 1-2 can ship immediately without breaking anything.

---

## Branch Strategy

```
main (b289e75)
  |
  +-- refactor/ufosint-cli
        |
        +-- Phase 1: scaffold + config
        +-- Phase 2: CLI + status
        +-- Phase 3: importers (one commit per source)
        +-- Phase 4: processors (one commit per step)
        +-- Phase 5: LLM tools
        +-- Phase 6: pipeline runner
        |
        +-- Squash merge back to main when all tests pass
```

Before branching, commit all current v0.14 work to `main` (the audit pipeline, LLM extraction, duration/shape fixes, new files). Then branch for the refactor.
