"""
Tests for the ufosint package infrastructure.

Covers: config, db, importers, processors, llm cache, display, cli.
All tests use in-memory or temp databases — never touches production.
"""

import json
import os
import sqlite3
import tempfile

import pytest


# ============================================================
# Config
# ============================================================

class TestConfig:
    def test_project_root(self):
        from ufosint.config import Config
        root = Config.project_root()
        assert os.path.isdir(root)
        assert os.path.exists(os.path.join(root, "ufosint.toml"))

    def test_db_path_is_absolute(self):
        from ufosint.config import Config
        assert os.path.isabs(Config.db_path())

    def test_db_path_ends_with_unified(self):
        from ufosint.config import Config
        assert Config.db_path().endswith("ufo_unified.db")

    def test_public_db_path(self):
        from ufosint.config import Config
        assert Config.public_db_path().endswith("ufo_public.db")

    def test_llm_model_default(self):
        from ufosint.config import Config
        model = Config.llm_model()
        assert "gemini" in model or "gpt" in model or len(model) > 5

    def test_llm_workers_is_int(self):
        from ufosint.config import Config
        assert isinstance(Config.llm_workers(), int)
        assert Config.llm_workers() > 0

    def test_gpu_batch_size(self):
        from ufosint.config import Config
        assert isinstance(Config.gpu_batch_size(), int)
        assert Config.gpu_batch_size() > 0

    def test_summary_returns_dict(self):
        from ufosint.config import Config
        s = Config.summary()
        assert isinstance(s, dict)
        assert "db_path" in s
        assert "llm_model" in s

    def test_env_override(self, monkeypatch):
        from ufosint.config import Config
        monkeypatch.setenv("UFOSINT_DATA_DIR", "/tmp/test_data")
        assert Config.raw_data_dir() == "/tmp/test_data"


# ============================================================
# Database
# ============================================================

class TestDatabase:
    def test_create_temp_db(self, tmp_path):
        from ufosint.db import Database
        db_path = str(tmp_path / "test.db")
        db = Database(db_path)
        assert not db.exists()
        conn = db.connect()
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        conn.close()
        assert db.exists()
        assert db.size_mb() > 0

    def test_count(self, tmp_path):
        from ufosint.db import Database
        db_path = str(tmp_path / "test.db")
        db = Database(db_path)
        conn = db.connect()
        conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'a')")
        conn.execute("INSERT INTO items VALUES (2, 'b')")
        conn.execute("INSERT INTO items VALUES (3, 'c')")
        conn.commit()
        conn.close()
        assert db.count("items") == 3
        assert db.count("items", "id > 1") == 2

    def test_execute(self, tmp_path):
        from ufosint.db import Database
        db_path = str(tmp_path / "test.db")
        db = Database(db_path)
        conn = db.connect()
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.execute("INSERT INTO t VALUES (42)")
        conn.commit()
        conn.close()
        rows = db.execute("SELECT v FROM t")
        assert rows == [(42,)]

    def test_status_missing_db(self, tmp_path):
        from ufosint.db import Database
        db = Database(str(tmp_path / "nonexistent.db"))
        s = db.status()
        assert s["exists"] is False

    def test_status_empty_db(self, tmp_path):
        from ufosint.db import Database
        db_path = str(tmp_path / "empty.db")
        db = Database(db_path)
        conn = db.connect()
        conn.execute("CREATE TABLE sighting (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        s = db.status()
        assert s["exists"] is True
        assert s["total_sightings"] == 0

    def test_wal_mode(self, tmp_path):
        from ufosint.db import Database
        db = Database(str(tmp_path / "wal.db"))
        conn = db.connect()
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        conn.close()


# ============================================================
# Importers
# ============================================================

class TestImporterRegistry:
    def test_all_importers_registered(self):
        from ufosint.importers import IMPORTERS
        assert len(IMPORTERS) == 6
        assert "nuforc" in IMPORTERS
        assert "mufon" in IMPORTERS
        assert "ufocat" in IMPORTERS
        assert "updb" in IMPORTERS
        assert "geldreich" in IMPORTERS
        assert "reddit" in IMPORTERS

    def test_get_importer(self):
        from ufosint.importers import get_importer
        imp = get_importer("nuforc")
        assert imp.source_name == "NUFORC"

    def test_get_importer_alias(self):
        from ufosint.importers import get_importer
        imp = get_importer("ufo-search")
        assert imp.source_name == "UFO-search"

    def test_get_importer_unknown(self):
        from ufosint.importers import get_importer
        with pytest.raises(KeyError):
            get_importer("nonexistent")

    def test_importers_have_file_paths(self):
        from ufosint.importers import IMPORTERS
        for name, cls in IMPORTERS.items():
            imp = cls()
            assert imp.file_path is not None
            assert len(imp.file_path) > 0

    def test_importers_have_source_names(self):
        from ufosint.importers import IMPORTERS
        names = set()
        for name, cls in IMPORTERS.items():
            imp = cls()
            assert imp.source_name
            names.add(imp.source_name)
        # All source names should be unique
        assert len(names) == 6


class TestNuforcParser:
    def test_parse_date(self):
        from ufosint.importers.nuforc import parse_nuforc_date
        iso, raw = parse_nuforc_date("1995-02-02 23:00 Local")
        assert iso == "1995-02-02T23:00"
        assert "Local" in raw

    def test_parse_date_no_time(self):
        from ufosint.importers.nuforc import parse_nuforc_date
        iso, raw = parse_nuforc_date("2020-01-15")
        assert iso == "2020-01-15"

    def test_parse_date_empty(self):
        from ufosint.importers.nuforc import parse_nuforc_date
        assert parse_nuforc_date("") == (None, None)
        assert parse_nuforc_date(None) == (None, None)

    def test_parse_location(self):
        from ufosint.importers.nuforc import parse_nuforc_location
        city, state, country = parse_nuforc_location("Portland, OR, USA")
        assert city == "Portland"
        assert state == "OR"
        assert country == "USA"


class TestMufonParser:
    def test_parse_date_with_time(self):
        from ufosint.importers.mufon import parse_mufon_date
        iso, raw = parse_mufon_date("1992-08-19\n5:45AM")
        assert iso == "1992-08-19T05:45"

    def test_parse_date_pm(self):
        from ufosint.importers.mufon import parse_mufon_date
        iso, _ = parse_mufon_date("2020-03-15\n8:30PM")
        assert iso == "2020-03-15T20:30"

    def test_parse_location_escaped(self):
        from ufosint.importers.mufon import parse_mufon_location
        city, state, country = parse_mufon_location("Newscandia\\, MN\\, US")
        assert city == "Newscandia"
        assert state == "MN"
        assert country == "US"


class TestGeldreichParser:
    def test_parse_date_iso(self):
        from ufosint.importers.geldreich import parse_geldreich_date
        iso, _ = parse_geldreich_date("1947-06-24")
        assert iso == "1947-06-24"

    def test_parse_date_us_format(self):
        from ufosint.importers.geldreich import parse_geldreich_date
        iso, _ = parse_geldreich_date("6/24/1947")
        assert iso == "1947-06-24"

    def test_parse_date_year_only(self):
        from ufosint.importers.geldreich import parse_geldreich_date
        iso, _ = parse_geldreich_date("1947")
        assert iso == "1947"


# ============================================================
# Processors
# ============================================================

class TestProcessorRegistry:
    def test_all_processors_registered(self):
        from ufosint.processors import PROCESSORS
        assert len(PROCESSORS) == 9
        assert "shapes" in PROCESSORS
        assert "quality" in PROCESSORS
        assert "duration" in PROCESSORS

    def test_get_processor(self):
        from ufosint.processors import get_processor
        p = get_processor("shapes")
        assert p.name == "shapes"

    def test_quality_has_dependencies(self):
        from ufosint.processors import get_processor
        p = get_processor("quality")
        assert "shapes" in p.depends_on
        assert "movement" in p.depends_on

    def test_topic_is_stub(self):
        from ufosint.processors import get_processor
        p = get_processor("topic")
        assert p.name == "topic"


class TestShapeNormalizer:
    def test_exact_match(self):
        from ufosint.processors.shapes import _normalize_token, _match_shape
        token = _normalize_token("Triangle")
        shape, method = _match_shape(token, None)
        assert shape == "Triangle"
        assert method == "exact"

    def test_alias_match(self):
        from ufosint.processors.shapes import _normalize_token, _match_shape
        token = _normalize_token("Ovoid")
        shape, method = _match_shape(token, None)
        assert shape == "Oval"
        assert method == "substring"

    def test_ufocat_abbreviation(self):
        from ufosint.processors.shapes import _normalize_token, _match_shape
        token = _normalize_token("Rectangl")
        shape, method = _match_shape(token, None)
        assert shape == "Rectangle"
        assert method == "substring"

    def test_unmatched(self):
        from ufosint.processors.shapes import _normalize_token, _match_shape
        token = _normalize_token("xyzgarbage")
        shape, method = _match_shape(token, None)
        assert shape == "Other"
        assert method == "unmatched"

    def test_none_input(self):
        from ufosint.processors.shapes import _normalize_token, _match_shape
        token = _normalize_token(None)
        shape, method = _match_shape(token, None)
        assert shape == "Unknown"


class TestDurationParser:
    def test_minutes(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("5 minutes") == 300

    def test_seconds(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("30 seconds") == 30

    def test_hours(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("2 hours") == 7200

    def test_ufocat_code_brief(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("B") == 3

    def test_ufocat_code_medium(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("M") == 120

    def test_bare_number(self):
        from ufosint.processors.duration import parse_duration_text
        # Bare numbers are UFOCAT decimal minutes
        assert parse_duration_text("5") == 300

    def test_decimal_minutes(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text(".5") == 30

    def test_about_prefix(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("about 10 minutes") == 600

    def test_few_minutes(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("few minutes") == 180

    def test_unknown(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("unknown") is None

    def test_none(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text(None) is None

    def test_empty(self):
        from ufosint.processors.duration import parse_duration_text
        assert parse_duration_text("") is None


# ============================================================
# LLM Cache
# ============================================================

class TestResultCache:
    def test_create_and_append(self, tmp_path):
        from ufosint.llm.cache import ResultCache
        cache = ResultCache("test_cache",
                            columns=["id", "value"],
                            path=str(tmp_path / "test.csv"))
        assert not cache.exists()

        cache.append_rows([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])
        assert cache.exists()
        assert cache.row_count() == 2

    def test_load(self, tmp_path):
        from ufosint.llm.cache import ResultCache
        cache = ResultCache("test_cache",
                            columns=["id", "value"],
                            path=str(tmp_path / "test.csv"))
        cache.append_rows([{"id": 1, "value": "a"}])
        rows = cache.load()
        assert len(rows) == 1
        assert rows[0]["id"] == "1"
        assert rows[0]["value"] == "a"

    def test_load_seen_ids(self, tmp_path):
        from ufosint.llm.cache import ResultCache
        cache = ResultCache("test_cache",
                            columns=["sighting_id", "value"],
                            path=str(tmp_path / "test.csv"))
        cache.append_rows([
            {"sighting_id": 100, "value": "a"},
            {"sighting_id": 200, "value": "b"},
        ])
        seen = cache.load_seen_ids()
        assert seen == {100, 200}

    def test_summary(self, tmp_path):
        from ufosint.llm.cache import ResultCache
        cache = ResultCache("test_cache",
                            columns=["id"],
                            path=str(tmp_path / "test.csv"))
        s = cache.summary()
        assert s["exists"] is False
        assert s["rows"] == 0

        cache.append_rows([{"id": 1}])
        s = cache.summary()
        assert s["exists"] is True
        assert s["rows"] == 1

    def test_empty_cache(self, tmp_path):
        from ufosint.llm.cache import ResultCache
        cache = ResultCache("empty", path=str(tmp_path / "empty.csv"))
        assert cache.load() == []
        assert cache.load_seen_ids() == set()


class TestParseJsonResponse:
    def test_plain_json(self):
        from ufosint.llm.client import parse_json_response
        result = parse_json_response('{"key": "value"}')
        assert result == {"key": "value"}

    def test_markdown_fenced(self):
        from ufosint.llm.client import parse_json_response
        result = parse_json_response('```json\n{"key": "value"}\n```')
        assert result == {"key": "value"}

    def test_json_array(self):
        from ufosint.llm.client import parse_json_response
        result = parse_json_response('[{"a": 1}, {"b": 2}]')
        assert len(result) == 2

    def test_invalid(self):
        from ufosint.llm.client import parse_json_response
        assert parse_json_response("not json at all") is None

    def test_none(self):
        from ufosint.llm.client import parse_json_response
        assert parse_json_response(None) is None


# ============================================================
# Display
# ============================================================

class TestDisplay:
    def test_progress_bar(self):
        from ufosint.display.progress import progress_bar
        bar = progress_bar(0.5, 20)
        assert len(bar) == 20
        assert bar.count("\u2588") == 10
        assert bar.count("\u2591") == 10

    def test_progress_bar_bounds(self):
        from ufosint.display.progress import progress_bar
        assert len(progress_bar(0.0, 10)) == 10
        assert len(progress_bar(1.0, 10)) == 10
        assert len(progress_bar(-0.5, 10)) == 10  # clamped
        assert len(progress_bar(1.5, 10)) == 10   # clamped

    def test_format_duration(self):
        from ufosint.display.progress import format_duration
        assert format_duration(45) == "45s"
        assert format_duration(125) == "2.1m"
        assert "h" in format_duration(7200)

    def test_format_eta(self):
        from ufosint.display.progress import format_eta
        assert format_eta(0) == "done"
        assert format_eta(30) == "30s"
        assert "m" in format_eta(90)

    def test_format_count(self):
        from ufosint.display.progress import format_count
        assert format_count(1234567) == "1,234,567"

    def test_format_pct(self):
        from ufosint.display.progress import format_pct
        assert format_pct(50, 100) == "50.0%"
        assert format_pct(0, 0) == "0.0%"

    def test_colors_singleton(self):
        from ufosint.display.colors import C
        assert hasattr(C, "GREEN")
        assert hasattr(C, "RESET")
        assert hasattr(C, "BOLD")


# ============================================================
# Export
# ============================================================

class TestExportModule:
    def test_import_run_export(self):
        from ufosint.export.public_db import run_export
        assert callable(run_export)

    def test_public_tables_set(self):
        from ufosint.export.public_db import PUBLIC_TABLES
        assert "sighting" in PUBLIC_TABLES
        assert "location" in PUBLIC_TABLES
        assert "sighting_analysis" in PUBLIC_TABLES

    def test_fmt_bytes(self):
        from ufosint.export.public_db import fmt_bytes
        assert "KB" in fmt_bytes(2048)
        assert "MB" in fmt_bytes(2 * 1024 * 1024)
        assert "B" in fmt_bytes(500)


# ============================================================
# Inlined Processor Constants
# ============================================================

class TestProcessorConstants:
    def test_movement_behavior_keywords(self):
        from ufosint.processors.movement import BEHAVIOR_KEYWORDS
        assert "hovering" in BEHAVIOR_KEYWORDS
        assert "silent" in BEHAVIOR_KEYWORDS
        assert len(BEHAVIOR_KEYWORDS) == 14

    def test_movement_categories(self):
        from ufosint.processors.movement import MOVEMENT_CATEGORY_PATTERNS
        assert "hovering" in MOVEMENT_CATEGORY_PATTERNS
        assert "erratic" in MOVEMENT_CATEGORY_PATTERNS
        assert len(MOVEMENT_CATEGORY_PATTERNS) == 10

    def test_color_words(self):
        from ufosint.processors.colors import COLOR_WORDS
        assert "red" in COLOR_WORDS
        assert "metallic silver" in COLOR_WORDS

    def test_quality_fields(self):
        from ufosint.processors.quality import QUALITY_STRUCTURED_FIELDS
        assert "shape" in QUALITY_STRUCTURED_FIELDS
        assert "hynek" in QUALITY_STRUCTURED_FIELDS
        assert len(QUALITY_STRUCTURED_FIELDS) == 9

    def test_quality_caps(self):
        from ufosint.processors.quality import (
            UNKNOWN_DATE_CAP, UNKNOWN_DATE_CAP_RICH,
        )
        assert UNKNOWN_DATE_CAP == 15
        assert UNKNOWN_DATE_CAP_RICH == 35

    def test_hoax_weights(self):
        from ufosint.processors.hoax import HOAX_WEIGHTS
        assert "very_short_text" in HOAX_WEIGHTS
        assert "generic_phrasing" in HOAX_WEIGHTS
        assert len(HOAX_WEIGHTS) == 5

    def test_sentiment_emotion_keys(self):
        from ufosint.processors.sentiment import EMOTION_KEYS
        assert "joy" in EMOTION_KEYS
        assert "fear" in EMOTION_KEYS
        assert len(EMOTION_KEYS) == 8


# ============================================================
# Pipeline
# ============================================================

class TestPipeline:
    def test_step_list(self):
        from ufosint.pipeline import STEPS, STEP_NAMES
        assert len(STEPS) == 17
        assert STEP_NAMES[0] == "schema"
        assert STEP_NAMES[-1] == "export"
        assert "analyze" in STEP_NAMES
        assert "audit" in STEP_NAMES

    def test_pipeline_init(self, tmp_path):
        from ufosint.pipeline import Pipeline
        from ufosint.db import Database
        db = Database(str(tmp_path / "test.db"))
        p = Pipeline(db=db)
        assert p.db_path == str(tmp_path / "test.db")


# ============================================================
# CLI
# ============================================================

class TestCLI:
    def test_cli_entry_point(self):
        from ufosint.cli import main
        assert main is not None

    def test_version(self):
        from click.testing import CliRunner
        from ufosint.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.14.0" in result.output

    def test_config_command(self):
        from click.testing import CliRunner
        from ufosint.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["config"])
        assert result.exit_code == 0
        assert "Db Path" in result.output

    def test_import_list(self):
        from click.testing import CliRunner
        from ufosint.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["import", "--list"])
        assert result.exit_code == 0
        assert "nuforc" in result.output
        assert "reddit" in result.output

    def test_analyze_list(self):
        from click.testing import CliRunner
        from ufosint.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["analyze", "--list"])
        assert result.exit_code == 0
        assert "shapes" in result.output
        assert "quality" in result.output

    def test_rebuild_list(self):
        from click.testing import CliRunner
        from ufosint.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["rebuild", "--list"])
        assert result.exit_code == 0
        assert "schema" in result.output
        assert "export" in result.output
