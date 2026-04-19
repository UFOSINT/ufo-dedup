"""
Central configuration for the UFOSINT pipeline.

Resolution order (highest priority first):
  1. Explicit function arguments
  2. Environment variables (UFOSINT_*, OPENROUTER_API_KEY)
  3. ufosint.toml in the project root
  4. Built-in defaults

Usage:
    from ufosint.config import Config

    db_path = Config.db_path()
    model = Config.llm_model()
    workers = Config.llm_workers()
"""

import os
import sys

# Find the project root (directory containing ufosint.toml or this package's parent)
_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_PACKAGE_DIR)

# Try to load ufosint.toml
_toml_data = {}
_toml_path = os.path.join(_PROJECT_ROOT, "ufosint.toml")
if os.path.exists(_toml_path):
    try:
        if sys.version_info >= (3, 11):
            import tomllib
        else:
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib
        with open(_toml_path, "rb") as f:
            _toml_data = tomllib.load(f)
    except Exception:
        pass  # fall back to defaults


def _get(section, key, default=None, env_var=None):
    """Resolve a config value: env var > toml > default."""
    if env_var and os.environ.get(env_var):
        return os.environ[env_var]
    toml_section = _toml_data.get(section, {})
    return toml_section.get(key, default)


class Config:
    """Central configuration access. All paths are absolute."""

    # ── Paths ──

    @staticmethod
    def project_root():
        return _PROJECT_ROOT

    @staticmethod
    def db_path():
        rel = _get("paths", "db", "data/output/ufo_unified.db")
        if os.path.isabs(rel):
            return rel
        return os.path.join(_PROJECT_ROOT, rel)

    @staticmethod
    def public_db_path():
        rel = _get("paths", "public_db", "data/output/ufo_public.db")
        if os.path.isabs(rel):
            return rel
        return os.path.join(_PROJECT_ROOT, rel)

    @staticmethod
    def raw_data_dir():
        return _get("paths", "raw_data",
                     os.path.join(_PROJECT_ROOT, "..", "data", "raw"),
                     env_var="UFOSINT_DATA_DIR")

    @staticmethod
    def cache_dir():
        rel = _get("paths", "cache", "data/output")
        if os.path.isabs(rel):
            return rel
        return os.path.join(_PROJECT_ROOT, rel)

    @staticmethod
    def geodata_dir():
        rel = _get("paths", "geodata", "geodata")
        if os.path.isabs(rel):
            return rel
        return os.path.join(_PROJECT_ROOT, rel)

    @staticmethod
    def cache_path(filename):
        """Return absolute path for a cache file."""
        return os.path.join(Config.cache_dir(), filename)

    # ── LLM ──

    @staticmethod
    def llm_model():
        return _get("llm", "model", "google/gemini-2.0-flash-001",
                     env_var="AUDIT_MODEL")

    @staticmethod
    def llm_workers():
        val = _get("llm", "workers", 15)
        return int(val)

    @staticmethod
    def llm_batch_size():
        val = _get("llm", "batch_size", 25)
        return int(val)

    @staticmethod
    def openrouter_api_key():
        return os.environ.get("OPENROUTER_API_KEY", "")

    # ── GPU ──

    @staticmethod
    def gpu_batch_size():
        val = _get("gpu", "batch_size", 64)
        return int(val)

    @staticmethod
    def gpu_device():
        return _get("gpu", "device", "auto")

    # ── Pipeline ──

    @staticmethod
    def unknown_date_cap():
        val = _get("pipeline", "unknown_date_cap", 15)
        return int(val)

    @staticmethod
    def unknown_date_cap_rich():
        val = _get("pipeline", "unknown_date_cap_rich", 35)
        return int(val)

    @staticmethod
    def fuzzy_shape_cutoff():
        val = _get("pipeline", "fuzzy_shape_cutoff", 85)
        return int(val)

    # ── Convenience ──

    @staticmethod
    def summary():
        """Return a dict of all config values for display."""
        return {
            "project_root": Config.project_root(),
            "db_path": Config.db_path(),
            "public_db_path": Config.public_db_path(),
            "raw_data_dir": Config.raw_data_dir(),
            "cache_dir": Config.cache_dir(),
            "geodata_dir": Config.geodata_dir(),
            "llm_model": Config.llm_model(),
            "llm_workers": Config.llm_workers(),
            "llm_batch_size": Config.llm_batch_size(),
            "has_api_key": bool(Config.openrouter_api_key()),
            "gpu_batch_size": Config.gpu_batch_size(),
            "gpu_device": Config.gpu_device(),
        }
