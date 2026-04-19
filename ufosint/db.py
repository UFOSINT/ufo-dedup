"""
Database manager for the UFOSINT unified sighting database.

Provides connection management, schema operations, and status queries.
Single source of truth — no more 21 files with their own DB_PATH.

Usage:
    from ufosint.db import Database

    db = Database()                         # uses Config.db_path()
    db = Database("path/to/custom.db")      # explicit path

    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sighting")

    db.status()       # coverage stats
    db.ensure_schema()  # create tables if missing
"""

import os
import sqlite3

from ufosint.config import Config


class Database:
    """Manage connections and operations on the unified sighting database."""

    def __init__(self, path=None):
        self.path = path or Config.db_path()

    def exists(self):
        return os.path.exists(self.path)

    def size_mb(self):
        if not self.exists():
            return 0
        return os.path.getsize(self.path) / (1024 * 1024)

    def connect(self):
        """Open a WAL-mode connection with sensible defaults.

        Returns a sqlite3.Connection. Use as context manager or close manually.
        """
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def execute(self, sql, params=None):
        """One-shot execute: open, run, commit, close."""
        conn = self.connect()
        cur = conn.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        conn.commit()
        result = cur.fetchall()
        conn.close()
        return result

    def count(self, table, where=None):
        """Quick row count."""
        sql = f"SELECT COUNT(*) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        rows = self.execute(sql)
        return rows[0][0]

    def status(self):
        """Return a comprehensive coverage report as a dict."""
        if not self.exists():
            return {"exists": False, "path": self.path}

        conn = self.connect()
        cur = conn.cursor()

        def _count(sql):
            cur.execute(sql)
            return cur.fetchone()[0]

        report = {
            "exists": True,
            "path": self.path,
            "size_mb": round(self.size_mb(), 1),
        }

        # Core counts
        try:
            report["total_sightings"] = _count("SELECT COUNT(*) FROM sighting")
        except sqlite3.OperationalError:
            conn.close()
            report["error"] = "sighting table not found"
            return report

        # Per source
        cur.execute("""
            SELECT sd.name, COUNT(*) FROM sighting s
            JOIN source_database sd ON s.source_db_id = sd.id
            GROUP BY sd.name ORDER BY COUNT(*) DESC
        """)
        report["sources"] = {name: count for name, count in cur.fetchall()}

        # Coverage metrics
        coverage = {}
        checks = [
            ("geocoded", "lat IS NOT NULL AND lng IS NOT NULL"),
            ("has_description", "has_description = 1"),
            ("quality_gte_60", "quality_score >= 60"),
            ("has_std_shape", "standardized_shape IS NOT NULL"),
            ("has_color", "color IS NOT NULL"),
            ("has_duration", "duration_seconds IS NOT NULL"),
            ("has_sound", "sound IS NOT NULL"),
            ("has_direction", "direction IS NOT NULL"),
            ("has_movement", "has_movement_mentioned = 1"),
            ("has_sentiment", "sentiment_score IS NOT NULL"),
            ("has_emotion_28", "emotion_28_dominant IS NOT NULL"),
            ("has_emotion_7", "emotion_7_dominant IS NOT NULL"),
            ("has_vader", "vader_compound IS NOT NULL"),
            ("has_nrc", "nrc_fear IS NOT NULL"),
            ("has_nuclear_proximity", "distance_to_nearest_nuclear_site_km IS NOT NULL"),
            ("has_reddit", "reddit_post_id IS NOT NULL"),
            ("audit_extracted", "audit_status = 'extracted'"),
            ("audit_geocode_mismatch", "audit_geocode_check = 'mismatch'"),
            ("audit_location_normalized", "audit_location_check = 'normalized'"),
        ]
        for name, where in checks:
            try:
                coverage[name] = _count(f"SELECT COUNT(*) FROM sighting WHERE {where}")
            except sqlite3.OperationalError:
                coverage[name] = None  # column doesn't exist yet

        report["coverage"] = coverage

        # Quality score stats
        try:
            cur.execute("SELECT AVG(quality_score), MIN(quality_score), MAX(quality_score) FROM sighting")
            avg, mn, mx = cur.fetchone()
            report["quality"] = {"avg": round(avg, 1) if avg else None, "min": mn, "max": mx}
        except sqlite3.OperationalError:
            pass

        # Cache files
        cache_dir = Config.cache_dir()
        caches = {}
        for name, filename in [
            ("audit_tier_b", "audit_tier_b_fixes.csv"),
            ("llm_extractions", "llm_field_extractions.csv"),
            ("emotion_cache", "emotion_classification_cache.csv"),
        ]:
            path = os.path.join(cache_dir, filename)
            if os.path.exists(path):
                size = os.path.getsize(path) / (1024 * 1024)
                caches[name] = {"path": path, "size_mb": round(size, 1)}
        report["caches"] = caches

        conn.close()
        return report

    def print_status(self):
        """Print a formatted status report to stdout."""
        s = self.status()

        if not s.get("exists"):
            print(f"\n  Database not found at {s['path']}")
            return

        total = s["total_sightings"]
        cov = s.get("coverage", {})

        print()
        print("=" * 62)
        print(f"  UFOSINT Database Status")
        print("=" * 62)
        print(f"  Path: {s['path']}")
        print(f"  Size: {s['size_mb']} MB")
        print(f"  Total sightings: {total:,}")
        print()

        # Sources
        print("  Sources:")
        for name, count in s.get("sources", {}).items():
            pct = 100 * count / total if total else 0
            print(f"    {name:<14} {count:>10,}  ({pct:.1f}%)")

        # Coverage
        print()
        print("  Coverage:")
        labels = {
            "geocoded": "Map coordinates",
            "has_description": "Has description",
            "quality_gte_60": "Quality >= 60",
            "has_std_shape": "Standardized shape",
            "has_color": "Color",
            "has_duration": "Duration (seconds)",
            "has_sound": "Sound",
            "has_direction": "Direction",
            "has_movement": "Movement mentioned",
            "has_sentiment": "Sentiment score",
            "has_emotion_28": "GoEmotions 28-class",
            "has_emotion_7": "7-class RoBERTa",
            "has_vader": "VADER compound",
            "has_nrc": "NRC word counts",
            "has_nuclear_proximity": "Nuclear proximity",
            "has_reddit": "Reddit posts",
        }
        for key, label in labels.items():
            val = cov.get(key)
            if val is not None:
                pct = 100 * val / total if total else 0
                print(f"    {label:<25} {val:>10,}  ({pct:.1f}%)")

        # Quality
        q = s.get("quality")
        if q and q.get("avg"):
            print(f"\n  Quality: avg={q['avg']}, min={q['min']}, max={q['max']}")

        # Caches
        caches = s.get("caches", {})
        if caches:
            print("\n  Cached results (replay on rebuild):")
            for name, info in caches.items():
                print(f"    {name:<25} {info['size_mb']:>6.1f} MB")

        # Audit
        audit_keys = [k for k in cov if k.startswith("audit_")]
        if any(cov.get(k) for k in audit_keys):
            print("\n  Audit:")
            for key in audit_keys:
                val = cov.get(key)
                if val:
                    label = key.replace("audit_", "").replace("_", " ").title()
                    print(f"    {label:<25} {val:>10,}")

        print("=" * 62)
        print()
