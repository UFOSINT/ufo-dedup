"""
CSV-backed cache for expensive LLM/GPU operations.

Caches are written during live runs and replayed on future rebuilds
without re-calling APIs or re-running GPU inference.

Usage:
    from ufosint.llm.cache import ResultCache

    cache = ResultCache("audit_tier_b_fixes", [
        "sighting_id", "raw_location", "new_city", "new_state", "new_country",
    ])

    # During live run:
    cache.append([{"sighting_id": 123, "new_city": "Toronto", ...}])

    # During replay:
    for row in cache.load():
        # apply row to DB
"""

import csv
import os

from ufosint.config import Config


class ResultCache:
    """CSV-backed cache with append and replay."""

    def __init__(self, name, columns=None, path=None):
        """
        Args:
            name: cache name (e.g., "audit_tier_b_fixes")
            columns: list of column names for the CSV header
            path: explicit file path (default: data/output/{name}.csv)
        """
        self.name = name
        self.columns = columns
        self.path = path or Config.cache_path(f"{name}.csv")

    def exists(self):
        return os.path.exists(self.path) and os.path.getsize(self.path) > 0

    def size_mb(self):
        if not self.exists():
            return 0
        return os.path.getsize(self.path) / (1024 * 1024)

    def row_count(self):
        """Count rows (excluding header)."""
        if not self.exists():
            return 0
        with open(self.path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1

    def load_seen_ids(self, id_column="sighting_id"):
        """Load all IDs already in the cache (for resume-safe processing)."""
        seen = set()
        if not self.exists():
            return seen
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        seen.add(int(row[id_column]))
                    except (ValueError, KeyError):
                        pass
        except Exception:
            pass
        return seen

    def load(self):
        """Load all rows as list of dicts."""
        if not self.exists():
            return []
        with open(self.path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def open_writer(self):
        """Open for appending. Returns (file_handle, csv_writer).

        Writes header if file is new/empty.
        """
        is_new = not self.exists()
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        fh = open(self.path, "a", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=self.columns or [],
                                extrasaction="ignore")
        if is_new and self.columns:
            writer.writeheader()
        return fh, writer

    def append_rows(self, rows):
        """Append rows (list of dicts) and flush."""
        fh, writer = self.open_writer()
        for row in rows:
            writer.writerow(row)
        fh.flush()
        fh.close()

    def summary(self):
        """Return a summary dict for display."""
        return {
            "name": self.name,
            "path": self.path,
            "exists": self.exists(),
            "size_mb": round(self.size_mb(), 1),
            "rows": self.row_count() if self.exists() else 0,
        }
