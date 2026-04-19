"""
Base class for source importers.

Each importer reads a source file (CSV/JSON), parses rows into
standardized (location_dict, sighting_dict) tuples, and batch-inserts
them into the unified database.

To add a new source:
    1. Create a new module in ufosint/importers/
    2. Subclass Importer
    3. Implement source_name, file_path, parse_row()
    4. Register in ufosint/importers/__init__.py
"""

import csv
import json
import os
import sqlite3
import sys
import time
from abc import ABC, abstractmethod

from ufosint.config import Config
from ufosint.db import Database


class Importer(ABC):
    """Base class for all source importers."""

    # ── Subclass must define ──

    @property
    @abstractmethod
    def source_name(self):
        """Source database name as seeded in source_database table (e.g., 'NUFORC')."""

    @property
    @abstractmethod
    def file_path(self):
        """Absolute path to the source file."""

    @abstractmethod
    def parse_row(self, raw):
        """Parse one raw row into (location_dict, sighting_dict).

        Args:
            raw: dict from CSV DictReader or JSON record

        Returns:
            (location_dict, sighting_dict) where each is a dict of column->value.
            Return (None, None) to skip this row.

        location_dict keys: raw_text, city, county, state, country, region,
                            latitude, longitude
        sighting_dict keys: any column on the sighting table (except id,
                            source_db_id, location_id — those are handled
                            by the base class)
        """

    # ── Optional overrides ──

    @property
    def file_format(self):
        """'csv' or 'json'. Default: inferred from file_path extension."""
        ext = os.path.splitext(self.file_path)[1].lower()
        return "json" if ext == ".json" else "csv"

    @property
    def csv_encoding(self):
        return "utf-8"

    @property
    def batch_size(self):
        return 5000

    def should_skip_row(self, raw):
        """Return True to skip this row entirely (e.g., UPDB MUFON/NUFORC filter)."""
        return False

    def on_skip(self, raw, reason=None):
        """Called when a row is skipped. Override for side effects (e.g., enrichment sidecar)."""
        pass

    def on_complete(self, stats):
        """Called after import completes. Override for post-import actions."""
        pass

    # ── Shared import logic ──

    def run(self, db=None):
        """Execute the full import."""
        db = db or Database()

        if not os.path.exists(self.file_path):
            print(f"  ERROR: Source file not found: {self.file_path}")
            return {"imported": 0, "skipped": 0, "error": "file_not_found"}

        conn = db.connect()
        cur = conn.cursor()

        # Look up source_db_id
        cur.execute("SELECT id FROM source_database WHERE name = ?", (self.source_name,))
        row = cur.fetchone()
        if not row:
            print(f"  ERROR: Source '{self.source_name}' not found in source_database.")
            print(f"  Run create_schema.py first.")
            conn.close()
            return {"imported": 0, "skipped": 0, "error": "source_not_found"}
        source_db_id = row[0]

        # Check for existing imports
        cur.execute("SELECT COUNT(*) FROM sighting WHERE source_db_id = ?", (source_db_id,))
        existing = cur.fetchone()[0]
        if existing > 0:
            print(f"  {self.source_name}: {existing:,} rows already exist — skipping.")
            conn.close()
            return {"imported": 0, "skipped": 0, "existing": existing}

        # Read source file
        print(f"  Reading {self.file_path}...")
        raw_rows = self._read_source()
        print(f"  Records in file: {len(raw_rows):,}")

        # Process rows
        imported = 0
        skipped = 0
        loc_batch = []
        sight_batch = []
        t0 = time.time()

        for raw in raw_rows:
            if self.should_skip_row(raw):
                self.on_skip(raw)
                skipped += 1
                continue

            try:
                loc_dict, sight_dict = self.parse_row(raw)
            except Exception:
                skipped += 1
                continue

            if loc_dict is None or sight_dict is None:
                skipped += 1
                continue

            # Build location tuple
            loc_tuple = (
                None,  # id (autoincrement)
                loc_dict.get("raw_text"),
                loc_dict.get("city"),
                loc_dict.get("county"),
                loc_dict.get("state"),
                loc_dict.get("country"),
                loc_dict.get("region"),
                loc_dict.get("latitude"),
                loc_dict.get("longitude"),
                None,  # geoname_id
            )
            loc_batch.append(loc_tuple)

            # Build sighting dict with source_db_id
            sight_dict["source_db_id"] = source_db_id
            sight_batch.append(sight_dict)

            if len(sight_batch) >= self.batch_size:
                self._flush_batch(conn, cur, loc_batch, sight_batch)
                imported += len(sight_batch)
                loc_batch = []
                sight_batch = []
                elapsed = time.time() - t0
                rate = imported / elapsed if elapsed > 0 else 0
                sys.stdout.write(f"\r  {imported:,} imported ({rate:.0f}/s)...")
                sys.stdout.flush()

        # Final batch
        if sight_batch:
            self._flush_batch(conn, cur, loc_batch, sight_batch)
            imported += len(sight_batch)

        conn.close()
        elapsed = time.time() - t0

        stats = {
            "source": self.source_name,
            "imported": imported,
            "skipped": skipped,
            "elapsed_s": round(elapsed, 1),
        }

        print(f"\n  {self.source_name}: {imported:,} imported, {skipped:,} skipped ({elapsed:.1f}s)")
        self.on_complete(stats)
        return stats

    def _flush_batch(self, conn, cur, loc_batch, sight_batch):
        """Insert a batch of locations + sightings."""
        # Insert locations
        cur.executemany(
            "INSERT INTO location (id, raw_text, city, county, state, country, "
            "region, latitude, longitude, geoname_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            loc_batch
        )

        # Get the location IDs (last N autoincrement IDs)
        last_id = cur.lastrowid
        loc_ids = range(last_id - len(loc_batch) + 1, last_id + 1)

        # Build sighting tuples
        for sight_dict, loc_id in zip(sight_batch, loc_ids):
            sight_dict["location_id"] = loc_id

        # Determine columns from the first dict
        columns = list(sight_batch[0].keys())
        placeholders = ", ".join("?" * len(columns))
        col_names = ", ".join(columns)

        cur.executemany(
            f"INSERT INTO sighting ({col_names}) VALUES ({placeholders})",
            [tuple(d.get(c) for c in columns) for d in sight_batch]
        )
        conn.commit()

    def _read_source(self):
        """Read the source file and return a list of dicts."""
        if self.file_format == "json":
            with open(self.file_path, "r", encoding=self.csv_encoding) as f:
                return json.load(f)
        else:
            with open(self.file_path, "r", encoding=self.csv_encoding, errors="replace") as f:
                reader = csv.DictReader(f)
                return list(reader)
