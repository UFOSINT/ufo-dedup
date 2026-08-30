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


# Origins we import directly from that origin's own dataset. An aggregator
# (UPDB) should skip a row only when its origin appears here, because we
# already hold that origin's richer original.
#
# v0.16.3 -- MUFON was removed from this set. The mufon.csv import was
# retired in v0.16, so UPDB was skipping ~MUFON-origin rows on a rationale
# that no longer held, and MUFON coverage from UPDB was being lost entirely
# rather than merely deduplicated. NUFORC stays: it is still imported
# directly from its own dataset.
#
# Keep this in step with ufosint/pipeline.py STEPS. tests/test_origins.py
# asserts the two agree, because the failure is silent -- dropping an
# importer without updating this set quietly deletes that source's coverage
# from every aggregator too.
DIRECTLY_IMPORTED_ORIGINS = {
    "NUFORC": ("NUFORC", "National UFO Reporting Center"),
}


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
    def json_root(self):
        """Key holding the record list, when the JSON is a dict wrapper.

        None means the document is already a list. Getting this wrong used to
        fail silently: iterating a dict yields its keys, so the importer saw
        one "row", skipped it, and reported success having imported nothing.
        """
        return None

    @property
    def batch_size(self):
        return 5000

    def should_skip_row(self, raw):
        """Return True to skip this row entirely.

        Aggregators (UPDB) use this to drop rows whose origin we already
        import from that origin's own richer dataset. See
        DIRECTLY_IMPORTED_ORIGINS — the skip must be derived from what the
        pipeline actually imports, never hardcoded, or dropping a source
        from the pipeline silently loses its coverage everywhere.
        """
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
        rows_read = len(raw_rows)
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

        # A source file that exists but yields nothing is a structural problem
        # (wrong shape, wrong key, changed schema), not a legitimate outcome.
        # It used to pass silently: the v0.16.4 rebuild lost all 54,751
        # UFO-search rows to a "0 imported, 1 skipped" that looked like success.
        if imported == 0 and rows_read > 0:
            stats["error"] = "imported_nothing"
            print(f"  !! {self.source_name}: read {rows_read:,} record(s) but "
                  f"imported none — the file shape is probably wrong")

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

        # Translate the human-readable origin name an importer may have
        # attached into the source_origin FK. Aggregator rows (UPDB,
        # GELDREICH) carry the body that originally reported the case, which
        # is what makes "MUFON via UPDB" distinguishable from the retired
        # mufon.csv import -- the v0.16 purge keyed on exactly that
        # distinction (source_db_id, never origin_id).
        #
        # Done here rather than in parse_row because it needs a cursor.
        # Every row in the batch gets the key so the column list below stays
        # consistent across dicts.
        if any("origin_name" in d for d in sight_batch):
            omap = self._origin_id_map(cur)
            for d in sight_batch:
                name = d.pop("origin_name", None)
                d["origin_id"] = omap.get(name.strip().upper()) if name else None

        # Determine columns from the first dict
        columns = list(sight_batch[0].keys())
        placeholders = ", ".join("?" * len(columns))
        col_names = ", ".join(columns)

        cur.executemany(
            f"INSERT INTO sighting ({col_names}) VALUES ({placeholders})",
            [tuple(d.get(c) for c in columns) for d in sight_batch]
        )
        conn.commit()

    def _origin_id_map(self, cur):
        """Cached {UPPERCASE origin name: id} from source_origin."""
        if getattr(self, "_origin_cache", None) is None:
            cur.execute("SELECT id, name FROM source_origin")
            self._origin_cache = {n.strip().upper(): i for i, n in cur.fetchall()}
        return self._origin_cache

    def _read_source(self):
        """Read the source file and return a list of dicts."""
        if self.file_format == "json":
            with open(self.file_path, "r", encoding=self.csv_encoding) as f:
                data = json.load(f)
            if self.json_root is not None:
                if not isinstance(data, dict) or self.json_root not in data:
                    raise ValueError(
                        f"{self.source_name}: expected a dict with key "
                        f"{self.json_root!r} in {self.file_path}, got "
                        f"{type(data).__name__} with keys "
                        f"{list(data)[:5] if isinstance(data, dict) else 'n/a'}"
                    )
                data = data[self.json_root]
            if not isinstance(data, list):
                raise ValueError(
                    f"{self.source_name}: {self.file_path} did not yield a list "
                    f"of records (got {type(data).__name__}). Set json_root if "
                    f"the records are nested under a key."
                )
            return data
        else:
            with open(self.file_path, "r", encoding=self.csv_encoding, errors="replace") as f:
                reader = csv.DictReader(f)
                return list(reader)
