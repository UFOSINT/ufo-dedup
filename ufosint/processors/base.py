"""
Base class for analysis/enrichment processors.

Each processor operates on the unified database, reading rows and
writing derived columns. Processors are registered in PROCESSOR_STEPS
and executed in dependency order by the pipeline runner.

To add a new processor:
    1. Create a module in ufosint/processors/
    2. Subclass Processor
    3. Implement name, label, process()
    4. Register in ufosint/processors/__init__.py
"""

import sqlite3
import time
from abc import ABC, abstractmethod

from ufosint.db import Database

BATCH_SIZE = 5000


class Processor(ABC):
    """Base class for all analysis/enrichment processors."""

    @property
    @abstractmethod
    def name(self):
        """Short identifier (e.g., 'shapes', 'quality'). Used in CLI and logs."""

    @property
    @abstractmethod
    def label(self):
        """Human-readable label for progress display."""

    @property
    def depends_on(self):
        """List of processor names that must run before this one."""
        return []

    @abstractmethod
    def process(self, conn):
        """Run this processor. Receives an open sqlite3 connection."""

    def reset(self, conn):
        """Clear this processor's output columns. Override if needed."""
        pass

    def status(self, conn):
        """Return a dict of coverage stats. Override for custom stats."""
        return {}

    def run(self, db=None):
        """Execute the processor with timing and progress."""
        db = db or Database()
        conn = db.connect()
        t0 = time.time()
        self.process(conn)
        elapsed = time.time() - t0
        conn.close()
        return {"name": self.name, "elapsed_s": round(elapsed, 1)}


def executemany_batched(conn, sql, rows, batch_size=BATCH_SIZE):
    """Execute an UPDATE/INSERT in batches with periodic commits."""
    cur = conn.cursor()
    for i in range(0, len(rows), batch_size):
        cur.executemany(sql, rows[i:i + batch_size])
    conn.commit()
