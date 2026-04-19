"""
Dual-output logger: writes to both a log file and stdout.

Usage:
    from ufosint.display.log import Logger

    logger = Logger("enrich_extract.log")
    logger.info("Processing started")           # writes to file + returns string
    logger.info("FIX: city=Toronto", echo=True)  # also prints to stdout
    logger.close()
"""

import os
from datetime import datetime

from ufosint.config import Config


class Logger:
    """File logger with optional stdout echo."""

    def __init__(self, filename, log_dir=None):
        """Open a log file for appending.

        Args:
            filename: log file name (e.g., "enrich_extract.log")
            log_dir: directory for log file. Default: project root.
        """
        if log_dir is None:
            log_dir = Config.project_root()
        self.path = os.path.join(log_dir, filename)
        self._file = open(self.path, "a", encoding="utf-8")
        self.separator()

    def separator(self):
        self._file.write("=" * 60 + "\n")
        self._file.flush()

    def info(self, msg, echo=False):
        """Write a timestamped log line."""
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self._file.write(line + "\n")
        self._file.flush()
        if echo:
            print(line)
        return line

    def error(self, msg):
        """Write an error line (always echoed)."""
        return self.info(f"ERROR: {msg}", echo=True)

    def complete(self, summary_dict):
        """Write a JSON completion summary."""
        import json
        self.info(f"COMPLETE: {json.dumps(summary_dict)}")

    def close(self):
        if self._file and not self._file.closed:
            self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
