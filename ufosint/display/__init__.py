"""
Display components for the UFOSINT CLI.

    from ufosint.display import Dashboard, Logger, C
    from ufosint.display.progress import progress_bar, format_eta
"""

from ufosint.display.colors import C
from ufosint.display.dashboard import Dashboard
from ufosint.display.log import Logger
from ufosint.display.progress import (
    progress_bar,
    format_duration,
    format_eta,
    format_count,
    format_pct,
)

__all__ = [
    "C", "Dashboard", "Logger",
    "progress_bar", "format_duration", "format_eta",
    "format_count", "format_pct",
]