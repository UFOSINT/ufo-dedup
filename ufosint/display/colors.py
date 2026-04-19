"""
ANSI color codes for terminal output.

Usage:
    from ufosint.display.colors import C
    print(f"{C.GREEN}Success{C.RESET}")
"""

import os
import sys


def _supports_color():
    """Check if the terminal supports ANSI colors."""
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    if sys.platform == "win32":
        # Windows Terminal and modern PowerShell support ANSI
        return os.environ.get("WT_SESSION") or os.environ.get("TERM_PROGRAM")
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


class _Colors:
    """ANSI color codes. Falls back to empty strings if terminal doesn't support them."""

    def __init__(self):
        use = _supports_color()
        self.RESET   = "\033[0m"   if use else ""
        self.BOLD    = "\033[1m"   if use else ""
        self.DIM     = "\033[2m"   if use else ""
        self.GREEN   = "\033[32m"  if use else ""
        self.YELLOW  = "\033[33m"  if use else ""
        self.CYAN    = "\033[36m"  if use else ""
        self.RED     = "\033[31m"  if use else ""
        self.MAGENTA = "\033[35m"  if use else ""
        self.WHITE   = "\033[97m"  if use else ""
        self.BLUE    = "\033[34m"  if use else ""


# Singleton — import this
C = _Colors()
