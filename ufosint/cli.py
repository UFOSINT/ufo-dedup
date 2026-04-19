"""
UFOSINT CLI — single entry point for all pipeline operations.

Usage:
    ufosint status              # database coverage dashboard
    ufosint config              # show resolved configuration
    ufosint rebuild             # full pipeline (17 steps)
    ufosint --help              # all commands

Commands are added incrementally as modules are migrated.
"""

import click

from ufosint import __version__
from ufosint.config import Config
from ufosint.db import Database


@click.group()
@click.version_option(__version__, prog_name="ufosint")
def main():
    """UFOSINT — Unified UFO/UAP Sighting Analysis Pipeline"""
    pass


@main.command()
def status():
    """Show database coverage dashboard."""
    db = Database()
    db.print_status()


@main.command()
def config():
    """Show resolved configuration."""
    print()
    print("=" * 62)
    print("  UFOSINT Configuration")
    print("=" * 62)
    for key, val in Config.summary().items():
        label = key.replace("_", " ").title()
        print(f"  {label:<25} {val}")
    print("=" * 62)
    print()


# ── Future commands (added in later phases) ──
# @main.command()
# def rebuild(): ...
#
# @main.group()
# def import_(): ...
#
# @main.command()
# def geocode(): ...
#
# @main.command()
# def analyze(): ...
#
# @main.command()
# def emotions(): ...
#
# @main.group()
# def audit(): ...
#
# @main.command()
# def enrich(): ...
#
# @main.group()
# def export(): ...
#
# @main.command()
# def spot_check(): ...


if __name__ == "__main__":
    main()
