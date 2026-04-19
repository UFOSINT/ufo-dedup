"""
UFOSINT CLI — single entry point for all pipeline operations.

Usage:
    ufosint status              # database coverage dashboard
    ufosint config              # show resolved configuration
    ufosint rebuild             # full pipeline (17 steps)
    ufosint --help              # all commands

Commands are added incrementally as modules are migrated.
"""

import os

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


# ── Import command ──

@main.command("import")
@click.argument("source", required=False)
@click.option("--all", "import_all", is_flag=True, help="Import all sources")
@click.option("--list", "list_sources", is_flag=True, help="List available sources")
def import_cmd(source, import_all, list_sources):
    """Import source data into the database.

    Examples:
        ufosint import nuforc
        ufosint import --all
        ufosint import --list
    """
    from ufosint.importers import IMPORTERS, get_importer

    if list_sources:
        print("\nAvailable sources:")
        for name, cls in IMPORTERS.items():
            imp = cls()
            exists = "OK" if os.path.exists(imp.file_path) else "MISSING"
            print(f"  {exists:>7}  {name:<14} {imp.source_name:<14} {imp.file_path}")
        print()
        return

    if import_all:
        db = Database()
        for name, cls in IMPORTERS.items():
            imp = cls()
            print(f"\n--- {imp.source_name} ---")
            imp.run(db)
        return

    if not source:
        click.echo("Usage: ufosint import <source> | --all | --list")
        return

    try:
        imp = get_importer(source)
    except KeyError as e:
        click.echo(str(e))
        return

    imp.run()


# ── Future commands (added in later phases) ──
# @main.command()
# def rebuild(): ...
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
# @main.command()
# def audit(): ...
#
# @main.command()
# def enrich(): ...
#
# @main.command()
# def export(): ...


if __name__ == "__main__":
    main()
