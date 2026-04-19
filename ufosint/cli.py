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
import time

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


# ── Analyze command ──

@main.command()
@click.argument("step", required=False)
@click.option("--list", "list_steps", is_flag=True, help="List available processors")
def analyze(step, list_steps):
    """Run derived analysis pipeline.

    Examples:
        ufosint analyze              # run all 9 steps
        ufosint analyze shapes       # run just shape normalization
        ufosint analyze --list       # list available processors
    """
    from ufosint.processors import PROCESSORS, get_processor

    if list_steps:
        print("\nAvailable processors (in execution order):")
        for name, cls in PROCESSORS.items():
            p = cls()
            deps = ", ".join(p.depends_on) if p.depends_on else "none"
            print(f"  {name:<20} {p.label:<35} deps: {deps}")
        print()
        return

    db = Database()

    if step:
        try:
            proc = get_processor(step)
        except KeyError as e:
            click.echo(str(e))
            return
        print(f"\n[{proc.name}] {proc.label}...")
        proc.run(db)
        return

    # Run all in order
    conn = db.connect()

    # Ensure sighting_analysis rows exist
    try:
        conn.execute("""
            INSERT INTO sighting_analysis (sighting_id)
            SELECT id FROM sighting
            WHERE id NOT IN (SELECT sighting_id FROM sighting_analysis)
        """)
        conn.commit()
    except Exception:
        pass

    import time
    t0 = time.time()
    for i, (name, cls) in enumerate(PROCESSORS.items()):
        proc = cls()
        print(f"\n[{i+1}/{len(PROCESSORS)}] {proc.label}...")
        proc.process(conn)

    elapsed = time.time() - t0
    print(f"\n  Analysis complete in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    conn.close()


# ── Audit command ──

@main.command()
@click.argument("tier", required=False, type=click.Choice(["a", "b", "c"]))
@click.option("--fix-geocodes", is_flag=True, help="Fix bad geocodes (Tier A)")
@click.option("--replay", is_flag=True, help="Replay cached Tier B fixes")
@click.option("--pipeline", is_flag=True, help="Full deterministic audit (Tier A + replay)")
@click.option("--stats", is_flag=True, help="Show audit status")
@click.option("--limit", type=int, default=120000)
@click.option("--workers", type=int, default=None)
@click.option("--preview", is_flag=True)
def audit(tier, fix_geocodes, replay, pipeline, stats, limit, workers, preview):
    """LLM-powered data quality audit.

    Examples:
        ufosint audit --stats
        ufosint audit a                    # detect bad geocodes
        ufosint audit --fix-geocodes       # fix them
        ufosint audit b --limit 500        # LLM location normalization
        ufosint audit --replay             # replay cached fixes
        ufosint audit --pipeline           # full deterministic (Tier A + replay)
    """
    # Delegate to existing audit.py during transition
    import sys as _sys
    _sys.path.insert(0, Config.project_root())
    import audit as audit_mod

    if stats:
        audit_mod.print_stats(Config.db_path())
    elif fix_geocodes:
        audit_mod.tier_a_geocode_verify(Config.db_path(), fix=True)
    elif replay:
        audit_mod.replay_tier_b(Config.db_path())
    elif pipeline:
        audit_mod.run_audit_pipeline(Config.db_path())
    elif tier == "a":
        audit_mod.tier_a_geocode_verify(Config.db_path(), fix=False)
    elif tier == "b":
        w = workers or Config.llm_workers()
        audit_mod.tier_b_location_normalize(
            Config.db_path(), limit=limit, preview=preview, workers=w)
    elif tier == "c":
        audit_mod.tier_c_data_mine(
            Config.db_path(), limit=limit, preview=preview)
    else:
        click.echo("Usage: ufosint audit <a|b|c> | --fix-geocodes | --replay | --pipeline | --stats")


# ── Enrich command ──

@main.command()
@click.option("--limit", type=int, default=5000, help="Max records to process")
@click.option("--workers", type=int, default=None)
@click.option("--apply", is_flag=True, help="Apply cached extractions to DB")
@click.option("--stats", is_flag=True, help="Show extraction status")
def enrich(limit, workers, apply, stats):
    """LLM field extraction from descriptions.

    Examples:
        ufosint enrich --limit 5000        # extract from 5K records
        ufosint enrich --apply             # apply cached results to DB
        ufosint enrich --stats             # show status
    """
    import sys as _sys
    _sys.path.insert(0, Config.project_root())
    import run_enrich

    if stats:
        run_enrich.print_stats()
    elif apply:
        run_enrich.apply_extractions()
    else:
        w = workers or Config.llm_workers()
        if not Config.openrouter_api_key():
            click.echo("ERROR: OPENROUTER_API_KEY not set")
            return
        run_enrich.run_extraction(limit=limit, workers=w)


# ── Spot-check command ──

@main.command("spot-check")
@click.option("--count", type=int, default=500, help="Sample size")
@click.option("--workers", type=int, default=10)
@click.option("--preview", is_flag=True, help="Show sample without calling LLM")
def spot_check(count, workers, preview):
    """LLM quality grading of a random sample.

    Examples:
        ufosint spot-check --count 100 --preview
        ufosint spot-check --count 500
    """
    import sys as _sys
    _sys.path.insert(0, Config.project_root())
    import spot_check as sc

    sc.run_spot_check(count=count, workers=workers, preview=preview)


# ── Export command ──

@main.command()
@click.argument("target", type=click.Choice(["public"]), default="public")
def export(target):
    """Export database for distribution.

    Examples:
        ufosint export public      # clean public SQLite
    """
    if target == "public":
        import sys as _sys
        _sys.path.insert(0, Config.project_root())
        import export_public
        export_public.main()


# ── Emotions command ──

@main.command()
@click.option("--stats", is_flag=True, help="Show emotion coverage stats")
@click.option("--export-cache", is_flag=True, help="Export to CSV cache")
@click.option("--replay", is_flag=True, help="Replay cached emotions")
def emotions(stats, export_cache, replay):
    """GPU-accelerated transformer emotion classification.

    Examples:
        ufosint emotions              # run classification (requires CUDA GPU)
        ufosint emotions --stats      # show coverage
        ufosint emotions --replay     # replay from cache (no GPU needed)
    """
    import sys as _sys
    _sys.path.insert(0, Config.project_root())
    import emotions as emo_mod

    if stats:
        emo_mod.print_stats(Config.db_path())
    elif export_cache:
        emo_mod.export_emotion_cache(Config.db_path())
    elif replay:
        emo_mod.replay_emotion_cache(Config.db_path())
    else:
        emo_mod.run_emotions(Config.db_path())


# ── Rebuild command ──

@main.command()
@click.option("--from", "from_step", type=str, help="Resume from this step")
@click.option("--skip", type=str, multiple=True, help="Steps to skip (repeatable)")
@click.option("--only", type=str, help="Run only this single step")
@click.option("--list", "list_steps", is_flag=True, help="List all pipeline steps")
@click.option("--test", is_flag=True, help="Use throwaway DB at data/test/ (protects production)")
def rebuild(from_step, skip, only, list_steps, test):
    """Run the full rebuild pipeline (17 steps).

    Examples:
        ufosint rebuild                        # full pipeline
        ufosint rebuild --from geocode1        # resume from geocoding
        ufosint rebuild --skip dedup --skip emotions
        ufosint rebuild --only analyze         # single step
        ufosint rebuild --list                 # show steps
        ufosint rebuild --test                 # throwaway DB (safe to experiment)
    """
    from ufosint.pipeline import Pipeline, STEPS

    if list_steps:
        print("\nPipeline steps:")
        for i, (name, label) in enumerate(STEPS):
            print(f"  {i+1:>2}. {name:<18} {label}")
        print()
        return

    if test:
        test_db = os.path.join(Config.project_root(), "data", "test", "test_unified.db")
        os.makedirs(os.path.dirname(test_db), exist_ok=True)
        if os.path.exists(test_db):
            os.remove(test_db)
            print(f"  Removed old test DB")
        print(f"  TEST MODE: writing to {test_db}")
        print(f"  Production DB is UNTOUCHED\n")
        db = Database(test_db)
    else:
        db = None  # use default

    p = Pipeline(db=db)
    p.run(from_step=from_step, skip=set(skip), only=only)


if __name__ == "__main__":
    main()
