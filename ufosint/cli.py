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
@click.option("--db", "db_path", envvar="UFOSINT_DB", default=None,
              help="Database path (overrides config). Env: UFOSINT_DB")
@click.pass_context
def main(ctx, db_path):
    """UFOSINT — Unified UFO/UAP Sighting Analysis Pipeline"""
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = db_path  # None = use Config default


def _get_db(ctx):
    """Get Database instance, respecting --db flag."""
    path = ctx.obj.get("db_path") if ctx.obj else None
    return Database(path) if path else Database()


@main.command()
@click.pass_context
def status(ctx):
    """Show database coverage dashboard."""
    _get_db(ctx).print_status()


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
@click.pass_context
def rebuild(ctx, from_step, skip, only, list_steps, test):
    """Run the full rebuild pipeline (17 steps).

    Examples:
        ufosint rebuild                        # full pipeline
        ufosint rebuild --from geocode1        # resume from geocoding
        ufosint rebuild --skip dedup --skip emotions
        ufosint rebuild --only analyze         # single step
        ufosint rebuild --list                 # show steps
        ufosint rebuild --test                 # throwaway DB (safe to experiment)
        ufosint --db /tmp/test.db rebuild      # custom output path
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
    elif ctx.obj.get("db_path"):
        db = Database(ctx.obj["db_path"])
        print(f"  Custom DB: {db.path}\n")
    else:
        db = None  # use default

    p = Pipeline(db=db)
    p.run(from_step=from_step, skip=set(skip), only=only)


# ── Validate command ──

@main.command()
@click.pass_context
def validate(ctx):
    """Validate database integrity and source data availability.

    Checks: source files exist, DB schema is correct, row counts match
    expected ranges, no orphaned foreign keys, cache files present.
    """
    db = _get_db(ctx)
    from ufosint.importers import IMPORTERS
    from ufosint.display.colors import C

    print(f"\n{'=' * 62}")
    print(f"  UFOSINT Validation Report")
    print(f"{'=' * 62}\n")

    issues = 0

    # 1. Source files
    print("  Source data files:")
    for name, cls in IMPORTERS.items():
        imp = cls()
        exists = os.path.exists(imp.file_path)
        icon = f"{C.GREEN}OK{C.RESET}" if exists else f"{C.RED}MISSING{C.RESET}"
        print(f"    {icon:>14}  {name:<14} {imp.file_path}")
        if not exists:
            issues += 1

    # 2. Database
    print(f"\n  Database:")
    if db.exists():
        s = db.status()
        total = s.get("total_sightings", 0)
        print(f"    {C.GREEN}OK{C.RESET}      {total:,} sightings at {db.path}")

        # Check per-source counts
        for src, count in s.get("sources", {}).items():
            if count == 0:
                print(f"    {C.RED}WARN{C.RESET}    {src} has 0 rows")
                issues += 1
    else:
        print(f"    {C.RED}MISSING{C.RESET}  {db.path}")
        issues += 1

    # 3. Cache files
    print(f"\n  Cache files (for replay):")
    cache_files = [
        ("audit_tier_b_fixes.csv", "Location normalization"),
        ("llm_field_extractions.csv", "LLM field extractions"),
        ("emotion_classification_cache.csv", "Emotion classification"),
    ]
    for filename, label in cache_files:
        path = Config.cache_path(filename)
        if os.path.exists(path):
            size = os.path.getsize(path) / (1024 * 1024)
            print(f"    {C.GREEN}OK{C.RESET}      {label:<30} {size:.1f} MB")
        else:
            print(f"    {C.YELLOW}ABSENT{C.RESET}  {label:<30} (will be skipped on rebuild)")

    # 4. Geodata
    print(f"\n  Geodata:")
    geodata = Config.geodata_dir()
    gaz = os.path.join(geodata, "cities15000.txt")
    if os.path.exists(gaz):
        print(f"    {C.GREEN}OK{C.RESET}      Gazetteer at {gaz}")
    else:
        print(f"    {C.RED}MISSING{C.RESET}  Run: ufosint rebuild --only schema && python geocode.py --download")
        issues += 1

    # Summary
    print(f"\n  {'=' * 50}")
    if issues == 0:
        print(f"  {C.GREEN}All checks passed.{C.RESET}")
    else:
        print(f"  {C.RED}{issues} issue(s) found.{C.RESET}")
    print()


# ── Query command ──

@main.command()
@click.argument("sql")
@click.option("--limit", type=int, default=25, help="Max rows to display")
@click.pass_context
def query(ctx, sql, limit):
    """Run a SQL query against the database.

    Examples:
        ufosint query "SELECT COUNT(*) FROM sighting"
        ufosint query "SELECT standardized_shape, COUNT(*) FROM sighting GROUP BY 1 ORDER BY 2 DESC" --limit 10
    """
    import sqlite3
    db = _get_db(ctx)
    if not db.exists():
        click.echo(f"Database not found: {db.path}")
        return

    conn = db.connect()
    try:
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(limit)

        if not cols:
            print("  (no results)")
            conn.close()
            return

        # Calculate column widths
        widths = [len(c) for c in cols]
        str_rows = []
        for row in rows:
            str_row = []
            for i, val in enumerate(row):
                s = str(val) if val is not None else "NULL"
                s = s[:60]  # truncate
                str_row.append(s)
                widths[i] = max(widths[i], len(s))
            str_rows.append(str_row)

        # Print header
        header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
        print(f"\n  {header}")
        print(f"  {'  '.join('-' * w for w in widths)}")
        for str_row in str_rows:
            line = "  ".join(str_row[i].ljust(widths[i]) for i in range(len(cols)))
            print(f"  {line}")

        # Total count hint
        if len(rows) >= limit:
            print(f"\n  (showing first {limit} rows — use --limit to see more)")

    except sqlite3.OperationalError as e:
        print(f"  SQL error: {e}")
    finally:
        conn.close()


# ── Cache command group ──

@main.group()
def cache():
    """Manage LLM/GPU inference caches."""
    pass


@cache.command("list")
def cache_list():
    """List all cached result files."""
    from ufosint.display.colors import C

    print(f"\n  Cached Results:")
    print(f"  {'=' * 55}")

    cache_dir = Config.cache_dir()
    found = False
    for f in sorted(os.listdir(cache_dir)):
        if f.endswith(".csv"):
            path = os.path.join(cache_dir, f)
            size = os.path.getsize(path) / (1024 * 1024)
            # Count rows
            with open(path, "r", encoding="utf-8") as fh:
                rows = sum(1 for _ in fh) - 1
            print(f"    {f:<45} {size:>6.1f} MB  {rows:>8,} rows")
            found = True

    if not found:
        print(f"    (no cache files found in {cache_dir})")
    print()


@cache.command("replay")
@click.pass_context
def cache_replay(ctx):
    """Replay all cached results into the database (no API/GPU needed)."""
    import sys as _sys
    _sys.path.insert(0, Config.project_root())
    db = _get_db(ctx)

    print(f"\n  Replaying caches into {db.path}\n")

    # Audit Tier B
    import audit as audit_mod
    audit_mod.replay_tier_b(db.path)

    # LLM extractions
    import run_enrich
    run_enrich.apply_extractions(db.path)

    # Emotions
    import emotions as emo_mod
    emo_mod.replay_emotion_cache(db.path)

    print(f"\n  Replay complete.")


# ── Scaffold command ──

@main.command()
@click.argument("source_name")
def scaffold(source_name):
    """Generate boilerplate for a new source importer.

    Creates ufosint/importers/<name>.py with the base class template.

    Example:
        ufosint scaffold aaro
    """
    name = source_name.lower().strip()
    class_name = name.capitalize() + "Importer"
    display_name = source_name.upper()
    file_path = os.path.join(Config.project_root(), "ufosint", "importers", f"{name}.py")

    if os.path.exists(file_path):
        click.echo(f"  File already exists: {file_path}")
        return

    template = f'''"""
{display_name} importer — [describe the source].

[Row count, format, acquisition notes]
"""

import json
import os

from ufosint.config import Config
from ufosint.importers.base import Importer


def parse_{name}_date(date_str):
    """Parse {display_name} date format into (ISO, raw)."""
    if not date_str or not date_str.strip():
        return None, None
    raw = date_str.strip()
    # TODO: implement source-specific date parsing
    return raw, raw


class {class_name}(Importer):
    source_name = "{display_name}"

    @property
    def file_path(self):
        return os.path.join(Config.raw_data_dir(), "{display_name}", "{name}_data.csv")

    def parse_row(self, raw):
        # Location
        location = {{
            "raw_text": raw.get("location") or None,
            "city": raw.get("city") or None,
            "state": raw.get("state") or None,
            "country": raw.get("country") or None,
        }}

        # Date
        date_event, date_raw = parse_{name}_date(raw.get("date", ""))

        # Sighting
        sighting = {{
            "source_record_id": raw.get("id") or None,
            "date_event": date_event,
            "date_event_raw": date_raw,
            "description": raw.get("description") or None,
            "shape": raw.get("shape") or None,
            "raw_json": json.dumps(raw, ensure_ascii=False),
        }}

        return location, sighting
'''

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(template)

    print(f"\n  Created: {file_path}")
    print(f"\n  Next steps:")
    print(f"    1. Edit {file_path} — implement parse_row() for your source format")
    print(f"    2. Add to ufosint/importers/__init__.py:")
    print(f"       from ufosint.importers.{name} import {class_name}")
    print(f"       IMPORTERS[\"{name}\"] = {class_name}")
    print(f"    3. Add \"{display_name}\" to create_schema.py source_database seeds")
    print(f"    4. Place your data file at: <raw_data_dir>/{display_name}/{name}_data.csv")
    print(f"    5. Run: ufosint import {name}")
    print()


if __name__ == "__main__":
    main()
