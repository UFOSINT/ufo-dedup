"""
Pipeline runner — orchestrates the full rebuild from raw data to export.

The pipeline is a sequence of named steps with dependencies. Each step
delegates to existing modules during the transition period.

Usage:
    from ufosint.pipeline import Pipeline

    p = Pipeline()
    p.run()                          # all steps
    p.run(from_step="geocode1")      # resume from step
    p.run(skip={"emotions", "dedup"})  # skip steps
    p.run(only="analyze")            # single step
"""

import os
import sys
import time

from ufosint.config import Config
from ufosint.db import Database


# Each step: (name, label, function_name_or_callable)
# Steps are run in order. Dependencies are implicit in ordering.
STEPS = [
    ("schema",      "Create schema"),
    ("ufocat",      "Import UFOCAT"),
    ("nuforc",      "Import NUFORC"),
    ("mufon",       "Import MUFON"),
    ("updb",        "Import UPDB"),
    ("geldreich",   "Import UFO-search"),
    ("reddit",      "Import Reddit r/UFOs"),
    ("fixes",       "Apply data quality fixes"),
    ("geocode1",    "Geocode locations (pass 1)"),
    ("audit",       "Audit: fix geocodes + replay LLM location fixes"),
    ("geocode2",    "Geocode locations (pass 2)"),
    ("enrich_nuforc", "Enrich NUFORC with UFOCAT metadata"),
    ("dedup",       "Deduplication"),
    ("sentiment",   "Sentiment analysis (VADER + NRC)"),
    ("analyze",     "Derived analysis (9 steps)"),
    ("replay",      "Replay cached enrichments"),
    ("export",      "Export public DB"),
]

STEP_NAMES = [s[0] for s in STEPS]


class Pipeline:
    """Orchestrates the full rebuild pipeline."""

    def __init__(self, db=None):
        self.db = db or Database()
        self.db_path = self.db.path
        # Add project root to sys.path so legacy modules are importable
        root = Config.project_root()
        if root not in sys.path:
            sys.path.insert(0, root)

    def _patch_legacy_db_path(self):
        """Override DB_PATH in legacy modules to use our db_path.

        This is the bridge between the old scripts (hardcoded paths)
        and the new architecture (configurable paths). Removed once
        all modules are fully migrated.
        """
        for mod_name in [
            "create_schema", "geocode", "sentiment", "enrich", "dedup",
            "rebuild_db", "analyze", "audit", "emotions", "gerb_overlay",
            "export_public", "run_enrich",
        ]:
            if mod_name in sys.modules:
                mod = sys.modules[mod_name]
                if hasattr(mod, "DB_PATH"):
                    mod.DB_PATH = self.db_path

    def run(self, from_step=None, skip=None, only=None):
        """Execute the pipeline.

        Args:
            from_step: start from this step (skip prior steps)
            skip: set of step names to skip
            only: run only this single step
        """
        skip = set(skip or [])
        t0 = time.time()

        if only:
            if only not in STEP_NAMES:
                print(f"  Unknown step: {only}")
                print(f"  Available: {', '.join(STEP_NAMES)}")
                return
            self._run_step(STEP_NAMES.index(only), only,
                           dict(STEPS)[only])
            return

        started = from_step is None
        for i, (name, label) in enumerate(STEPS):
            if not started:
                if name == from_step:
                    started = True
                else:
                    continue

            if name in skip:
                print(f"\n  Skipping {name} (--skip)")
                continue

            self._run_step(i + 1, name, label)

        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"  PIPELINE COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} min)")

        # Print final stats
        self.db.print_status()

    def _run_step(self, num, name, label):
        """Execute a single pipeline step."""
        print(f"\n{'=' * 60}")
        print(f"  STEP {num}: {label}")
        print(f"{'=' * 60}\n")

        handler = getattr(self, f"_step_{name}", None)
        if handler:
            handler()
        else:
            print(f"  WARNING: no handler for step '{name}'")

    # ── Step implementations ──
    # Each delegates to existing modules. As the refactor progresses,
    # these will be replaced with calls to the new OOP classes.

    def _step_schema(self):
        import create_schema
        create_schema.DB_PATH = self.db_path
        create_schema.create_schema(self.db_path)
        self._patch_legacy_db_path()

    def _step_ufocat(self):
        from ufosint.importers import get_importer
        get_importer("ufocat").run(self.db)

    def _step_nuforc(self):
        from ufosint.importers import get_importer
        get_importer("nuforc").run(self.db)

    def _step_mufon(self):
        from ufosint.importers import get_importer
        get_importer("mufon").run(self.db)

    def _step_updb(self):
        from ufosint.importers import get_importer
        get_importer("updb").run(self.db)

    def _step_geldreich(self):
        from ufosint.importers import get_importer
        get_importer("geldreich").run(self.db)

    def _step_reddit(self):
        from ufosint.importers import get_importer
        imp = get_importer("reddit")
        if os.path.exists(imp.file_path):
            imp.run(self.db)
        else:
            print(f"  Reddit CSV not found — skipping.")

    def _step_fixes(self):
        import rebuild_db
        rebuild_db.DB_PATH = self.db_path
        rebuild_db.apply_data_fixes()

    def _step_geocode1(self):
        import geocode
        geocode.DB_PATH = self.db_path
        geocode.run_geocoding(self.db_path)

    def _step_audit(self):
        import audit as audit_mod
        audit_mod.DB_PATH = self.db_path
        audit_mod.run_audit_pipeline(self.db_path)

    def _step_geocode2(self):
        import geocode
        geocode.DB_PATH = self.db_path
        geocode.run_geocoding(self.db_path)

    def _step_enrich_nuforc(self):
        import enrich
        enrich.DB_PATH = self.db_path
        enrich.run_enrichment()

    def _step_dedup(self):
        import dedup
        dedup.DB_PATH = self.db_path
        old_argv = sys.argv
        sys.argv = ["dedup.py"]
        dedup.main()
        sys.argv = old_argv

    def _step_sentiment(self):
        import sentiment
        sentiment.DB_PATH = self.db_path
        sentiment.run_sentiment(self.db_path)

    def _step_analyze(self):
        from ufosint.processors import PROCESSORS
        conn = self.db.connect()
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

        for i, (name, cls) in enumerate(PROCESSORS.items()):
            proc = cls()
            print(f"  [{i + 1}/{len(PROCESSORS)}] {proc.label}...")
            proc.process(conn)
        conn.close()

    def _step_replay(self):
        # Replay emotion cache
        import emotions as emo_mod
        if os.path.exists(emo_mod.EMOTION_CACHE_CSV):
            emo_mod.replay_emotion_cache(self.db_path)
        else:
            print("  No emotion cache — run `ufosint emotions` to generate")

        # Replay LLM field extractions
        extract_csv = Config.cache_path("llm_field_extractions.csv")
        if os.path.exists(extract_csv):
            print("  Replaying LLM field extractions...")
            import run_enrich
            run_enrich.apply_extractions(self.db_path)
        else:
            print("  No extraction cache — run `ufosint enrich` to generate")

        # Gerb overlay
        bundle = os.path.join(Config.project_root(), "..", "uap-gerb-integration-bundle.zip")
        if os.path.exists(bundle):
            print("  Running Gerb overlay...")
            import gerb_overlay
            gerb_overlay.DB_PATH = self.db_path
            gerb_overlay.run_gerb_overlay(self.db_path, bundle)
        else:
            print("  No Gerb bundle — skipping nuclear proximity")

    def _step_export(self):
        import export_public
        # For test mode, export to a test public DB
        test_public = self.db_path.replace("unified", "public")
        export_public.DEFAULT_SOURCE = self.db_path
        export_public.DEFAULT_TARGET = test_public
        export_public.main()
