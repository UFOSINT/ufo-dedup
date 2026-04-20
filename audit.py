"""
LLM-powered data quality audit pipeline for the unified UFO sightings database.

Usage:
    python audit.py --stats
    python audit.py --tier a
    python audit.py --fix-geocodes
    python audit.py --tier b --limit 500
"""
# ──────────────────────────────────────────────────────────────
# LEGACY SCRIPT — prefer the unified CLI:
#   ufosint audit
#
# This file still works standalone but the canonical implementation
# is in the ufosint/ package. See: pip install -e . && ufosint --help
# ──────────────────────────────────────────────────────────────

import os

from ufosint.llm.audit import (
    # Re-export all public functions for backward compatibility
    tier_a_geocode_verify,
    tier_b_location_normalize,
    tier_c_data_mine,
    apply_tier_c_extractions,
    replay_tier_b,
    reset_audit,
    run_audit_pipeline,
    print_stats,
    main,
    # Internal functions used by run_audit.py
    _process_tier_b_batch,
    _create_batch,
    _complete_batch,
    _call_openrouter,
    _parse_json_response,
    # Constants
    DEFAULT_MODEL,
    OPENROUTER_API_KEY,
    OPENROUTER_URL,
    BATCH_SIZE,
    US_STATE_BOUNDS,
    CA_PROVINCE_BOUNDS,
    TIER_B_SYSTEM_PROMPT,
    TIER_B_BATCH_PROMPT,
    FIXES_CSV_PATH,
)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "output", "ufo_unified.db")

if __name__ == "__main__":
    main()
