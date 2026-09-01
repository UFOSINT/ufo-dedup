"""v0.16.9 — NRC word counts must reach sighting.nrc_*.

The `sentiment` step writes NRC counts into sentiment_analysis.emo_*, but
every consumer reads sighting.nrc_*: the packed map buffer (bytes 40-47)
and the Insights emotion donut. denormalize_nrc() is the only thing that
copies between the two.

Its only caller was the root script gerb_overlay.py, deleted in 56c87c3.
That refactor rehomed the script's other nuclear.py function
(run_gerb_overlay, now in _step_replay) and missed this one. It sat
orphaned, and every rebuild after it left sighting.nrc_* NULL for the
whole corpus — 365,600 populated rows before the v0.16.4 rebuild, 0
after.

Nothing failed loudly. The source rows were fine (461,551 of them, more
than the old build had), VADER writes down a different path so sentiment
still worked, and no rebuild ran for four months. That combination is why
this needs a test rather than a comment.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from ufosint.processors.nuclear import denormalize_nrc

ROOT = Path(__file__).resolve().parent.parent

NRC = ["joy", "fear", "anger", "sadness", "surprise", "disgust", "trust",
       "anticipation"]


def _make_db(path):
    """Minimal schema: just what denormalize_nrc touches."""
    conn = sqlite3.connect(path)
    nrc_cols = ", ".join(f"nrc_{e} INTEGER" for e in NRC)
    emo_cols = ", ".join(f"emo_{e} INTEGER" for e in NRC)
    conn.execute(f"CREATE TABLE sighting (id INTEGER PRIMARY KEY, {nrc_cols})")
    conn.execute(
        f"CREATE TABLE sentiment_analysis ("
        f"id INTEGER PRIMARY KEY, sighting_id INTEGER, {emo_cols})"
    )
    return conn


# ---------------------------------------------------------------------------
# The copy itself
# ---------------------------------------------------------------------------

def test_denormalize_copies_emo_to_nrc(tmp_path):
    conn = _make_db(tmp_path / "t.db")
    conn.execute("INSERT INTO sighting (id) VALUES (1), (2)")
    conn.execute(
        "INSERT INTO sentiment_analysis (sighting_id, %s) VALUES (1, %s)"
        % (", ".join(f"emo_{e}" for e in NRC), ", ".join(str(i) for i in range(1, 9)))
    )
    conn.commit()

    denormalize_nrc(conn)

    row = conn.execute(
        "SELECT %s FROM sighting WHERE id = 1" % ", ".join(f"nrc_{e}" for e in NRC)
    ).fetchone()
    assert row == tuple(range(1, 9)), (
        f"NRC counts did not reach sighting.nrc_*: got {row}"
    )

    # A sighting with no sentiment row keeps NULLs rather than gaining zeros —
    # "not analysed" and "analysed, scored zero" are different claims.
    row2 = conn.execute("SELECT nrc_joy FROM sighting WHERE id = 2").fetchone()
    assert row2[0] is None


def test_denormalize_is_safe_with_no_sentiment_data(tmp_path):
    """Must no-op, not raise — it runs unconditionally in the pipeline."""
    conn = _make_db(tmp_path / "t.db")
    conn.execute("INSERT INTO sighting (id) VALUES (1)")
    conn.commit()
    denormalize_nrc(conn)  # sentiment_analysis is empty
    assert conn.execute("SELECT nrc_joy FROM sighting").fetchone()[0] is None


# ---------------------------------------------------------------------------
# The wiring — this is the assertion that would have caught the outage
# ---------------------------------------------------------------------------

def test_pipeline_actually_calls_denormalize_nrc():
    """denormalize_nrc must have a caller inside the package.

    It was orphaned for four months because deleting its caller broke
    nothing that any test or import could see. A function that only the
    pipeline is meant to call needs the call site pinned.
    """
    hits = [
        p for p in (ROOT / "ufosint").rglob("*.py")
        if p.name != "nuclear.py"
        and "denormalize_nrc(" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert hits, (
        "denormalize_nrc() has no caller in ufosint/ — sighting.nrc_* will "
        "be NULL after every rebuild, and the Insights emotion donut will "
        "render empty"
    )


def test_denormalize_runs_after_the_processors_loop():
    """Order dependency: it reads what `sentiment` and the processors write."""
    src = (ROOT / "ufosint" / "pipeline.py").read_text(encoding="utf-8")
    call_at = src.find("denormalize_nrc(conn)")
    loop_at = src.find("proc.process(conn)")
    assert call_at != -1 and loop_at != -1
    assert loop_at < call_at, (
        "denormalize_nrc must run after the PROCESSORS loop"
    )
