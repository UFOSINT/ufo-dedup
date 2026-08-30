"""v0.16.3 — aggregator origin retention.

UPDB and UFOCAT are aggregators: they carry cases that other bodies
originally reported. Skipping those rows is only correct when we import that
body's own richer dataset. When it isn't, the skip stops being
deduplication and becomes deletion.

That is exactly what happened to MUFON. The mufon.csv import was retired in
v0.16, but UPDB kept skipping MUFON-origin rows on the old rationale, so
MUFON coverage from UPDB went to zero rather than falling back to the
aggregator copy.

These tests pin the rule and the invariant that keeps it true: the skip set
must be derived from what the pipeline actually imports.
"""
from __future__ import annotations

import sqlite3

import pytest

from ufosint.importers.base import DIRECTLY_IMPORTED_ORIGINS
from ufosint.importers.updb import SKIP_NAMES, canonical_origin, UpdbImporter
from ufosint.pipeline import STEP_NAMES


# ---------------------------------------------------------------------------
# The invariant: skip set follows the pipeline
# ---------------------------------------------------------------------------

def test_skip_set_matches_what_the_pipeline_imports():
    """Every skipped origin must correspond to an active import step.

    If this fails, an importer was added or removed without updating
    DIRECTLY_IMPORTED_ORIGINS — which either duplicates a source or silently
    deletes its coverage from the aggregators.
    """
    for origin in DIRECTLY_IMPORTED_ORIGINS:
        assert origin.lower() in STEP_NAMES, (
            f"{origin} is skipped by aggregators but is not imported by the "
            f"pipeline — its coverage is being deleted, not deduplicated"
        )


def test_retired_sources_are_not_skipped():
    """MUFON and Reddit were purged in v0.16 and are no longer imported, so
    aggregators must retain their rows."""
    for retired in ("MUFON", "r/UFOs"):
        assert retired not in DIRECTLY_IMPORTED_ORIGINS
    assert not any("mufon" in s.lower() for s in SKIP_NAMES), (
        "UPDB must no longer skip MUFON-origin rows — mufon.csv is not imported"
    )


def test_nuforc_is_still_skipped():
    """NUFORC is still imported directly, so its UPDB copies stay redundant."""
    assert "NUFORC" in DIRECTLY_IMPORTED_ORIGINS
    assert any("nuforc" in s.lower() for s in SKIP_NAMES)


def test_retired_importers_not_in_default_pipeline():
    for retired in ("mufon", "reddit"):
        assert retired not in STEP_NAMES, (
            f"the {retired} import step would reinstate a source purged in v0.16"
        )


# ---------------------------------------------------------------------------
# Row-level behaviour
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", [
    "MUFON 12345",
    "Mutual UFO Network case 88",
    "mufon",
])
def test_mufon_rows_are_kept(name):
    imp = UpdbImporter()
    assert imp.should_skip_row({"name": name}) is False, (
        f"{name!r} must be retained — MUFON is no longer imported directly"
    )


@pytest.mark.parametrize("name", [
    "NUFORC",
    "National UFO Reporting Center",
    "nuforc 4412",
])
def test_nuforc_rows_are_skipped(name):
    imp = UpdbImporter()
    assert imp.should_skip_row({"name": name}) is True


def test_rows_without_an_origin_are_kept():
    imp = UpdbImporter()
    for raw in ({"name": ""}, {"name": None}, {}):
        assert imp.should_skip_row(raw) is False


# ---------------------------------------------------------------------------
# Origin labelling — retention is only useful if the rows stay identifiable
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("MUFON 12345",                   "MUFON"),
    ("Mutual UFO Network case 88",    "MUFON"),
    ("National UFO Reporting Center", "NUFORC"),
    ("UFODNA 88",                     "UFODNA"),
    ("Blue Book 1952",                "BLUEBOOK"),
    ("N-971",                         None),
    ("",                              None),
    (None,                            None),
])
def test_canonical_origin(raw, expected):
    assert canonical_origin(raw) == expected


def test_longest_alias_wins():
    """'National UFO Reporting Center' must not be shadowed by a short alias."""
    assert canonical_origin("National UFO Reporting Center") == "NUFORC"


def test_parse_row_emits_origin_name():
    """parse_row must always set origin_name, even when unresolved.

    Importer._flush_batch derives the INSERT column list from the first dict
    in a batch, so a key that appears on only some rows would be dropped for
    the rest.
    """
    imp = UpdbImporter()
    for name in ("MUFON 12345", "N-971", ""):
        _loc, sighting = imp.parse_row({
            "name": name, "location": "Phoenix, AZ, US",
            "date": "1997-03-13", "case_number": "X1",
            "short_desc": "s", "long_desc": "l",
        })
        assert "origin_name" in sighting
    _loc, mufon_row = imp.parse_row({
        "name": "MUFON 12345", "location": "Phoenix, AZ, US",
        "date": "1997-03-13", "case_number": "X1",
        "short_desc": "s", "long_desc": "l",
    })
    assert mufon_row["origin_name"] == "MUFON"


# ---------------------------------------------------------------------------
# origin_name -> origin_id translation
# ---------------------------------------------------------------------------

def test_origin_name_resolves_to_source_origin_fk(tmp_path):
    """The base importer must translate origin_name into the FK and drop the
    transient key, so 'MUFON via UPDB' is queryable the way the v0.16 purge
    predicate assumed."""
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE source_origin (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO source_origin (name) VALUES (?)",
                     [("MUFON",), ("NUFORC",)])
    conn.commit()
    cur = conn.cursor()

    imp = UpdbImporter()
    omap = imp._origin_id_map(cur)
    assert omap["MUFON"] == 1

    batch = [{"origin_name": "MUFON"}, {"origin_name": None}]
    for d in batch:
        name = d.pop("origin_name", None)
        d["origin_id"] = omap.get(name.strip().upper()) if name else None

    assert batch[0]["origin_id"] == 1
    assert batch[1]["origin_id"] is None
    assert all("origin_name" not in d for d in batch)
    conn.close()
