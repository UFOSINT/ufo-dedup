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


# ---------------------------------------------------------------------------
# NUFORC source file selection
# ---------------------------------------------------------------------------

def test_nuforc_prefers_refreshed_extract(tmp_path, monkeypatch):
    """nuforcpy.csv supersedes nuforc.csv and must win when both exist.

    Verified a strict superset of the original: identical header, all 159,320
    original case numbers present, 2,253 added, none dropped.
    """
    from ufosint.importers.nuforc import NuforcImporter
    from ufosint.config import Config

    monkeypatch.setattr(Config, "raw_data_dir", staticmethod(lambda: str(tmp_path)))
    imp = NuforcImporter()

    (tmp_path / "nuforc.csv").write_text("x", encoding="utf-8")
    assert imp.file_path.endswith("nuforc.csv"), "must fall back when only the old file exists"

    (tmp_path / "nuforcpy.csv").write_text("x", encoding="utf-8")
    assert imp.file_path.endswith("nuforcpy.csv"), "refreshed extract must take precedence"


def test_nuforc_names_preferred_file_when_none_present(tmp_path, monkeypatch):
    """A missing-file error should name the file we actually want."""
    from ufosint.importers.nuforc import NuforcImporter
    from ufosint.config import Config

    monkeypatch.setattr(Config, "raw_data_dir", staticmethod(lambda: str(tmp_path)))
    assert NuforcImporter().file_path.endswith("nuforcpy.csv")


# ---------------------------------------------------------------------------
# JSON source shape — the UFO-search regression
# ---------------------------------------------------------------------------

def test_geldreich_declares_its_json_root():
    """majestic.json nests records under a key, not a bare list.

    Without json_root the base reader returned the dict, iteration yielded its
    single key, and the import reported "0 imported, 1 skipped" while exiting
    successfully — silently dropping all 54,751 UFO-search rows from a rebuild.
    """
    from ufosint.importers.geldreich import GeldreichImporter
    assert GeldreichImporter().json_root == "Majestic Timeline"


def test_json_root_unwraps(tmp_path, monkeypatch):
    import json as _json
    from ufosint.importers.base import Importer

    class Wrapped(Importer):
        source_name = "T"
        @property
        def file_path(self): return str(tmp_path / "d.json")
        @property
        def file_format(self): return "json"
        @property
        def json_root(self): return "records"
        def parse_row(self, raw): return {}, {}

    (tmp_path / "d.json").write_text(_json.dumps({"records": [{"a": 1}, {"a": 2}]}), encoding="utf-8")
    assert Wrapped()._read_source() == [{"a": 1}, {"a": 2}]


def test_wrong_json_root_raises_instead_of_importing_nothing(tmp_path):
    import json as _json
    import pytest as _pytest
    from ufosint.importers.base import Importer

    class Wrapped(Importer):
        source_name = "T"
        @property
        def file_path(self): return str(tmp_path / "d.json")
        @property
        def file_format(self): return "json"
        @property
        def json_root(self): return "records"
        def parse_row(self, raw): return {}, {}

    (tmp_path / "d.json").write_text(_json.dumps({"other": [{"a": 1}]}), encoding="utf-8")
    with _pytest.raises(ValueError, match="expected a dict with key"):
        Wrapped()._read_source()


def test_dict_json_without_root_raises(tmp_path):
    import json as _json
    import pytest as _pytest
    from ufosint.importers.base import Importer

    class Bare(Importer):
        source_name = "T"
        @property
        def file_path(self): return str(tmp_path / "d.json")
        @property
        def file_format(self): return "json"
        def parse_row(self, raw): return {}, {}

    (tmp_path / "d.json").write_text(_json.dumps({"a": [1]}), encoding="utf-8")
    with _pytest.raises(ValueError, match="did not yield a list"):
        Bare()._read_source()


def test_geldreich_handles_list_valued_location():
    """majestic.json gives `location` as a list for multi-place cases.

    3,567 of 54,751 records. This raised AttributeError on .strip(), which the
    base importer's bare `except Exception` swallowed as a skip — so a rebuild
    silently dropped them.
    """
    from ufosint.importers.geldreich import parse_geldreich_location

    city, state, country, raw = parse_geldreich_location(
        ["Lyon, France", "Magonia", "Mahon, Menorca"]
    )
    assert city == "France"
    assert raw == "Lyon, France; Magonia; Mahon, Menorca", "full list must be preserved"

    assert parse_geldreich_location([]) == (None, None, None, None)
    assert parse_geldreich_location(["", "  "]) == (None, None, None, None)
    # strings still behave
    assert parse_geldreich_location("Phoenix, AZ") == ("Phoenix", "AZ", "US", "Phoenix, AZ")


def test_geldreich_every_record_parses():
    """No record may fall into the base importer's silent exception skip."""
    import os
    from ufosint.importers.geldreich import GeldreichImporter
    imp = GeldreichImporter()
    if not os.path.exists(imp.file_path):
        import pytest as _p
        _p.skip("majestic.json not available in this checkout")
    rows = imp._read_source()
    bad = []
    for r in rows:
        try:
            loc, s = imp.parse_row(r)
        except Exception as e:
            bad.append(type(e).__name__)
    assert not bad, f"{len(bad)} records raised during parse_row: {set(bad)}"


# ---------------------------------------------------------------------------
# location_id assignment — the corruption the first rebuild produced
# ---------------------------------------------------------------------------

def test_lastrowid_is_not_reliable_after_executemany():
    """Documents the sqlite3 behaviour this class of bug rests on.

    If a future Python starts setting lastrowid for executemany, this test
    fails and the note below can be revisited — but the importer must not go
    back to depending on it either way.
    """
    import sqlite3 as _s
    c = _s.connect(":memory:")
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
    cur = c.cursor()
    cur.execute("INSERT INTO t (id, v) VALUES (?, ?)", (None, "seed"))
    seeded = cur.lastrowid
    cur.executemany("INSERT INTO t (id, v) VALUES (?, ?)",
                    [(None, f"r{i}") for i in range(10)])
    assert cur.lastrowid == seeded, "lastrowid unexpectedly tracked executemany"
    assert c.execute("SELECT MAX(id) FROM t").fetchone()[0] == 11
    c.close()


def test_importer_assigns_correct_location_ids(tmp_path):
    """Every sighting must point at its own location, across batch boundaries.

    The v0.16.4 rebuild produced sighting.location_id values from -9999 up,
    and only 149,778 of 573,210 rows joined to a location at all.
    """
    import sqlite3 as _s
    from ufosint.importers.base import Importer
    from ufosint.db import Database

    db_path = tmp_path / "t.db"
    conn = _s.connect(db_path)
    conn.executescript("""
        CREATE TABLE location (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_text TEXT,
            city TEXT, county TEXT, state TEXT, country TEXT, region TEXT,
            latitude REAL, longitude REAL, geoname_id INTEGER);
        CREATE TABLE source_database (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        CREATE TABLE source_origin (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        CREATE TABLE sighting (id INTEGER PRIMARY KEY AUTOINCREMENT, source_db_id INTEGER,
            location_id INTEGER, source_record_id TEXT);
        INSERT INTO source_database (name) VALUES ('T');
    """)
    conn.commit(); conn.close()

    class Tiny(Importer):
        source_name = "T"
        batch_size = 10          # force several batches
        @property
        def file_path(self): return str(tmp_path / "src.json")
        @property
        def file_format(self): return "json"
        def parse_row(self, raw):
            return ({"raw_text": raw["c"], "city": raw["c"]},
                    {"source_record_id": raw["c"]})

    import json as _json
    (tmp_path / "src.json").write_text(
        _json.dumps([{"c": f"city{i}"} for i in range(35)]), encoding="utf-8")

    Tiny().run(Database(str(db_path)))

    conn = _s.connect(db_path)
    n = conn.execute("SELECT COUNT(*) FROM sighting").fetchone()[0]
    joined = conn.execute(
        "SELECT COUNT(*) FROM sighting s JOIN location l ON l.id = s.location_id"
    ).fetchone()[0]
    assert n == 35
    assert joined == 35, f"only {joined}/{n} sightings joined to their location"

    lo = conn.execute("SELECT MIN(location_id) FROM sighting").fetchone()[0]
    assert lo >= 1, f"negative location_id leaked in: {lo}"

    # each sighting must point at ITS OWN city, not another row's
    bad = conn.execute("""
        SELECT COUNT(*) FROM sighting s JOIN location l ON l.id = s.location_id
        WHERE l.city != s.source_record_id
    """).fetchone()[0]
    assert bad == 0, f"{bad} sightings are wearing another row's location"
    conn.close()


def test_locations_are_shared_not_per_sighting(tmp_path):
    """Two sightings at the same place must point at one location row.

    The Phase 3 refactor dropped this. Because geocoding resolves places
    rather than sightings, one row per sighting cut map coverage by a third
    on a rebuild that otherwise added 97,015 sightings.
    """
    import json as _json
    import sqlite3 as _s
    from ufosint.importers.base import Importer
    from ufosint.db import Database

    db_path = tmp_path / "t.db"
    conn = _s.connect(db_path)
    conn.executescript("""
        CREATE TABLE location (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_text TEXT,
            city TEXT, county TEXT, state TEXT, country TEXT, region TEXT,
            latitude REAL, longitude REAL, geoname_id INTEGER);
        CREATE TABLE source_database (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        CREATE TABLE source_origin (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        CREATE TABLE sighting (id INTEGER PRIMARY KEY AUTOINCREMENT, source_db_id INTEGER,
            location_id INTEGER, source_record_id TEXT);
        INSERT INTO source_database (name) VALUES ('T');
    """)
    conn.commit(); conn.close()

    class Tiny(Importer):
        source_name = "T"
        batch_size = 4                     # force reuse across batch boundaries
        @property
        def file_path(self): return str(tmp_path / "src.json")
        @property
        def file_format(self): return "json"
        def parse_row(self, raw):
            return ({"raw_text": raw["c"], "city": raw["c"], "state": "AZ"},
                    {"source_record_id": raw["id"]})

    # 12 sightings across 3 distinct places
    recs = [{"id": str(i), "c": ["Phoenix", "Tucson", "Mesa"][i % 3]} for i in range(12)]
    (tmp_path / "src.json").write_text(_json.dumps(recs), encoding="utf-8")

    Tiny().run(Database(str(db_path)))

    conn = _s.connect(db_path)
    sightings = conn.execute("SELECT COUNT(*) FROM sighting").fetchone()[0]
    locations = conn.execute("SELECT COUNT(*) FROM location").fetchone()[0]
    assert sightings == 12
    assert locations == 3, f"expected 3 shared locations, got {locations}"

    # and every sighting still points at the RIGHT one
    bad = conn.execute("""
        SELECT COUNT(*) FROM sighting s JOIN location l ON l.id = s.location_id
        WHERE l.city != (SELECT CASE CAST(s.source_record_id AS INTEGER) % 3
                              WHEN 0 THEN 'Phoenix' WHEN 1 THEN 'Tucson' ELSE 'Mesa' END)
    """).fetchone()[0]
    assert bad == 0, f"{bad} sightings point at the wrong shared location"
    conn.close()


def test_location_reuse_distinguishes_explicit_coordinates(tmp_path):
    """Same name, different explicit coords = different places, kept apart."""
    import json as _json
    import sqlite3 as _s
    from ufosint.importers.base import Importer
    from ufosint.db import Database

    db_path = tmp_path / "t.db"
    conn = _s.connect(db_path)
    conn.executescript("""
        CREATE TABLE location (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_text TEXT,
            city TEXT, county TEXT, state TEXT, country TEXT, region TEXT,
            latitude REAL, longitude REAL, geoname_id INTEGER);
        CREATE TABLE source_database (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        CREATE TABLE source_origin (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        CREATE TABLE sighting (id INTEGER PRIMARY KEY AUTOINCREMENT, source_db_id INTEGER,
            location_id INTEGER, source_record_id TEXT);
        INSERT INTO source_database (name) VALUES ('T');
    """)
    conn.commit(); conn.close()

    class Tiny(Importer):
        source_name = "T"
        @property
        def file_path(self): return str(tmp_path / "src.json")
        @property
        def file_format(self): return "json"
        def parse_row(self, raw):
            return ({"raw_text": "Springfield", "city": "Springfield",
                     "latitude": raw["lat"], "longitude": raw["lng"]},
                    {"source_record_id": raw["id"]})

    (tmp_path / "src.json").write_text(_json.dumps([
        {"id": "1", "lat": 39.8, "lng": -89.6},
        {"id": "2", "lat": 39.8, "lng": -89.6},   # identical -> shared
        {"id": "3", "lat": 42.1, "lng": -72.6},   # different -> separate
    ]), encoding="utf-8")

    Tiny().run(Database(str(db_path)))
    conn = _s.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM location").fetchone()[0] == 2
    conn.close()


# ---------------------------------------------------------------------------
# Source column mappings — fields silently read from columns that don't exist
# ---------------------------------------------------------------------------

def test_updb_reads_real_csv_columns():
    """UPDB's columns are id, source, source_id, name, date, location, city,
    country, description.

    The importer used to split `location` into city/state/country, but that
    column is a numeric GeoNames reference ("5193892"). It also read
    case_number, short_desc and long_desc, none of which exist. Result: every
    UPDB row imported with a number for a city and NULL description, and 0 of
    159,778 geocoded against 39.5% in the April corpus.
    """
    from ufosint.importers.updb import UpdbImporter
    row = {
        "id": "5182466", "source": "3", "source_id": "2csohq", "name": "NICAP",
        "date": "1993-05-20 00:00:00", "location": "5193892",
        "city": "OTTAWA", "country": "CA",
        "description": "Airliner crew saw a dark blue triangular object.",
    }
    loc, s = UpdbImporter().parse_row(row)

    assert loc["city"] == "OTTAWA"
    assert loc["country"] == "CA"
    assert loc["raw_text"] == "OTTAWA, CA"
    assert "5193892" not in str(loc), "the numeric GeoNames id must not become a place"

    assert s["source_record_id"] == "5182466"
    assert s["origin_record_id"] == "2csohq"
    assert s["origin_name"] == "NICAP"
    assert s["description"].startswith("Airliner crew")


def test_updb_row_without_place_yields_no_location_text():
    from ufosint.importers.updb import UpdbImporter
    loc, _s = UpdbImporter().parse_row(
        {"id": "1", "source_id": "x", "name": "NICAP", "date": "1900-01-01",
         "location": "5176696", "city": "", "country": "", "description": "d"}
    )
    assert loc["city"] is None and loc["country"] is None
    assert loc["raw_text"] is None


def test_ufocat_reads_source_coordinates():
    """The columns are LATITUDE / LONGITUDE, not LAT / LON.

    raw.get("LAT") always returned "", so every UFOCAT coordinate was
    discarded and the source fell back to name-only geocoding — 44.7% mapped
    against 89.3% in April. Values carry leading whitespace.
    """
    from ufosint.importers.ufocat import UfocatImporter
    loc, _s = UfocatImporter().parse_row({
        "LOCATION": "CINTEGABELLE W", "LATITUDE": "  43.33", "LONGITUDE": "  -1.48",
        "DATE": "19730101", "SOURCE": "X",
    })
    assert loc["latitude"] == 43.33
    assert loc["longitude"] == -1.48


def test_ufocat_blank_coordinates_stay_none():
    from ufosint.importers.ufocat import UfocatImporter
    loc, _s = UfocatImporter().parse_row({
        "LOCATION": "LAS PISCINAS", "LATITUDE": "", "LONGITUDE": "",
        "DATE": "19730101", "SOURCE": "X",
    })
    assert loc["latitude"] is None and loc["longitude"] is None
