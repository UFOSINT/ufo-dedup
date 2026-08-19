"""Tests for ufosint.processors.country — coordinate to ISO-3166 country.

The two exclusion rules carry most of the weight here. Nearest-city is a
cheap stand-in for point-in-polygon and it is wrong offshore and near
borders, so the module is required to decline rather than guess. These
tests pin that refusal: a regression that "improves coverage" by answering
in those cases is a regression, not an improvement.
"""
import pytest

from ufosint.processors import country as C


@pytest.fixture(scope="module")
def gaz():
    return C.Gazetteer().load()


# ---------------------------------------------------------------------------
# Gazetteer loading
# ---------------------------------------------------------------------------

def test_gazetteer_loads_places_with_iso_codes(gaz):
    assert len(gaz.lat) > 100_000
    assert len(gaz.lat) == len(gaz.lng) == len(gaz.iso2)
    assert all(len(c) == 2 for c in gaz.iso2[:500])


# ---------------------------------------------------------------------------
# Interior points resolve
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,lat,lng,expected", [
    ("New Delhi",  28.6139,   77.2090, "IN"),
    ("Mumbai",     19.0760,   72.8777, "IN"),
    ("Phoenix",    33.4484, -112.0740, "US"),
    ("London",     51.5074,   -0.1278, "GB"),
    ("Tokyo",      35.6762,  139.6503, "JP"),
    ("Sao Paulo", -23.5505,  -46.6333, "BR"),
    ("Perth",     -31.9505,  115.8605, "AU"),
])
def test_interior_points_resolve(gaz, name, lat, lng, expected):
    iso2, reason = gaz.lookup(lat, lng)
    assert reason == "ok", f"{name} should resolve, got {reason}"
    assert iso2 == expected


# ---------------------------------------------------------------------------
# Offshore is declined, not guessed
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,lat,lng", [
    ("mid-Atlantic",  30.0,  -40.0),
    ("mid-Pacific",    0.0, -150.0),
    ("Antarctica",   -82.0,    0.0),
    ("North Sea",     56.0,    3.0),
    ("Indian Ocean", -20.0,   75.0),
])
def test_offshore_returns_none(gaz, name, lat, lng):
    iso2, reason = gaz.lookup(lat, lng)
    assert iso2 is None, f"{name} must not be country-coded (got {iso2})"
    assert reason == "offshore"


# ---------------------------------------------------------------------------
# Borders are declined, not guessed
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,lat,lng", [
    ("El Paso / Ciudad Juarez", 31.7619, -106.4850),
    ("Detroit / Windsor",       42.3314,  -83.0458),
])
def test_border_returns_none(gaz, name, lat, lng):
    iso2, reason = gaz.lookup(lat, lng)
    assert iso2 is None, f"{name} straddles a border and must not be coded"
    assert reason == "border"


def test_border_margin_is_what_suppresses_the_answer(gaz):
    """With the margin at zero the same point resolves.

    Guards against the border exclusion silently becoming a no-op or, worse,
    swallowing interior points for some unrelated reason.
    """
    lat, lng = 31.7619, -106.4850
    assert gaz.lookup(lat, lng)[0] is None

    original = C.BORDER_MARGIN_KM
    try:
        C.BORDER_MARGIN_KM = 0.0
        iso2, reason = gaz.lookup(lat, lng)
        assert reason == "ok"
        assert iso2 in {"US", "MX"}
    finally:
        C.BORDER_MARGIN_KM = original


# ---------------------------------------------------------------------------
# Bad input
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lat,lng,reason", [
    (None,   10.0, "no_coords"),
    (10.0,   None, "no_coords"),
    (91.0,   10.0, "bad_coords"),
    (10.0,  181.0, "bad_coords"),
])
def test_invalid_coords_declined(gaz, lat, lng, reason):
    iso2, got = gaz.lookup(lat, lng)
    assert iso2 is None
    assert got == reason


def test_antimeridian_does_not_crash(gaz):
    """Longitude cells wrap; a point at +179.9 must still find candidates."""
    for lng in (179.9, -179.9):
        iso2, reason = gaz.lookup(-16.5, lng)
        assert reason in {"ok", "offshore", "border"}
