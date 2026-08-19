"""
Derive an ISO-3166-1 alpha-2 country code for a location from its coordinates.

Why coordinates and not the `country` text column: that column holds 715
distinct values across the corpus — a mix of ISO codes, full country names,
US/Canadian state codes (TX, FL, NY, MB), cities, oceans, placeholders
("Unspecified", "International Waters") and CSV parse fragments. Only 41.2%
of mapped sightings carry a usable one. Coordinates are unambiguous and
present on every mapped row, so they are the authoritative source; the text
column is a fallback for rows that have no coordinates at all.

Method is nearest populated place from the GeoNames gazetteer
(geodata/cities1000.txt, column 8 = ISO country code). Nearest-city is an
approximation of point-in-polygon, and it is wrong in exactly two situations
— so this module refuses to answer in both rather than guessing:

  OFFSHORE  The nearest populated place is further than OFFSHORE_KM. Ocean
            sightings, Antarctic sightings and remote wilderness land here.
            Assigning them the nearest coastline's country would invent a
            fact the data does not support.

  BORDER    A populated place in a *different* country sits within
            BORDER_MARGIN_KM of the nearest one. Near a land border the
            nearest city is a coin flip, so the answer is withheld.

Both cases return None with a reason. A None is a real, reportable outcome
here — not a failure — and callers should store NULL rather than a guess.
"""

import math
import os
from collections import defaultdict

import numpy as np

from ufosint.config import Config

# A populated place further away than this means we are not confident the
# point sits in that place's country. 100 km comfortably covers sparse land
# (the emptiest parts of Australia, Nevada, Siberia still fall inside) while
# putting anything genuinely offshore out of reach.
OFFSHORE_KM = 100.0

# If the closest city in some other country is within this margin of the
# closest city overall, the point is close enough to a border that
# nearest-city cannot separate them.
BORDER_MARGIN_KM = 25.0

EARTH_RADIUS_KM = 6371.0088

# Grid cell size in degrees for the candidate index.
_CELL_DEG = 1.0


class Gazetteer:
    """Spatial index over GeoNames populated places, keyed by country.

    Loads once, then answers nearest-country queries in roughly constant
    time via a 1-degree grid. cities1000 is ~168k rows, which is small
    enough to keep resident as three parallel numpy arrays.
    """

    def __init__(self, path=None):
        self.path = path or os.path.join(
            Config.project_root(), "geodata", "cities1000.txt"
        )
        self.lat = None
        self.lng = None
        self.iso2 = None
        self._grid = defaultdict(list)

    def load(self):
        lats, lngs, isos = [], [], []
        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                parts = line.split("\t")
                # GeoNames layout: 4=lat, 5=lng, 8=country code.
                if len(parts) < 9:
                    continue
                cc = parts[8].strip().upper()
                if len(cc) != 2:
                    continue
                try:
                    la = float(parts[4])
                    ln = float(parts[5])
                except ValueError:
                    continue
                lats.append(la)
                lngs.append(ln)
                isos.append(cc)

        self.lat = np.asarray(lats, dtype=np.float64)
        self.lng = np.asarray(lngs, dtype=np.float64)
        self.iso2 = np.asarray(isos, dtype="U2")

        for i in range(len(lats)):
            self._grid[(int(math.floor(lats[i] / _CELL_DEG)),
                        int(math.floor(lngs[i] / _CELL_DEG)))].append(i)
        for k in self._grid:
            self._grid[k] = np.asarray(self._grid[k], dtype=np.int64)
        return self

    def _candidates(self, lat, lng, radius_km):
        """Indices of places whose grid cell intersects the search box.

        Longitude degrees shrink toward the poles, so the box widens with
        latitude. Above ~89 degrees the cosine collapses and we simply take
        every longitude band.
        """
        dlat = radius_km / 111.32
        coslat = math.cos(math.radians(lat))
        if coslat < 0.02:
            lng_cells = range(int(-180 / _CELL_DEG), int(180 / _CELL_DEG) + 1)
        else:
            dlng = radius_km / (111.32 * coslat)
            lo = int(math.floor((lng - dlng) / _CELL_DEG))
            hi = int(math.floor((lng + dlng) / _CELL_DEG))
            lng_cells = range(lo, hi + 1)

        lat_lo = int(math.floor((lat - dlat) / _CELL_DEG))
        lat_hi = int(math.floor((lat + dlat) / _CELL_DEG))

        out = []
        for la in range(lat_lo, lat_hi + 1):
            for ln in lng_cells:
                # Wrap longitude cells across the antimeridian.
                key = (la, ((ln + 180) % 360) - 180)
                hit = self._grid.get(key)
                if hit is not None:
                    out.append(hit)
        if not out:
            return np.empty(0, dtype=np.int64)
        return np.concatenate(out)

    def lookup(self, lat, lng):
        """Return (iso2, reason) for a coordinate.

        iso2 is None when the point is offshore or too close to a border,
        with reason one of "offshore" / "border". On success reason is
        "ok". Callers should treat a None as NULL, never as unknown-but-
        guessable.
        """
        if lat is None or lng is None:
            return None, "no_coords"
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lng <= 180.0):
            return None, "bad_coords"

        # Searching beyond OFFSHORE_KM + BORDER_MARGIN_KM is pointless: a
        # nearest city past OFFSHORE_KM is offshore regardless, and the
        # border test only looks within the margin of the nearest.
        radius = OFFSHORE_KM + BORDER_MARGIN_KM
        idx = self._candidates(lat, lng, radius)
        if idx.size == 0:
            return None, "offshore"

        d = _haversine_km(lat, lng, self.lat[idx], self.lng[idx])
        order = np.argsort(d)
        d = d[order]
        codes = self.iso2[idx][order]

        nearest_km = float(d[0])
        if nearest_km > OFFSHORE_KM:
            return None, "offshore"

        winner = str(codes[0])
        other = np.nonzero(codes != winner)[0]
        if other.size:
            if float(d[other[0]]) - nearest_km < BORDER_MARGIN_KM:
                return None, "border"

        return winner, "ok"


def _haversine_km(lat1, lng1, lat2, lng2):
    """Great-circle distance in km. lat2/lng2 may be numpy arrays."""
    p1 = math.radians(lat1)
    p2 = np.radians(lat2)
    dphi = p2 - p1
    dlam = np.radians(lng2 - lng1)
    a = np.sin(dphi / 2.0) ** 2 + math.cos(p1) * np.cos(p2) * np.sin(dlam / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))
