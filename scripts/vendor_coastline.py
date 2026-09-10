"""Vendor a simplified coastline for the dashboard basemap.

Kept in the repo rather than run once and forgotten, so the provenance of
api/static/coastline.json is auditable and the file can be regenerated.

Source: Natural Earth 1:50m Admin 0 countries, which is in the **public
domain** - no attribution required, though it is given in the output anyway.

Note the coastline's resolution says nothing about the data's. The basemap is
there so a reader can orient themselves; the aggregates remain 1-degree cells
regardless. 110m was tried first and rendered Great Britain as an unreadable
polygon, which looked like a mistake rather than like restraint.

The output is clipped to the configured region and stored as plain polylines,
because the basemap is decoration - nothing is computed from it.

    python scripts/vendor_coastline.py
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

SOURCE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_50m_admin_0_countries.geojson"
)
OUTPUT = Path(__file__).resolve().parent.parent / "api" / "static" / "coastline.json"

# The region of interest, plus a margin so coastlines run off the edge of the
# map rather than stopping short at the frame.
MARGIN = 3.0
LAT_MIN, LAT_MAX = 49.0 - MARGIN, 61.0 + MARGIN
LON_MIN, LON_MAX = -10.0 - MARGIN, 3.0 + MARGIN

# Everything with a coastline inside the frame.
WANTED = {
    "United Kingdom",
    "Ireland",
    "France",
    "Belgium",
    "Netherlands",
    "Germany",
    "Denmark",
    "Norway",
    "Iceland",
    "Faroe Islands",
    "Isle of Man",
}

# Two decimal places is about 1 km, far finer than a 110m source resolves and
# far finer than the aggregates warrant.
PRECISION = 2
MIN_POINTS = 3


def inside(lon: float, lat: float) -> bool:
    return LON_MIN <= lon <= LON_MAX and LAT_MIN <= lat <= LAT_MAX


def clip_ring(ring: list) -> list[list]:
    """Split a ring into the runs of points that fall inside the frame.

    A real polygon clip would be the correct thing for filled geography. These
    are stroked outlines, so splitting into polylines is both simpler and
    visually identical inside the frame.
    """
    runs, current = [], []
    for point in ring:
        lon, lat = float(point[0]), float(point[1])
        if inside(lon, lat):
            current.append([round(lon, PRECISION), round(lat, PRECISION)])
        elif current:
            runs.append(current)
            current = []
    if current:
        runs.append(current)
    return [run for run in runs if len(run) >= MIN_POINTS]


def rings_of(geometry: dict):
    kind, coords = geometry["type"], geometry["coordinates"]
    if kind == "Polygon":
        yield from coords
    elif kind == "MultiPolygon":
        for polygon in coords:
            yield from polygon


def main() -> None:
    with urllib.request.urlopen(SOURCE_URL, timeout=60) as response:
        data = json.loads(response.read().decode("utf-8"))

    lines: list[list] = []
    matched: set[str] = set()

    for feature in data["features"]:
        name = feature["properties"].get("ADMIN")
        if name not in WANTED:
            continue
        for ring in rings_of(feature["geometry"]):
            for run in clip_ring(ring):
                lines.append(run)
                matched.add(name)

    payload = {
        "source": "Natural Earth 1:50m Admin 0 countries",
        "source_url": "https://www.naturalearthdata.com/",
        "licence": "Public domain",
        "note": "Basemap decoration only. Nothing is computed from this geometry.",
        "bbox": [LON_MIN, LAT_MIN, LON_MAX, LAT_MAX],
        "lines": lines,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_bytes((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))

    points = sum(len(line) for line in lines)
    print(f"countries: {sorted(matched)}")
    print(f"polylines: {len(lines)}  points: {points}")
    print(f"written:   {OUTPUT}  ({OUTPUT.stat().st_size / 1024:.1f} KiB)")


if __name__ == "__main__":
    main()
