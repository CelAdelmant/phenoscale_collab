""" This script is used to generate shapefiles for each flight area, with tiles matching those of the species classifier model. 
Python environment needed "geo"
Created by: Stephanie Koolen
Last modified: 29/01/2026

Processing steps:
- Assign grid tiles to flight polygons by largest overlap
- Skip tiles where the chosen polygon has no FlightID
- Save one GeoPackage with one layer per flight (tiles)
"""
#!/usr/bin/env python3

from pathlib import Path
import csv
from collections import OrderedDict

import geopandas as gpd
from rtree import index
from pyproj import CRS

# ------------------ SETTINGS ------------------
SCRIPT_DIR = Path(__file__).resolve().parent
# auto-detect project root (nearest ancestor containing 'data') or fallback
PROJECT_ROOT = next((p for p in SCRIPT_DIR.parents if (p / "data").exists()), SCRIPT_DIR.parents[2])
DATA_DIR = PROJECT_ROOT / "data" / "drone_data"

GRID_PATH = DATA_DIR / "testgrid_15m.gpkg"
POLY_PATH = DATA_DIR / "full_wytham.gpkg"
FLIGHT_PATH = DATA_DIR / "flight_areas"   
POLY_FLD = "FlightID"
# ----------------------------------------------

def safe_layer_name(value):
    s = str(value)
    safe = "".join(ch if (ch.isalnum() or ch in (" ", "-", "_")) else "_" for ch in s).strip()
    safe = safe.replace(" ", "_") or "flight"
    # GeoPackage layer names should be <= 255 and avoid special chars; this is a simple sanitizer.
    return safe[:200]

def assign_tiles_to_flights(grid_gdf, poly_gdf, poly_field):
    """Return GeoDataFrame of grid tiles assigned to the polygon with largest overlap
       (skips tiles where the chosen polygon has no value for poly_field)."""
    # ensure same CRS
    grid_crs = CRS.from_user_input(grid_gdf.crs)
    poly_gdf = poly_gdf.to_crs(grid_crs.to_string())

    # build spatial index for polys
    idx = index.Index()
    poly_geoms = {}
    poly_rows = {}
    for i, row in poly_gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            continue
        poly_geoms[i] = g
        poly_rows[i] = row
        idx.insert(i, g.bounds)

    assigned = []
    for _, row in grid_gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            continue
        candidates = list(idx.intersection(g.bounds))
        best_pid = None
        best_area = 0.0
        for pid in candidates:
            pgeom = poly_geoms.get(pid)
            if pgeom is None:
                continue
            inter = g.intersection(pgeom)
            if inter is None or inter.is_empty:
                continue
            a = inter.area
            if a > best_area:
                best_area = a
                best_pid = pid
        if best_pid is None:
            continue
        prow = poly_rows[best_pid]
        flight_val = prow[poly_field]
        if flight_val is None or (isinstance(flight_val, str) and flight_val.strip() == ""):
            continue
        # create a simple record: original attributes + poly_fid, poly_FlightID, poly_overlap_area
        out = row.copy()
        out["poly_fid"] = int(best_pid)
        out[f"poly_{poly_field}"] = flight_val
        out["poly_overlap_area"] = float(best_area)
        assigned.append(out)

    if not assigned:
        raise RuntimeError("No tiles assigned — check polygon field values and CRS.")

    assigned_gdf = gpd.GeoDataFrame(assigned, geometry=[r.geometry for r in assigned], crs=grid_crs.to_string())
    return assigned_gdf

def write_per_flight_individual_gpkg(assigned_gdf, out_dir, poly_field):
    """
    Write one GeoPackage (.gpkg) file per flight.
    Each flight file contains a single layer (named 'tiles').
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    flight_col = f"poly_{poly_field}"
    written = []
    for fid_value, group in assigned_gdf.groupby(flight_col):
        safe_name = safe_layer_name(fid_value)
        out_gpkg = out_dir / f"flight_{safe_name}.gpkg"

        # If file exists, remove it so we start fresh (overwrite behavior)
        if out_gpkg.exists():
            out_gpkg.unlink()

        # Write the group's tiles into that gpkg; layer name 'tiles' keeps it consistent.
        group.to_file(str(out_gpkg), layer="tiles", driver="GPKG")
        written.append((out_gpkg, len(group)))
        print(f"  Wrote {len(group)} tiles -> {out_gpkg.name}")

    print(f"Done: wrote {len(written)} GeoPackage(s) to {out_dir}")
    return written

def main():
    print("Loading input files...")
    if not GRID_PATH.exists():
        raise FileNotFoundError(f"Grid not found: {GRID_PATH}")
    if not POLY_PATH.exists():
        raise FileNotFoundError(f"Polygons not found: {POLY_PATH}")

    grid = gpd.read_file(str(GRID_PATH))
    polys = gpd.read_file(str(POLY_PATH))
    print(f"Grid rows: {len(grid)}, Polygons: {len(polys)}")

    if POLY_FLD not in polys.columns:
        raise ValueError(f"Polygon field '{POLY_FLD}' not found in polygons (columns: {list(polys.columns)})")

    print("Assigning tiles to flights (largest overlap)...")
    assigned = assign_tiles_to_flights(grid, polys, POLY_FLD)
    print(f"Assigned tiles: {len(assigned)}")

    print("Writing per-flight GeoPackages (one .gpkg per flight)...")
    write_per_flight_individual_gpkg(assigned, FLIGHT_PATH, POLY_FLD)

    print("Done.")

if __name__ == "__main__":
    main()