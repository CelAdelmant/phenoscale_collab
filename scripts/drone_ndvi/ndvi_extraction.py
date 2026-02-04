#!/usr/bin/env python3
"""
Batch zonal-stats runner for multiple flights.

Structure expected:
<root_data>/F1/
  F1_24_03_28/NDVI.data.tif
  F1_24_04_01/NDVI.data.tif
<root_data>/F2/
  F2_24_03_30/NDVI.data.tif
...

GeoPackages expected in a directory:
<gpkg_dir>/flight_1.gpkg
<gpkg_dir>/flight_2.gpkg
...
"""

from pathlib import Path
import re
import rasterio
import fiona
import geopandas as gpd
import pandas as pd
from rasterstats import zonal_stats

# ------------- USER CONFIG -------------
# Root folder containing F1, F2, ... subfolders
ROOT_DATA = Path(r"C:/Users/reub0539/test_data")

# Folder containing flight N geopackages named like flight_1.gpkg
GPKG_DIR = Path(r"C:/Users/reub0539/Desktop/phenoscale_collab/data/drone_data/flight_areas")

# Regex to find flight folders (e.g., F1, F2) and to extract date token from subfolder name
FLIGHT_FOLDER_RE = re.compile(r"^F(\d+)$", re.IGNORECASE)
DATE_TOKEN_RE = re.compile(r"F\d+_(\d{2}_\d{2}_\d{2})")  # captures "24_03_28" from "F1_24_03_28"

# Zonal stats options
STATS = ["mean", "std", "count"]
ALL_TOUCHED = False
NODATA = None  # set to -9999 if your rasters use that nodata

# Output CSV filename template (saved into each flight folder)
OUT_FILENAME_TMPL = "zonal_stats_timeseries_flight{flight_num}.csv"

# Whether to drop count columns from final CSV
DROP_COUNTS = True
# ----------------------------------------

def list_flight_folders(root):
    """Return mapping flight_num (int) -> Path(folder) for folders like F1, F2, ..."""
    out = {}
    for child in sorted(root.iterdir()):
        if child.is_dir():
            m = FLIGHT_FOLDER_RE.match(child.name)
            if m:
                n = int(m.group(1))
                out[n] = child
    return out


def find_tifs_in_flight_folder(flight_folder):
    """Find NDVI.data.tif inside child date folders (e.g. F1_24_03_28/NDVI.data.tif)"""
    tifs = []
    for child in sorted(flight_folder.iterdir()):
        if child.is_dir():
            candidate = child / "NDVI.data.tif"
            if candidate.exists():
                tifs.append(candidate)
    return tifs


def read_gpkg_for_flight(gpkg_dir, flight_num):
    """Return GeoDataFrame for flight N, or None if missing / invalid."""
    gpkg_path = gpkg_dir / f"flight_{flight_num}.gpkg"
    if not gpkg_path.exists():
        # try alternative name patterns
        alt = gpkg_dir / f"flight_{flight_num:02d}.gpkg"
        if alt.exists():
            gpkg_path = alt
        else:
            return None, None

    # list layers and pick first (common for single-layer GPKG)
    try:
        layers = fiona.listlayers(str(gpkg_path))
    except Exception as e:
        print(f"[WARN] Could not list layers in {gpkg_path}: {e}")
        return None, None

    layer = layers[0] if layers else None
    try:
        if layer:
            gdf = gpd.read_file(str(gpkg_path), layer=layer)
        else:
            gdf = gpd.read_file(str(gpkg_path))
    except Exception as e:
        print(f"[WARN] Could not read GPKG {gpkg_path}: {e}")
        return None, None

    # ensure there's an 'id' column (create from index if needed)
    if "id" not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={"index": "id"})

    return gdf, gpkg_path


def process_flight(flight_num, flight_folder, gdf, gpkg_path):
    """Run zonal stats for all NDVI.tif files under flight_folder and save CSV there."""
    tifs = find_tifs_in_flight_folder(flight_folder)
    if not tifs:
        print(f"[SKIP] No NDVI.tif files found in {flight_folder}")
        return

    # Ensure gdf CRS matches rasters later. We'll use CRS of the first raster found.
    first_tif = tifs[0]
    try:
        with rasterio.open(first_tif) as src:
            raster_crs = src.crs
    except Exception as e:
        print(f"[ERROR] Can't open raster {first_tif}: {e}")
        return

    if gdf.crs != raster_crs:
        print(f"Reprojecting vector (flight {flight_num}) from {gdf.crs} -> {raster_crs}")
        gdf_proc = gdf.to_crs(raster_crs)
    else:
        gdf_proc = gdf.copy()

    # Keep only id & geometry to avoid carrying many attributes
    gdf_proc = gdf_proc[["id", gdf_proc.geometry.name]].rename(columns={gdf_proc.geometry.name: "geometry"})
    gdf_proc.set_geometry("geometry", inplace=True)

    # Prepare results DataFrame with id column
    results_df = pd.DataFrame({"id": gdf_proc["id"].values})

    processed_dates = []

    # Sort tifs by parent folder name to keep chronological-ish order
    for tif in sorted(tifs, key=lambda p: p.parent.name):
        parent = tif.parent.name
        m = DATE_TOKEN_RE.search(parent)
        if m:
            date_tok = m.group(1)
        else:
            # fallback: sanitize parent name
            date_tok = re.sub(r"\W+", "_", parent)

        col_mean = f"mean_{date_tok}"
        col_std = f"std_{date_tok}"
        col_count = f"count_{date_tok}"

        print(f"[Flight {flight_num}] Processing {tif.name} -> {col_mean}, {col_std}")

        try:
            zs = zonal_stats(
                gdf_proc,
                str(tif),
                stats=STATS,
                nodata=NODATA,
                all_touched=ALL_TOUCHED,
                geojson_out=False,
            )
        except Exception as e:
            print(f"[WARN] zonal_stats failed for {tif}: {e}")
            # fill with NaNs for this date
            empty_df = pd.DataFrame([{k: None for k in STATS}] * len(gdf_proc))
            stat_df = empty_df.rename(columns={"mean": col_mean, "std": col_std, "count": col_count})
            results_df = pd.concat([results_df.reset_index(drop=True), stat_df.reset_index(drop=True)], axis=1)
            processed_dates.append(date_tok)
            continue

        stat_df = pd.DataFrame(zs)[STATS].rename(columns={"mean": col_mean, "std": col_std, "count": col_count})
        results_df = pd.concat([results_df.reset_index(drop=True), stat_df.reset_index(drop=True)], axis=1)
        processed_dates.append(date_tok)

    # Optionally drop count columns
    if DROP_COUNTS:
        for tok in processed_dates:
            col = f"count_{tok}"
            if col in results_df.columns:
                results_df.drop(columns=[col], inplace=True)

    # Order columns: id, then mean/std pairs in processed_dates order
    cols = ["id"]
    for tok in processed_dates:
        cols.append(f"mean_{tok}")
        cols.append(f"std_{tok}")
    cols = [c for c in cols if c in results_df.columns]
    results_df = results_df[cols]

    # Save CSV into the flight folder (not the date subfolders)
    out_csv = flight_folder / OUT_FILENAME_TMPL.format(flight_num=flight_num)
    results_df.to_csv(out_csv, index=False)
    print(f"[OK] Saved CSV for flight {flight_num} -> {out_csv}")


def main():
    flight_folders = list_flight_folders(ROOT_DATA)
    if not flight_folders:
        print(f"No flight folders found under {ROOT_DATA} (expecting F1, F2, ...).")
        return

    # For each flight folder discovered, try to find matching GPKG and process
    for flight_num, folder in flight_folders.items():
        print("=" * 60)
        print(f"Flight {flight_num}: folder {folder}")
        gdf, gpkg_path = read_gpkg_for_flight(GPKG_DIR, flight_num)
        if gdf is None:
            print(f"[SKIP] No GPKG found for flight {flight_num} in {GPKG_DIR} (expected flight_{flight_num}.gpkg).")
            continue
        print(f"  Found GPKG: {gpkg_path} (rows: {len(gdf)}, crs: {gdf.crs})")
        process_flight(flight_num, folder, gdf, gpkg_path)

    print("All done.")


if __name__ == "__main__":
    main()
