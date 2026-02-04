#!/usr/bin/env python3
"""
Parallel batch zonal-stats runner.
Each flight (F1, F2, ...) is processed in its own worker process.
Outputs one CSV per flight folder.
"""

from pathlib import Path
import re
import os
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

import rasterio
import fiona
import geopandas as gpd
import pandas as pd
from rasterstats import zonal_stats

# ---------- USER CONFIG ----------
ROOT_DATA = Path(r"C:/Users/reub0539/Drone_data/Stitches 2024")  # contains F1, F2, ...
GPKG_DIR = Path(r"C:/Users/reub0539/OneDrive - Nexus365/Desktop/phenoscale_collab/data/drone_data/flight_areas")

FLIGHT_FOLDER_RE = re.compile(r"^F(\d+)$", re.IGNORECASE)
DATE_TOKEN_RE = re.compile(r"F\d+_(\d{2}_\d{2}_\d{2})")

STATS = ["mean", "std", "count"]
ALL_TOUCHED = False
NODATA = None

OUT_FILENAME_TMPL = "zonal_stats_timeseries_flight{flight_num}.csv"
DROP_COUNTS = True

# Number of worker processes to use. If None, automatically choose min(cpu_count()-1, n_flights)
MAX_WORKERS = None
# ----------------------------------


def list_flight_folders(root):
    out = {}
    for child in sorted(root.iterdir()):
        if child.is_dir():
            m = FLIGHT_FOLDER_RE.match(child.name)
            if m:
                out[int(m.group(1))] = child
    return out


def find_tifs_in_flight_folder(flight_folder):
    tifs = []
    for child in sorted(flight_folder.iterdir()):
        if child.is_dir():
            candidate = child / "NDVI.data.tif"
            if candidate.exists():
                tifs.append(candidate)
    return tifs


def read_gpkg_for_flight(gpkg_dir, flight_num):
    gpkg_path = gpkg_dir / f"flight_{flight_num}.gpkg"
    if not gpkg_path.exists():
        alt = gpkg_dir / f"flight_{flight_num:02d}.gpkg"
        if alt.exists():
            gpkg_path = alt
        else:
            return None, None

    try:
        layers = fiona.listlayers(str(gpkg_path))
    except Exception:
        layers = []

    layer = layers[0] if layers else None
    try:
        if layer:
            gdf = gpd.read_file(str(gpkg_path), layer=layer)
        else:
            gdf = gpd.read_file(str(gpkg_path))
    except Exception:
        return None, None

    if "id" not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={"index": "id"})
    return gdf, gpkg_path


def process_flight_worker(flight_num, flight_folder_str, gpkg_dir_str):
    """
    Worker function executed in a separate process.
    Returns dict with keys: flight_num, status, message, out_csv (or None), n_dates_processed
    """
    flight_folder = Path(flight_folder_str)
    gpkg_dir = Path(gpkg_dir_str)
    try:
        # Read gpkg inside worker to avoid pickling
        gdf, gpkg_path = read_gpkg_for_flight(gpkg_dir, flight_num)
        if gdf is None:
            return {"flight_num": flight_num, "status": "skip", "message": f"No GPKG for flight {flight_num}", "out_csv": None, "n_dates": 0}

        tifs = find_tifs_in_flight_folder(flight_folder)
        if not tifs:
            return {"flight_num": flight_num, "status": "skip", "message": f"No NDVI.tif in {flight_folder}", "out_csv": None, "n_dates": 0}

        # Use CRS of first raster and reproject vector if needed
        with rasterio.open(tifs[0]) as src:
            raster_crs = src.crs

        if gdf.crs != raster_crs:
            gdf_proc = gdf.to_crs(raster_crs)
        else:
            gdf_proc = gdf.copy()

        # Keep only 'id' and geometry
        geometry_col = gdf_proc.geometry.name
        gdf_proc = gdf_proc[["id", geometry_col]].rename(columns={geometry_col: "geometry"})
        gdf_proc.set_geometry("geometry", inplace=True)

        results_df = pd.DataFrame({"id": gdf_proc["id"].values})
        processed_dates = []

        for tif in sorted(tifs, key=lambda p: p.parent.name):
            parent = tif.parent.name
            m = DATE_TOKEN_RE.search(parent)
            if m:
                date_tok = m.group(1)
            else:
                date_tok = re.sub(r"\W+", "_", parent)

            col_mean = f"mean_{date_tok}"
            col_std = f"std_{date_tok}"
            col_count = f"count_{date_tok}"

            # compute zonal stats
            zs = zonal_stats(
                gdf_proc,
                str(tif),
                stats=STATS,
                nodata=NODATA,
                all_touched=ALL_TOUCHED,
                geojson_out=False,
            )
            stat_df = pd.DataFrame(zs)[STATS].rename(columns={"mean": col_mean, "std": col_std, "count": col_count})
            results_df = pd.concat([results_df.reset_index(drop=True), stat_df.reset_index(drop=True)], axis=1)
            processed_dates.append(date_tok)

        if DROP_COUNTS:
            for tok in processed_dates:
                c = f"count_{tok}"
                if c in results_df.columns:
                    results_df.drop(columns=[c], inplace=True)

        cols = ["id"]
        for tok in processed_dates:
            cols.append(f"mean_{tok}")
            cols.append(f"std_{tok}")
        cols = [c for c in cols if c in results_df.columns]
        results_df = results_df[cols]

        out_csv = flight_folder / OUT_FILENAME_TMPL.format(flight_num=flight_num)
        results_df.to_csv(out_csv, index=False)

        return {"flight_num": flight_num, "status": "ok", "message": f"Saved {out_csv}", "out_csv": str(out_csv), "n_dates": len(processed_dates)}

    except Exception as e:
        tb = traceback.format_exc()
        return {"flight_num": flight_num, "status": "error", "message": str(e), "traceback": tb, "out_csv": None, "n_dates": 0}


def main():
    flight_folders = list_flight_folders(ROOT_DATA)
    if not flight_folders:
        print(f"No flight folders found under {ROOT_DATA}.")
        return

    # Build list of tasks
    tasks = []
    for flight_num, folder in flight_folders.items():
        tasks.append((flight_num, str(folder), str(GPKG_DIR)))

    n_tasks = len(tasks)
    cpu_count = os.cpu_count() or 1
    default_workers = max(1, cpu_count - 1)
    workers = MAX_WORKERS if MAX_WORKERS is not None else min(default_workers, n_tasks)

    print(f"Discovered {n_tasks} flights. Running up to {workers} workers in parallel...")

    results = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        future_to_task = {ex.submit(process_flight_worker, t[0], t[1], t[2]): t for t in tasks}
        for fut in as_completed(future_to_task):
            task = future_to_task[fut]
            try:
                res = fut.result()
            except Exception as e:
                print(f"[ERROR] Flight {task[0]} crashed with exception: {e}")
                res = {"flight_num": task[0], "status": "error", "message": str(e), "traceback": None}
            results.append(res)
            # print a short summary
            if res.get("status") == "ok":
                print(f"[OK] Flight {res['flight_num']}: {res['message']} (dates={res['n_dates']})")
            elif res.get("status") == "skip":
                print(f"[SKIP] Flight {res['flight_num']}: {res['message']}")
            else:
                print(f"[FAIL] Flight {res.get('flight_num')}: {res.get('message')}")
                if res.get("traceback"):
                    print(res["traceback"])

    print("\nSummary:")
    for r in sorted(results, key=lambda x: x.get("flight_num", 0)):
        print(f" Flight {r.get('flight_num')}: {r.get('status')} - {r.get('message')}")

    print("Done.")


if __name__ == "__main__":
    main()
