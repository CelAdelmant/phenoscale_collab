""" This script is used to extract mean NDVI values for each tile at each flight date within the species classifier model across wytham woods for the years 2023-2026. 
Python environment needed "geo"
Created by: Stephanie Koolen
Last modified: 29/01/2026

Processing steps:
- 
"""
#!/usr/bin/env python3

from pathlib import Path
import csv
from collections import OrderedDict

import geopandas as gpd
from rtree import index
from pyproj import CRS
from rasterstats import zonal_stats

# ------------------ SETTINGS ------------------
SCRIPT_DIR = Path(__file__).resolve().parent
# auto-detect project root (nearest ancestor containing 'data') or fallback
PROJECT_ROOT = next((p for p in SCRIPT_DIR.parents if (p / "data").exists()), SCRIPT_DIR.parents[2])

# Access data
DATA_DIR = PROJECT_ROOT / "data" / "drone_data"

FLIGHT_PATH = DATA_DIR / "flight_areas" 
TIFF_PATH = "C:\Users\reub0539\Drone_data\Stitches 2024"


#output stored
NDVI_PATH = DATA_DIR  


POLY_FLD = "FlightID"
# ----------------------------------------------