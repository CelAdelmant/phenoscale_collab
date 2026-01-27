# Voronoi-per-year maps (Thiessen polygons) ----------------------------------
library(sf)
library(dplyr)
library(ggplot2)
library(stringr)
library(units)

# ---- Ensure data types and CRS ------------------------------------------------
# Make sure X and Y are numeric and that breeding.inds.territories has Box and Year
breeding.inds.territories <- breeding.inds.territories %>%
  mutate(X = as.numeric(X),
         Y = as.numeric(Y),
         Box = as.character(Box),
         Year = as.integer(Year))

# Ensure wood outline is valid and in same CRS (assume 27700 as your code does)
st_agr(wood.outline) <- "constant"
if (st_crs(wood.outline)$epsg != 27700) {
  wood.outline <- st_transform(wood.outline, crs = 27700)
}
if (exists("wood.outline.linestring")) {
  if (st_crs(wood.outline.linestring)$epsg != 27700) {
    wood.outline.linestring <- st_transform(wood.outline.linestring, crs = 27700)
  }
}

# Output directories
dir.create("maps", showWarnings = FALSE)
dir.create("output", showWarnings = FALSE)

# Helper: create a slightly larger bbox polygon from points (avoid infinite Voronoi cells)
make_expanded_bbox <- function(points_sf, expand_m = 500) {
  bb <- st_bbox(points_sf)
  # expand bbox by expand_m meters in each direction
  bb2 <- bb + c(-expand_m, -expand_m, expand_m, expand_m)
  st_as_sfc(bb2)
}

# Prepare container for results
voronoi_allyears <- list()

# Unique years with occupied boxes (distinct Box per year)
years <- breeding.inds.territories %>% 
  distinct(Year) %>% 
  arrange(Year) %>% 
  pull(Year)

for (yr in years) {
  message("Processing year: ", yr)
  
  # keep distinct boxes that were occupied that year
  pts_df <- breeding.inds.territories %>%
    filter(Year == yr) %>%
    distinct(Box, .keep_all = TRUE) %>%
    filter(!is.na(X) & !is.na(Y))
  
  if (nrow(pts_df) < 1) {
    warning("No points for year ", yr, " — skipping.")
    next
  }
  
  # Make sf points
  pts_sf <- st_as_sf(pts_df, coords = c("X", "Y"), crs = 27700, remove = FALSE)
  
  # Construct bounding polygon (expanded bbox) to avoid infinite cells
  bbox_poly <- make_expanded_bbox(pts_sf, expand_m = 800) # tweak expansion if needed
  # Optionally ensure bbox covers wood outline too:
  bbox_poly <- st_union(bbox_poly, st_buffer(st_union(wood.outline), 1000))
  
  # Compute Voronoi tessellation
  vor <- st_voronoi(st_union(pts_sf), envelope = bbox_poly)
  vor_polys <- st_collection_extract(vor, "POLYGON")
  vor_sf <- st_as_sf(st_cast(vor_polys)) # polygon geometries
  
  # Join polygons to original points by spatial join (centroid-based join to be safe)
  vor_sf <- vor_sf %>%
    mutate(tmp_id = row_number())
  # compute centroid and join to pts by nearest
  pts_cent <- st_centroid(pts_sf)
  # use st_join with st_nearest_feature
  nearest_idx <- st_nearest_feature(pts_cent, vor_sf)
  vor_sf$Box <- pts_sf$Box[nearest_idx]
  
  # now clip to wood outline
  vor_clipped <- st_intersection(vor_sf, st_union(wood.outline))
  
  # attach attributes from pts (e.g. Year, Box attributes)
  vor_clipped <- vor_clipped %>%
    left_join(pts_df %>% select(Box, Year, everything()), by = "Box")
  
  # compute area in hectares
  vor_clipped <- vor_clipped %>%
    mutate(Territory.size_m2 = as.numeric(st_area(x)),
           Territory.size_ha = Territory.size_m2 / 10000,
           Year = yr)
  
  # optionally cap territory size (as in Wilkin et al., cap 2 ha)
  vor_clipped <- vor_clipped %>%
    mutate(Territory.size_2capped_ha = pmin(Territory.size_ha, 2))
  
  voronoi_allyears[[as.character(yr)]] <- vor_clipped
  
  # ---- Plot and save PNG for this year ---------------------------------------
  p <- ggplot() +
    geom_sf(data = wood.outline, fill = NA, color = "black", size = 0.4) +
    geom_sf(data = vor_clipped, aes(fill = Territory.size_ha), color = "grey30", alpha = 0.8) +
    geom_sf(data = pts_sf, size = 1.5, shape = 21, fill = "white", color = "black") +
    scale_fill_viridis_c(option = "magma", name = "Territory (ha)", na.value = "grey80") +
    labs(title = paste0("Voronoi territories — Year: ", yr),
         subtitle = paste0("Occupied boxes: ", nrow(pts_sf)),
         caption = "Polygons clipped to wood outline") +
    theme_minimal() +
    theme(legend.position = "right")
  
  ggsave(filename = file.path("maps", paste0("voronoi_", yr, ".png")), plot = p,
         width = 8, height = 6, dpi = 300)
  
  # Optionally also save per-year GeoPackage layer appended to a single file
  gpkg_path <- file.path("output", "voronoi_by_year.gpkg")
  st_write(vor_clipped, gpkg_path, layer = paste0("voronoi_", yr), delete_layer = TRUE, quiet = TRUE)
}

# ---- Combine all years into one sf object (if desired) ------------------------
voronoi_combined <- do.call(rbind, voronoi_allyears)
# Save combined if you want
st_write(voronoi_combined, file.path("output", "voronoi_all_years.gpkg"), delete_dsn = TRUE, quiet = TRUE)

library(sf)
library(dplyr)
library(ggplot2)

vor_2000 <- voronoi_combined %>%
  filter(Year == 2022)

ggplot() +
  geom_sf(data = wood.outline, fill = NA, color = "black", size = 0.4) +
  geom_sf(data = vor_2000,
          aes(),
          color = "grey40", alpha = 0.85) +
  scale_fill_viridis_c(name = "Territory (ha)", option = "magma") +
  labs(title = "Voronoi (Thiessen) territories — Year 2000") +
  theme_minimal()

