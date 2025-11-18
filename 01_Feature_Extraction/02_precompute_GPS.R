#!/usr/bin/env Rscript
# 02_precompute_GPS.R
# Pre-computes GPS + land-use features for each window.

library(DBI)
library(dplyr)
library(lubridate)
library(sf)
library(tibble)
library(purrr)
library(readr)
library(osmdata)

# -------------------------------------------------
# Settings
setwd("/home/clemensschwarzmann/music-situation-prediction/01_Feature_Extraction")

# --- Load input data
music_windows <- read_csv("data/results/music_windows_all.csv",
                          col_types = cols(
                            user_id = col_character(),
                            start_time = col_datetime(),
                            end_time   = col_datetime()
                          ))

home_locations <- read.csv("data/helper/gps_home.csv")
work_locations <- read.csv("data/helper/gps_work.csv")

# -------------------------------------------------
# Helper functions

is_near_location <- function(user_lon, user_lat, ref_lon, ref_lat, tolerance_m = 15) {
  if (any(is.na(c(user_lon, user_lat, ref_lon, ref_lat)))) return(FALSE)
  u <- st_sfc(st_point(c(user_lon, user_lat)), crs = 4326) %>% st_transform(3857)
  r <- st_sfc(st_point(c(ref_lon, ref_lat)),  crs = 4326) %>% st_transform(3857)
  as.numeric(st_distance(u, r)) <= tolerance_m
}

# cache for repeated land-use lookups
.landuse_cache <- new.env(parent = emptyenv())

get_landuse_type_cached <- function(lat, lon, buffer_m = 60) {
  if (any(is.na(c(lat, lon)))) return("Unknown")
  key <- paste0(round(lat,6), "_", round(lon,6))
  if (exists(key, envir = .landuse_cache, inherits = FALSE))
    return(get(key, envir = .landuse_cache))
  
  old_s2 <- sf::sf_use_s2(FALSE)
  on.exit(sf::sf_use_s2(old_s2), add = TRUE)
  
  pt <- st_sfc(st_point(c(lon, lat)), crs = 4326)
  buffer <- pt %>% st_transform(3857) %>% st_buffer(buffer_m) %>% st_transform(4326)
  
  bbox <- st_bbox(buffer)
  query <- opq(bbox = bbox) %>% add_osm_feature(key = "!null")
  osm <- tryCatch(osmdata_sf(query), error = function(e) NULL)
  
  if (is.null(osm)) { assign(key,"query_failed",.landuse_cache); return("query_failed") }
  
  all_features <- list(osm$osm_points, osm$osm_lines, osm$osm_polygons,
                       osm$osm_multilines, osm$osm_multipolygons) %>%
    keep(~ !is.null(.) && nrow(.) > 0) %>% bind_rows()
  
  if (nrow(all_features) == 0) { assign(key,"no_features",.landuse_cache); return("no_features") }
  
  pt_tr <- st_transform(pt, st_crs(all_features))
  contains <- st_contains(all_features, pt_tr, sparse = FALSE)[,1]
  feats <- all_features[contains, ]
  if (nrow(feats) == 0) { assign(key,"outside",.landuse_cache); return("outside") }
  
  feats <- feats %>%
    mutate(area = ifelse(st_geometry_type(.) %in% c("POLYGON","MULTIPOLYGON"),
                         as.numeric(st_area(st_transform(.,3857))), NA_real_)) %>%
    arrange(area)
  
  priority <- c("amenity","shop","building","landuse","highway","natural",
                "leisure","place","tourism","man_made","office")
  
  for (i in seq_len(nrow(feats))) {
    tags <- feats[i,] %>% st_drop_geometry() %>% select(where(~ any(!is.na(.) & .!="")))
    for (k in priority) {
      if (!is.null(tags[[k]]) && !is.na(tags[[k]]) && tags[[k]] != "") {
        val <- paste0(k, ":", tags[[k]])
        assign(key, val, .landuse_cache)
        return(val)
      }
    }
  }
  
  assign(key,"unclassified",.landuse_cache)
  "unclassified"
}

# -------------------------------------------------
# Compute GPS + land-use per window
results <- vector("list", nrow(music_windows))

for (i in seq_len(nrow(music_windows))) {
  w <- music_windows[i,]
  uid <- w$user_id; s <- w$start_time; e <- w$end_time
  
  # fetch first GPS point in this window (adapt query to your DB)
  q <- sprintf(
    "SELECT latitude, longitude FROM ps_location
     WHERE user_id='%s' AND timestamp >= '%s' AND timestamp <= '%s'
     ORDER BY timestamp ASC LIMIT 1",
    uid, format(s,"%Y-%m-%d %H:%M:%S"), format(e,"%Y-%m-%d %H:%M:%S")
  )
  gps_pt <- tryCatch(dbGetQuery(phonestudy,q), error=function(e) tibble())
  
  if (nrow(gps_pt) == 0) {
    results[[i]] <- tibble(user_id=uid, start_time=s, end_time=e,
                           gps_count=0, gps_first_latitude=NA_real_,
                           gps_first_longitude=NA_real_, gps_landuse_type="Unknown",
                           gps_home=0L, gps_work=0L)
    next
  }
  
  lat <- gps_pt$latitude[1]; lon <- gps_pt$longitude[1]
  landuse <- get_landuse_type_cached(lat, lon)
  
  h <- home_locations %>% filter(user_id==uid)
  wloc <- work_locations %>% filter(user_id==uid)
  
  home_flag <- if (nrow(h)>=1) as.integer(is_near_location(lon,lat,h$longitude[1],h$latitude[1])) else 0L
  work_flag <- if (nrow(wloc)>=1) as.integer(is_near_location(lon,lat,wloc$longitude[1],wloc$latitude[1])) else 0L
  
  results[[i]] <- tibble(user_id=uid, start_time=s, end_time=e,
                         gps_count=1L,
                         gps_first_latitude=lat,
                         gps_first_longitude=lon,
                         gps_landuse_type=landuse,
                         gps_home=home_flag,
                         gps_work=work_flag)
  
  if (i %% 50 == 0) message("Processed ", i, " / ", nrow(music_windows))
}

gps_landuse_df <- bind_rows(results)

# -------------------------------------------------
# Save results
write_csv(gps_landuse_df, "data/results/gps_landuse_by_window.csv")
saveRDS(gps_landuse_df,  "data/results/gps_landuse_by_window.rds")

message("Finished computing GPS + land-use table.")
