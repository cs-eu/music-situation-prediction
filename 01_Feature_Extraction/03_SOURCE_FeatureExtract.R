# --- 1. Load Libraries ---
library(DBI)
library(data.table)
library(lubridate)
library(jsonlite)
library(future.apply)

# --- 2. Configuration & Setup ---
setwd("/home/clemensschwarzmann/MusicInSituations/01_Feature_Extraction")
source("utils/label_usage_sessions.R") # Assumes preprocessing_screen_window is here

# Load DB credentials
invisible(capture.output(source('/local/.meta/dbcredentials.R')))

# --- 3. Database Connection ---
con <- dbConnect(
  drv = RMariaDB::MariaDB(),
  username = mariadb_user,
  password = mariadb_pw,
  host = "localhost",
  port = 3306,
  dbname = "live"
)
on.exit(dbDisconnect(con), add = TRUE)

# --- 4. Load & Pre-process Data ---
message("Loading and pre-processing data...")

# Load raw data
music_raw <- DBI::dbReadTable(con, "ps_music")
snapshot_raw <- dbGetQuery(con, "SELECT id, headsetState, detectedActivities FROM ps_snapshot")
activity_data_raw <- readRDS("data/helper/combined_activity_data.rds")
music_windows <- fread("data/results/music_windows_all.csv")

# Load helper data
app_categorization <- fread("data/helper/app_categorisation_2020_v2.csv")
gps_landuse_df <- readRDS("data/results/gps_landuse_by_window.rds")
spotify_data <- readRDS("data/helper/distinct_tracks_enriched.rds")
genius_features <- fread("data/helper/lyrics-features/genius_features.csv")
topic_features <- fread("data/helper/lyrics-features/topic_features.csv")
liwc_features <- fread("data/helper/lyrics-features/Lyrics_LIWC.csv")

# --- 5. Convert to data.table & Set Keys for Performance ---
setDT(music_raw)
setDT(snapshot_raw)
setDT(activity_data_raw)
setDT(music_windows)
setDT(app_categorization)
setDT(gps_landuse_df)
setDT(spotify_data)
setDT(genius_features)
setDT(topic_features)
setDT(liwc_features)

# Set keys for fast filtering and joining
setkey(music_raw, id)
setkey(snapshot_raw, id)
setkey(activity_data_raw, user_id, timestamp)
setkey(gps_landuse_df, user_id, start_time, end_time)
setkey(app_categorization, App_name)
setkey(spotify_data, track, artist)
genius_features[, track_spotify_id := sub("lyrics:", "", lyricsID)]
setkey(genius_features, track_spotify_id)
topic_features[, track_spotify_id := sub("lyrics:", "", lyricsID)]
setkey(topic_features, track_spotify_id)
liwc_features[, track_spotify_id := sub("lyrics:", "", key)]
setkey(liwc_features, track_spotify_id)

# Clean up app categorization column names
setnames(app_categorization, "App_name", "packageName")
app_categorization[is.na(Final_Rating), Final_Rating := "Unknown"]

# # --- 6. Pre-process JSON Activities (Major Optimization) ---
# message("Pre-processing JSON activity data...")
# snapshot_activities <- snapshot_raw[!is.na(detectedActivities) & detectedActivities != "[]", .(id, detectedActivities)]
# snapshot_activities[, parsed := lapply(detectedActivities, function(j) tryCatch(as.data.table(fromJSON(j)), error = function(e) NULL))]
# snapshot_activities <- snapshot_activities[!sapply(parsed, is.null)]
# snapshot_activities <- snapshot_activities[, unlist(parsed, recursive = FALSE), by = id]
# setkey(snapshot_activities, id)

# --- 6. Pre-process JSON Activities (The Final, Robust Method) ---
message("Pre-processing JSON activity data with RcppSimdJson and rbindlist...")
library(RcppSimdJson)
library(data.table)

# Filter out rows with no activity data first
snapshot_filtered <- snapshot_raw[!is.na(detectedActivities) & detectedActivities != "[]", .(id, detectedActivities)]

# fparse returns a LIST of parsed objects
parsed_list <- fparse(snapshot_filtered$detectedActivities)

# Name each element in the list with its original database 'id'
# This is the key step to link the data back together.
names(parsed_list) <- snapshot_filtered$id

# rbindlist stacks the list into a single data.table.
# - 'idcol = "id"' creates a new 'id' column using the names we just set.
# - 'fill = TRUE' handles cases where some JSONs have activities others don't.
snapshot_activities <- rbindlist(parsed_list, idcol = "id", fill = TRUE)

# The 'id' column from rbindlist might be character, so ensure it's the correct type
snapshot_activities[, id := as.integer(id)]

# Replace any NAs created during the bind with 0
# This uses a fast loop within data.table to target only numeric columns
for (col in names(snapshot_activities)) {
  if (is.numeric(snapshot_activities[[col]])) {
    set(snapshot_activities, which(is.na(snapshot_activities[[col]])), col, 0)
  }
}

# Set the key for fast joins later
setkey(snapshot_activities, id)
setkey(snapshot_raw, id)

# --- 7. Define Constants and Column Names ---
# Define feature sets to ensure consistent column output
app_categories <- sort(unique(app_categorization$Final_Rating))
headset_states <- c("PLUGGED", "UNPLUGGED")
screen_categories <- c("ON_UNLOCKED", "OFF_UNLOCKED", "OFF_LOCKED", "ON_LOCKED", "UNKNOWN")
detected_activity_keys <- c("STILL", "WALKING", "RUNNING", "ON_BICYCLE", "ON_FOOT", "IN_VEHICLE", "IN_ROAD_VEHICLE", "IN_RAIL_VEHICLE", "IN_FOUR_WHEELER_VEHICLE")
all_activity_names <- c("AIRPLANE", "APPS", "BATTERYSAVINGMODE", "BLUETOOTH", "CAMERA", "CALENDAR", "GPS", "NOTIFICATION", "PHONE", "POWER", "SCREEN", "SMS")
gps_features <- c("gps_count",  "gps_first_latitude",  "gps_first_longitude",  "gps_landuse_type",  "gps_home",  "gps_work")

spotify_numeric_features <- c("track_danceability", "track_energy", "track_key", "track_loudness", "track_mode_major", "track_speechiness", "track_acousticness", "track_instrumentalness", "track_liveness", "track_valence", "track_tempo", "track_non_music")
genius_numeric_features <- c("fear", "anger", "trust", "surprise", "positive", "negative", "sadness", "disgust", "joy", "anticipation", "lyric_len")
topic_numeric_features <- paste0("Topic ", 0:29)
liwc_numeric_features <- c("WC", "Analytic", "Clout", "Authentic", "Tone", "WPS", "Sixltr", "Dic", "function.", "pronoun", "ppron", "i", "we", "you_total", "you_sing", "you_plur", "you_formal", "other", "shehe", "they", "ipron", "article", "prep", "auxverb", "adverb", "conj", "negate", "verb", "adj", "compare", "interrog", "number", "quant", "affect", "posemo", "negemo", "anx", "anger", "sad", "social", "family", "friend", "female", "male", "cogproc", "insight", "cause", "discrep", "tentat", "certain", "differ", "percept", "see", "hear", "feel", "bio", "body", "health", "sexual", "ingest", "drives", "affiliation", "achiev", "power", "reward", "risk", "focuspast", "focuspresent", "focusfuture", "relativ", "motion", "space", "time", "work", "leisure", "home", "money", "relig", "death", "informal", "swear", "netspeak", "assent", "nonflu", "filler", "AllPunc", "Period", "Comma", "Colon", "SemiC", "QMark", "Exclam", "Dash", "Quote", "Apostro", "Parenth", "OtherP", "Emoji")

# --- 5b. PRE-COMPUTE MASTER MUSIC FEATURES TABLE (MAJOR OPTIMIZATION) ---
message("Pre-joining all music feature tables into a master table...")

# Join all lyrics features together on their common key
lyrics_features_all <- genius_features[topic_features, on = "track_spotify_id"][liwc_features, on = "track_spotify_id"]

# Now, join the complete lyrics data with the Spotify audio feature data
# This creates a single, comprehensive table with all track features.
music_features_master <- spotify_data[lyrics_features_all, on = "track_spotify_id"]

# Pre-calculate the numeric column names ONCE, instead of inside the loop
spotify_numeric_cols <- intersect(spotify_numeric_features, names(music_features_master))
genius_numeric_cols  <- intersect(genius_numeric_features, names(music_features_master))
topic_numeric_cols   <- intersect(topic_numeric_features, names(music_features_master))
liwc_numeric_cols    <- intersect(liwc_numeric_features, names(music_features_master))

# Key the master table for the final, fast join inside the function
setkey(music_features_master, track, artist)

# --- 8. Helper Functions ---
# More efficient time bin function
get_time_bin <- function(start_time) {
  h <- hour(start_time)
  if (h >= 5 && h < 8) return("Morning")
  if (h >= 8 && h < 11) return("Late Morning")
  if (h >= 11 && h < 13) return("Noon")
  if (h >= 13 && h < 17) return("Afternoon")
  if (h >= 17 && h < 21) return("Evening")
  return("Night")
}

# Function to ensure all columns exist in a data.table
ensure_cols <- function(dt, col_names, fill = 0) {
  missing_cols <- setdiff(col_names, names(dt))
  if (length(missing_cols) > 0) {
    dt[, (missing_cols) := fill]
  }
  return(dt)
}

extract_features_for_window <- function(user_id, start_time, end_time) {
  # Start total timer
  t0 <- Sys.time()
  
  # Helper for timing blocks
  time_log <- function(label, start_t) {
    dt <- round(as.numeric(difftime(Sys.time(), start_t, units = "secs")), 3)
    message(sprintf("⏱ %s took %.3f sec", label, dt))
  }
  
  # --- 1. Filter activity data ---
  t1 <- Sys.time()
  acts <- activity_data_raw[user_id == user_id & timestamp %between% c(start_time, end_time)]
  time_log("Activity filtering", t1)
  if (nrow(acts) == 0) return(NULL)
  
  # --- 2. Time Bin ---
  t1 <- Sys.time()
  time_bin <- get_time_bin(start_time)
  time_log("Time bin assignment", t1)
  
  # --- 3. GPS / Landuse ---
  t1 <- Sys.time()
  gps_row <- gps_landuse_df[user_id == row$user_id &
                              start_time <= row$end_time &
                              end_time >= row$start_time]
  if (nrow(gps_row) == 0) {
    gps_features <- data.table(
      gps_count = 0,
      gps_first_latitude = NA_real_,
      gps_first_longitude = NA_real_,
      gps_landuse_type = "Unknown",
      gps_home = 0,
      gps_work = 0
    )
  } else {
    # This part remains the same, but now operates on a single row
    gps_features <- gps_row[, .(
      gps_count,
      gps_first_latitude,
      gps_first_longitude,
      gps_landuse_type,
      gps_home,
      gps_work
    )]
  }
  time_log("GPS features", t1)
  
  # --- 4. Snapshot-based Features ---
  t1 <- Sys.time()
  snapshot_ids <- unique(as.integer(acts$snapshot_id))
  # Use keyed joins instead of %in% for major speedup
  snapshot_info <- snapshot_raw[J(snapshot_ids), nomatch = 0L, on = "id"]
  
  # --- Headset summary ---
  if (nrow(snapshot_info) > 0 && "headsetState" %in% names(snapshot_info)) {
    tmp_headset <- snapshot_info[, .N, by = headsetState]
    headset_summary <- dcast(tmp_headset, . ~ headsetState, value.var = "N", fill = 0)[, . := NULL]
  } else {
    headset_summary <- data.table(matrix(0, nrow = 1, ncol = length(headset_states)))
    setnames(headset_summary, headset_states)
  }
  headset_summary <- ensure_cols(headset_summary, headset_states, fill = 0)
  setnames(headset_summary, names(headset_summary), paste0("headset_", names(headset_summary)))
  
  # --- Detected Activities ---
  if (length(snapshot_ids) > 0) {
    tmp <- snapshot_activities[J(snapshot_ids), .SD, .SDcols = detected_activity_keys, nomatch = 0L]
    if (nrow(tmp) == 0) {
      detected_activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(detected_activity_keys)))
      setnames(detected_activity_summary, detected_activity_keys)
    } else {
      detected_activity_summary <- tmp[, lapply(.SD, sum, na.rm = TRUE)]
    }
  } else {
    detected_activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(detected_activity_keys)))
    setnames(detected_activity_summary, detected_activity_keys)
  }
  
  # Ensure all columns exist
  detected_activity_summary <- ensure_cols(detected_activity_summary, detected_activity_keys, fill = 0)
  setnames(detected_activity_summary, names(detected_activity_summary), paste0("detected_", names(detected_activity_summary)))
  time_log("Snapshot-based features", t1)
  
  # --- 5. Phone Activities ---
  t1 <- Sys.time()
  if (nrow(acts) == 0) {
    activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(all_activity_names)))
    setnames(activity_summary, all_activity_names)
  } else {
    tmp_act <- acts[activityName %in% all_activity_names, .N, by = activityName]
    activity_summary <- dcast(tmp_act, . ~ activityName, value.var = "N", fill = 0)[, . := NULL]
    activity_summary <- ensure_cols(activity_summary, all_activity_names, fill = 0)
  }
  setnames(activity_summary, names(activity_summary), paste0("phone_", names(activity_summary)))
  time_log("Phone activities", t1)
  
  # --- 6. App Usage ---
  t1 <- Sys.time()
  acts_with_pkg <- acts[!is.na(packageName) & packageName != ""]
  if (nrow(acts_with_pkg) == 0) {
    app_usage_summary <- data.table(matrix(0, nrow = 1, ncol = length(app_categories)))
    setnames(app_usage_summary, app_categories)
  } else {
    app_usage <- app_categorization[acts_with_pkg, on = "packageName", nomatch = 0]
    if (nrow(app_usage) == 0) {
      app_usage_summary <- data.table(matrix(0, nrow = 1, ncol = length(app_categories)))
      setnames(app_usage_summary, app_categories)
    } else {
      tmp_app <- app_usage[, .N, by = Final_Rating]
      app_usage_summary <- dcast(tmp_app, . ~ Final_Rating, value.var = "N", fill = 0)[, . := NULL]
      app_usage_summary <- ensure_cols(app_usage_summary, app_categories, fill = 0)
    }
  }
  setnames(app_usage_summary, names(app_usage_summary), paste0("app_", names(app_usage_summary)))
  time_log("App usage", t1)
  
  # --- 7. Screen State ---
  t1 <- Sys.time()
  screen_state_durations <- preprocessing_screen_window(acts, start_time, end_time)
  if (is.null(screen_state_durations) || nrow(screen_state_durations) == 0) {
    screen_state_summary <- data.table(matrix(0, nrow = 1, ncol = length(screen_categories)))
    setnames(screen_state_summary, screen_categories)
  } else {
    setDT(screen_state_durations)
    tmp_screen <- screen_state_durations[, .(duration_sec = sum(duration_sec, na.rm = TRUE)), by = event]
    screen_state_summary <- dcast(tmp_screen, . ~ event, value.var = "duration_sec", fill = 0)[, . := NULL]
    screen_state_summary <- ensure_cols(screen_state_summary, screen_categories, fill = 0)
  }
  setnames(screen_state_summary, names(screen_state_summary), paste0("screen_", names(screen_state_summary)))
  time_log("Screen state", t1)
  
  # --- 8. Music Features (Corrected & Robust) ---
  t1 <- Sys.time()
  music_ids <- acts[activityName == "MUSIC" & !is.na(music_id), as.integer(music_id)]
  if (length(music_ids) == 0) return(NULL)
  
  music_window <- unique(music_raw[J(music_ids), .(title, artist), on = "id", nomatch = 0])
  if (nrow(music_window) == 0) return(NULL)
  
  all_matched_features <- music_features_master[music_window, on = .(track = title, artist = artist), nomatch = 0]
  if (nrow(all_matched_features) == 0) return(NULL)
  
  # --- Calculate means ---
  # These can result in 1-row, 0-column tables if the intersection of columns is empty
  spotify_means <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = spotify_numeric_cols]
  genius_means  <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = genius_numeric_cols]
  topic_means   <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = topic_numeric_cols]
  liwc_means    <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = liwc_numeric_cols]
  
  # --- FIX: Ensure all feature tables have the correct columns, filling with NA ---
  # This replaces the flawed `if (nrow(...) == 0)` logic and handles the 1-row, 0-column case.
  spotify_means <- ensure_cols(spotify_means, spotify_numeric_features, fill = NA_real_)
  genius_means  <- ensure_cols(genius_means,  genius_numeric_features,  fill = NA_real_)
  topic_means   <- ensure_cols(topic_means,   topic_numeric_features,   fill = NA_real_)
  liwc_means    <- ensure_cols(liwc_means,    liwc_numeric_features,    fill = NA_real_)
  
  # --- Add prefixes to column names ---
  # This is now safe because all tables are guaranteed to have the correct columns.
  setnames(spotify_means, paste0("music_", names(spotify_means)))
  setnames(genius_means, paste0("genius_", names(genius_means)))
  setnames(topic_means, paste0("topic_", gsub("\\s+", "_", trimws(names(topic_means)))))
  setnames(liwc_means, paste0("liwc_", names(liwc_means)))
  time_log("Music features", t1)
  
  # --- 9. Combine All Features ---
  t1 <- Sys.time()
  
  feature_tables_to_check <- list(
    gps = gps_features,
    headset = headset_summary,
    detected_activity = detected_activity_summary,
    phone_activity = activity_summary,
    app_usage = app_usage_summary,
    screen_state = screen_state_summary
  )
  
  for (tbl_name in names(feature_tables_to_check)) {
    if (nrow(feature_tables_to_check[[tbl_name]]) != 1) {
      warning(sprintf(
        "⚠️ VALIDATION FAILED: Feature table '%s' for user %s at %s has %d rows (expected 1). Skipping window to prevent NAs.",
        tbl_name, user_id, start_time, nrow(feature_tables_to_check[[tbl_name]])
      ))
    }
  }
  
  time_log("Feature table validation", t1)
  final_features <- cbind(
    data.table(user_id = user_id, start_time = start_time, end_time = end_time, time_bin = time_bin),
    gps_features,
    headset_summary,
    detected_activity_summary,
    activity_summary,
    app_usage_summary,
    screen_state_summary,
    spotify_means,
    genius_means,
    topic_means,
    liwc_means
  )
  time_log("Final combine", t1)

  total_dt <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 3)
  message(sprintf("✅ Window processed for user %s (%.3f sec total)", user_id, total_dt))
  
  return(final_features)
}

# # --- 9. Core Feature Extraction Function ---
# extract_features_for_window <- function(user_id, start_time, end_time) {
#   # --- 1. Filter activity data ---
#   acts <- activity_data_raw[user_id == user_id & timestamp %between% c(start_time, end_time)]
#   if (nrow(acts) == 0) return(NULL)
# 
#   # --- 2. Time Bin ---
#   time_bin <- get_time_bin(start_time)
# 
#   # --- 3. GPS / Landuse ---
#   gps_row <- gps_landuse_df[user_id == user_id & start_time == start_time & end_time == end_time]
#   if (nrow(gps_row) == 0) {
#     gps_features <- data.table(
#       gps_count = 0,
#       gps_first_latitude = NA_real_,
#       gps_first_longitude = NA_real_,
#       gps_landuse_type = "Unknown",
#       gps_home = 0,
#       gps_work = 0
#     )
#   } else {
#     gps_features <- gps_row[, .(
#       gps_count,
#       gps_first_latitude,
#       gps_first_longitude,
#       gps_landuse_type,
#       gps_home,
#       gps_work
#     )]
#   }
# 
#   # --- 4. Snapshot-based Features ---
#   snapshot_ids <- unique(acts$snapshot_id)
#   snapshot_info <- snapshot_raw[id %in% snapshot_ids]
# 
#   # Headset summary
#   if (nrow(snapshot_info) > 0 && "headsetState" %in% names(snapshot_info)) {
#     tmp_headset <- snapshot_info[, .N, by = headsetState]
#     headset_summary <- dcast(tmp_headset, . ~ headsetState, value.var = "N", fill = 0)[, . := NULL]
#   } else {
#     headset_summary <- data.table(matrix(0, nrow = 1, ncol = length(headset_states)))
#     setnames(headset_summary, headset_states)
#   }
#   headset_summary <- ensure_cols(headset_summary, headset_states, fill = 0)
#   setnames(headset_summary, names(headset_summary), paste0("headset_", names(headset_summary)))
# 
#   # Detected Activities
#   if (length(snapshot_ids) > 0) {
#     detected_activity_summary <- snapshot_activities[
#       id %in% snapshot_ids,
#       lapply(.SD, sum, na.rm = TRUE),
#       .SDcols = detected_activity_keys
#     ]
#   } else {
#     detected_activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(detected_activity_keys)))
#     setnames(detected_activity_summary, detected_activity_keys)
#   }
#   detected_activity_summary <- ensure_cols(detected_activity_summary, detected_activity_keys, fill = 0)
#   setnames(detected_activity_summary, names(detected_activity_summary), paste0("detected_", names(detected_activity_summary)))
# 
#   # --- 5. Phone Activities ---
#   if (nrow(acts) == 0) {
#     activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(all_activity_names)))
#     setnames(activity_summary, all_activity_names)
#   } else {
#     tmp_act <- acts[, .N, by = activityName]
#     activity_summary <- dcast(tmp_act, . ~ activityName, value.var = "N", fill = 0)[, . := NULL]
#     activity_summary <- ensure_cols(activity_summary, all_activity_names, fill = 0)
#   }
#   setnames(activity_summary, names(activity_summary), paste0("phone_", names(activity_summary)))
# 
#   # --- 6. App Usage ---
#   acts_with_pkg <- acts[!is.na(packageName) & packageName != ""]
#   if (nrow(acts_with_pkg) == 0) {
#     app_usage_summary <- data.table(matrix(0, nrow = 1, ncol = length(app_categories)))
#     setnames(app_usage_summary, app_categories)
#   } else {
#     app_usage <- app_categorization[acts_with_pkg, on = "packageName", nomatch = 0]
#     if (nrow(app_usage) == 0) {
#       app_usage_summary <- data.table(matrix(0, nrow = 1, ncol = length(app_categories)))
#       setnames(app_usage_summary, app_categories)
#     } else {
#       tmp_app <- app_usage[, .N, by = Final_Rating]
#       app_usage_summary <- dcast(tmp_app, . ~ Final_Rating, value.var = "N", fill = 0)[, . := NULL]
#       app_usage_summary <- ensure_cols(app_usage_summary, app_categories, fill = 0)
#     }
#   }
#   setnames(app_usage_summary, names(app_usage_summary), paste0("app_", names(app_usage_summary)))
# 
#   # --- 7. Screen State ---
#   screen_state_durations <- preprocessing_screen_window(acts, start_time, end_time)
#   if (is.null(screen_state_durations) || nrow(screen_state_durations) == 0) {
#     screen_state_summary <- data.table(matrix(0, nrow = 1, ncol = length(screen_categories)))
#     setnames(screen_state_summary, screen_categories)
#   } else {
#     setDT(screen_state_durations)
#     tmp_screen <- screen_state_durations[, .(duration_sec = sum(duration_sec, na.rm = TRUE)), by = event]
#     screen_state_summary <- dcast(tmp_screen, . ~ event, value.var = "duration_sec", fill = 0)[, . := NULL]
#     screen_state_summary <- ensure_cols(screen_state_summary, screen_categories, fill = 0)
#   }
#   setnames(screen_state_summary, names(screen_state_summary), paste0("screen_", names(screen_state_summary)))
# 
#   # --- 8. Music Features ---
#   music_ids <- acts[activityName == "MUSIC" & !is.na(music_id), music_id]
#   if (length(music_ids) == 0) return(NULL)
# 
#   music_window <- unique(music_raw[id %in% music_ids, .(title, artist)])
#   if (nrow(music_window) == 0) return(NULL)
# 
#   matched_tracks <- spotify_data[music_window, on = .(track = title, artist = artist), nomatch = 0]
#   if (nrow(matched_tracks) == 0) return(NULL)
# 
#   # Spotify audio features
#   spotify_numeric_cols <- names(Filter(is.numeric, matched_tracks))
#   spotify_means <- matched_tracks[, lapply(.SD, mean, na.rm = TRUE), .SDcols = spotify_numeric_cols]
#   setnames(spotify_means, names(spotify_means), paste0("music_", names(spotify_means)))
# 
#   # Lyrics features
#   matched_genius <- genius_features[matched_tracks, on = "track_spotify_id", nomatch = 0]
#   matched_topic <- topic_features[matched_tracks, on = "track_spotify_id", nomatch = 0]
#   matched_liwc <- liwc_features[matched_tracks, on = "track_spotify_id", nomatch = 0]
# 
#   genius_numeric_cols <- names(Filter(is.numeric, matched_genius))
#   topic_numeric_cols <- names(Filter(is.numeric, matched_topic))
#   liwc_numeric_cols <- names(Filter(is.numeric, matched_liwc))
# 
#   genius_means <- matched_genius[, lapply(.SD, mean, na.rm = TRUE), .SDcols = genius_numeric_cols]
#   topic_means <- matched_topic[, lapply(.SD, mean, na.rm = TRUE), .SDcols = topic_numeric_cols]
#   liwc_means <- matched_liwc[, lapply(.SD, mean, na.rm = TRUE), .SDcols = liwc_numeric_cols]
# 
#   # Fallbacks if no rows
#   if (nrow(genius_means) == 0)
#     genius_means <- data.table(matrix(NA, ncol = length(genius_numeric_cols), nrow = 1, dimnames = list(NULL, genius_numeric_cols)))
#   if (nrow(topic_means) == 0)
#     topic_means <- data.table(matrix(NA, ncol = length(topic_numeric_cols), nrow = 1, dimnames = list(NULL, topic_numeric_cols)))
#   if (nrow(liwc_means) == 0)
#     liwc_means <- data.table(matrix(NA, ncol = length(liwc_numeric_cols), nrow = 1, dimnames = list(NULL, liwc_numeric_cols)))
# 
#   setnames(genius_means, names(genius_means), paste0("genius_", names(genius_means)))
#   setnames(topic_means, names(topic_means), paste0("topic_", names(topic_means)))
#   setnames(liwc_means, names(liwc_means), paste0("liwc_", names(liwc_means)))
# 
#   # --- 9. Combine All Features ---
#   final_features <- cbind(
#     data.table(user_id = user_id, start_time = start_time, end_time = end_time, time_bin = time_bin),
#     gps_features,
#     headset_summary,
#     detected_activity_summary,
#     activity_summary,
#     app_usage_summary,
#     screen_state_summary,
#     spotify_means,
#     genius_means,
#     topic_means,
#     liwc_means
#   )
# 
#   return(final_features)
# }

# # --- 10. Main Execution Block ---
# offset <- 0
# output_file <- "data/results/features_all_windows_optimized.rds"
# window_indices <- seq((offset + 1), nrow(music_windows))
# 
# # Set up parallel processing
# options(future.globals.maxSize = 8 * 1024^3)  # 8 GB limit
# plan(multisession) # Automatically uses available cores
# 
# message(sprintf("Starting parallel feature extraction for %d windows...", length(window_indices)))
# 
# # Use future_lapply for parallel execution
# all_features_list <- future_lapply(window_indices, function(i) {
#   row <- music_windows[i, ]
#   
#   # Call the optimized function
#   features <- extract_features_for_window(
#     user_id = row$user_id,
#     start_time = row$start_time,
#     end_time = row$end_time
#   )
#   
#   # Optional progress indicator
#   if (i %% 5 == 0) {
#     cat(sprintf("✓ Window %d processed\n", i))
#   }
#   
#   return(features)
# }, future.seed = TRUE) # for reproducibility
# 
# # --- 11. Combine Results and Save ---
# message("Combining results...")
# # Filter out NULLs from skipped windows and bind rows efficiently
# all_features <- rbindlist(all_features_list, fill = TRUE)
# 
# # Ensure consistent column order before saving (optional but good practice)
# fixed_column_names <- c(
#   "user_id", "start_time", "end_time", "time_bin",
#   names(gps_features),
#   paste0("headset_", headset_states),
#   paste0("detected_", detected_activity_keys),
#   paste0("phone_", all_activity_names),
#   paste0("app_", app_categories),
#   paste0("screen_", screen_categories),
#   paste0("music_", spotify_numeric_features),
#   paste0("genius_", genius_numeric_features),
#   paste0("topic_", topic_numeric_features),
#   paste0("liwc_", liwc_numeric_features)
# )
# # Select only columns that actually exist in the final data table
# final_cols <- intersect(fixed_column_names, names(all_features))
# all_features <- all_features[, ..final_cols]
# 
# 
# saveRDS(all_features, output_file)
# 
# message(sprintf("Feature extraction completed. Saved %d rows to %s", nrow(all_features), output_file))



# --- 10. Main Execution Block (Sequential Mode with Resume) ---
offset <- 0
output_file <- "data/results/new/features_all_windows_optimized.rds"
window_indices <- seq((offset + 1), nrow(music_windows))

message(sprintf("Starting sequential feature extraction for %d windows...", length(window_indices)))

# --- Load previous intermediate results if any ---
intermediate_files <- list.files("data/new/results", pattern = "^features_partial_\\d+\\.rds$", full.names = TRUE)
all_features_list <- vector("list", length(window_indices))

if (length(intermediate_files) > 0) {
  message("Found intermediate files, resuming from previous progress...")
  # Load the most recent one
  latest_file <- intermediate_files[which.max(as.numeric(gsub(".*features_partial_(\\d+)\\.rds", "\\1", intermediate_files)))]
  prev_features <- readRDS(latest_file)
  all_features_list[seq_along(prev_features)] <- prev_features
  start_index <- sum(sapply(prev_features, function(x) !is.null(x))) + 1
  message(sprintf("Resuming from window %d", start_index))
} else {
  start_index <- 1
}

# Start timer
start_time_all <- Sys.time()

# Sequential execution with live progress
for (i in start_index:length(window_indices)) {
  row <- music_windows[window_indices[i], ]

  # Skip windows already processed
  if (!is.null(all_features_list[[i]])) next

  # Try-catch to skip any failed iterations safely
  features <- tryCatch(
    extract_features_for_window(
      user_id = row$user_id,
      start_time = row$start_time,
      end_time = row$end_time
    ),
    error = function(e) {
      message(sprintf("⚠️ Error in window %d (%s - %s): %s",
                      i, row$start_time, row$end_time, e$message))
      return(NULL)
    }
  )

  # Ensure the output is exactly one row
  if (is.data.table(features) && nrow(features) == 1L) {
    all_features_list[[i]] <- features
  } else if (!is.null(features) && nrow(features) > 1L) {
    warning(sprintf("Feature extraction returned %d rows for window %d — keeping first row.", nrow(features), i))
    all_features_list[[i]] <- features[1L, ]
  } else {
    all_features_list[[i]] <- NULL
  }

  # Progress indicator every N windows
  if (i %% 50 == 0 || i == length(window_indices)) {
    elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
    pct <- round(100 * i / length(window_indices), 1)
    message(sprintf("✓ Processed %d / %d windows (%.1f%%) — Elapsed: %.1f min",
                    i, length(window_indices), pct, as.numeric(elapsed)))

    # Intermediate save every 500 windows
    if (i %% 500 == 0 || i == length(window_indices)) {
      tmp_file <- sprintf("data/results/new/features_partial_%d.rds", i)
      saveRDS(all_features_list, tmp_file)
      message(sprintf("Saved intermediate results to %s", tmp_file))
    }
  }
}

# --- 11. Combine Results and Save ---
message("Combining results...")
valid_features <- Filter(Negate(is.null), all_features_list)
all_features <- rbindlist(valid_features, fill = TRUE)

# Ensure consistent column order before saving (optional but good practice)
fixed_column_names <- c(
  "user_id", "start_time", "end_time", "time_bin",
  names(gps_features),
  paste0("headset_", headset_states),
  paste0("detected_", detected_activity_keys),
  paste0("phone_", all_activity_names),
  paste0("app_", app_categories),
  paste0("screen_", screen_categories),
  paste0("music_", spotify_numeric_features),
  paste0("genius_", genius_numeric_features),
  paste0("topic_", topic_numeric_features),
  paste0("liwc_", liwc_numeric_features)
)
final_cols <- intersect(fixed_column_names, names(all_features))
all_features <- all_features[, ..final_cols]

# Save final results
saveRDS(all_features, output_file)

total_elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
message(sprintf("Feature extraction completed. Saved %d rows to %s (%.1f min total)",
                nrow(all_features), output_file, as.numeric(total_elapsed)))



# --- 10. Main Execution Block (Parallel, Resume-Safe, One File per Window) ----------------
# ------------------------------------------------------------------------------------------
library(future)
library(future.apply)
library(data.table)

output_dir <- "data/results/new"
final_output_file <- "data/results/new/features_all_windows_combined.rds"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

message(sprintf("📦 Preparing to extract features for %d windows...", nrow(music_windows)))

# --- Detect already processed windows ---
existing_files <- list.files(output_dir, pattern = "^window_\\d+\\.rds$", full.names = TRUE)
processed_indices <- as.integer(gsub(".*window_(\\d+)\\.rds", "\\1", existing_files))
remaining_indices <- setdiff(seq_len(nrow(music_windows)), processed_indices)

message(sprintf("🔍 Found %d processed windows, %d remaining to compute",
                length(processed_indices), length(remaining_indices)))

if (length(remaining_indices) == 0) {
  message("✅ All windows already processed — skipping computation.")
} else {
  
  # --- Parallel setup ---
  options(future.globals.maxSize = 8 * 1024^3)  # Allow up to 8GB per worker
  plan(multisession, workers = max(1, parallel::detectCores() - 1))
  
  # --- Run extraction in parallel ---
  start_time_all <- Sys.time()
  
  future_lapply(remaining_indices, function(i) {
    row <- music_windows[i, ]
    
    out_file <- sprintf("%s/window_%05d.rds", output_dir, i)
    if (file.exists(out_file)) return(NULL)  # skip if already done
    
    start_time <- Sys.time()
    features <- tryCatch(
      extract_features_for_window(
        user_id = row$user_id,
        start_time = row$start_time,
        end_time = row$end_time
      ),
      error = function(e) {
        message(sprintf("⚠️ Error in window %d (%s - %s): %s",
                        i, row$start_time, row$end_time, e$message))
        return(NULL)
      }
    )
    
    if (!is.null(features)) {
      saveRDS(features, out_file)
      elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 2)
      message(sprintf("✅ Saved window %d (User %s) — %.2f sec", i, row$user_id, elapsed))
    }
    gc()
    NULL
  })
  
  total_elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
  message(sprintf("🏁 Parallel extraction completed in %.1f minutes", as.numeric(total_elapsed)))
}

# --- 11. Combine All Per-Window Files ---
message("🔄 Combining all window results...")

all_files <- list.files(output_dir, pattern = "^window_\\d+\\.rds$", full.names = TRUE)
all_features_list <- lapply(all_files, function(f) {
  obj <- readRDS(f)
  if (is.null(obj)) return(NULL)
  as.data.table(obj)
})

valid_features <- Filter(Negate(is.null), all_features_list)
all_features <- rbindlist(valid_features, fill = TRUE)

# --- 12. Ensure Consistent Column Order ---
fixed_column_names <- c(
  "user_id", "start_time", "end_time", "time_bin",
  gps_features,
  paste0("headset_", headset_states),
  paste0("detected_", detected_activity_keys),
  paste0("phone_", all_activity_names),
  paste0("app_", app_categories),
  paste0("screen_", screen_categories),
  paste0("music_", spotify_numeric_features),
  paste0("genius_", genius_numeric_features),
  paste0("topic_", topic_numeric_features),
  paste0("liwc_", liwc_numeric_features)
)
final_cols <- intersect(fixed_column_names, names(all_features))
all_features <- all_features[, ..final_cols]

# --- 13. Save Final Combined Dataset ---
saveRDS(all_features, final_output_file)
message(sprintf("✅ Combined and saved %d total rows to %s", nrow(all_features), final_output_file))









library(data.table)
library(readr)

output_csv <- "data/results/features_all_combined.csv"
partial_files <- list.files("data/results", pattern = "^features_partial_\\d+\\.rds$", full.names = TRUE)
partial_files <- partial_files[order(as.numeric(gsub(".*features_partial_(\\d+)\\.rds", "\\1", partial_files)))]

# Start fresh CSV with header from first file
first_part <- readRDS(partial_files[1])
if (is.list(first_part) && !is.data.table(first_part)) {
  first_part <- Filter(Negate(is.null), first_part)
  first_part <- rbindlist(first_part, fill = TRUE)
}
fwrite(first_part, output_csv)
rm(first_part)
gc()

# Append remaining parts
for (file in partial_files[-1]) {
  message(sprintf("Appending %s ...", basename(file)))
  part <- readRDS(file)
  if (is.list(part) && !is.data.table(part)) {
    part <- Filter(Negate(is.null), part)
    part <- rbindlist(part, fill = TRUE)
  }
  fwrite(part, output_csv, append = TRUE)
  rm(part)
  gc()
}

message("✅ All partial files successfully combined into a single CSV.")

