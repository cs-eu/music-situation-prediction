# --- 1. Load Libraries ---
library(DBI)
library(data.table)
library(lubridate)
library(jsonlite)
library(future.apply)

# --- 2. Configuration & Setup ---
setwd("") # Set to your working directory
source("utils/label_usage_sessions.R")

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

# --- 6. Pre-process JSON Activities (The Final, Robust Method) ---
message("Pre-processing JSON activity data with RcppSimdJson and rbindlist...")
library(RcppSimdJson)
library(data.table)

# Filter out rows with no activity data first
snapshot_filtered <- snapshot_raw[!is.na(detectedActivities) & detectedActivities != "[]", .(id, detectedActivities)]

parsed_list <- fparse(snapshot_filtered$detectedActivities)
names(parsed_list) <- snapshot_filtered$id
snapshot_activities <- rbindlist(parsed_list, idcol = "id", fill = TRUE)
snapshot_activities[, id := as.integer(id)]

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

# Musical key is categorical and must not be averaged as a numeric outcome.
spotify_numeric_features <- c("track_danceability", "track_energy", "track_loudness", "track_mode_major", "track_speechiness", "track_acousticness", "track_instrumentalness", "track_liveness", "track_valence", "track_tempo", "track_non_music")
genius_numeric_features <- c("fear", "anger", "trust", "surprise", "positive", "negative", "sadness", "disgust", "joy", "anticipation", "lyric_len")
topic_numeric_features <- paste0("Topic ", 0:29)
liwc_numeric_features <- c("WC", "Analytic", "Clout", "Authentic", "Tone", "WPS", "Sixltr", "Dic", "function.", "pronoun", "ppron", "i", "we", "you_total", "you_sing", "you_plur", "you_formal", "other", "shehe", "they", "ipron", "article", "prep", "auxverb", "adverb", "conj", "negate", "verb", "adj", "compare", "interrog", "number", "quant", "affect", "posemo", "negemo", "anx", "anger", "sad", "social", "family", "friend", "female", "male", "cogproc", "insight", "cause", "discrep", "tentat", "certain", "differ", "percept", "see", "hear", "feel", "bio", "body", "health", "sexual", "ingest", "drives", "affiliation", "achiev", "power", "reward", "risk", "focuspast", "focuspresent", "focusfuture", "relativ", "motion", "space", "time", "work", "leisure", "home", "money", "relig", "death", "informal", "swear", "netspeak", "assent", "nonflu", "filler", "AllPunc", "Period", "Comma", "Colon", "SemiC", "QMark", "Exclam", "Dash", "Quote", "Apostro", "Parenth", "OtherP", "Emoji")

message("Pre-joining all music feature tables into a master table...")

# Namespace independent families before joining, especially Genius/LIWC anger.
spotify_feature_cols <- intersect(spotify_numeric_features, names(spotify_data))
setnames(spotify_data, spotify_feature_cols, paste0("music_", spotify_feature_cols))
for (family in c("genius", "topic", "liwc")) {
  table_name <- paste0(family, "_features")
  features <- get(table_name)
  features <- features[!is.na(track_spotify_id) & nzchar(track_spotify_id)]
  if (anyDuplicated(features, by = "track_spotify_id")) {
    stop(sprintf("%s must contain one row per track_spotify_id", table_name))
  }
  feature_cols <- setdiff(names(features), "track_spotify_id")
  prefixed_cols <- paste0(family, "_", feature_cols)
  if (family == "topic") prefixed_cols <- gsub("\\s+", "_", trimws(prefixed_cols))
  setnames(features, feature_cols, prefixed_cols)
  assign(table_name, features)
}
spotify_numeric_features <- paste0("music_", spotify_numeric_features)
genius_numeric_features <- paste0("genius_", genius_numeric_features)
topic_numeric_features <- paste0("topic_", gsub("\\s+", "_", trimws(topic_numeric_features)))
liwc_numeric_features <- paste0("liwc_", liwc_numeric_features)

# Spotify is the authoritative track table: missing lyrics must not drop audio.
music_features_master <- genius_features[spotify_data, on = "track_spotify_id"]
music_features_master <- topic_features[music_features_master, on = "track_spotify_id"]
music_features_master <- liwc_features[music_features_master, on = "track_spotify_id"]
if (anyDuplicated(music_features_master, by = c("track", "artist"))) {
  stop("Music features must contain one row per track/artist for unweighted track means")
}

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

extract_features_for_window <- function(uid, window_start, window_end, screen_lookback_seconds = 7200) {
  if (length(uid) != 1L || is.na(uid) ||
      length(window_start) != 1L || length(window_end) != 1L ||
      is.na(window_start) || is.na(window_end)) return(NULL)
  if (!inherits(window_start, "POSIXt") || !inherits(window_end, "POSIXt")) {
    stop("Window timestamps must be POSIXt; parse them with an explicit source timezone")
  }
  window_seconds <- as.numeric(difftime(window_end, window_start, units = "secs"))
  if (!is.finite(window_seconds) || window_seconds <= 0) return(NULL)
  if (length(screen_lookback_seconds) != 1L || !is.numeric(screen_lookback_seconds) ||
      !is.finite(screen_lookback_seconds) || screen_lookback_seconds < 0) {
    stop("screen_lookback_seconds must be a finite nonnegative number")
  }
  # Start total timer
  t0 <- Sys.time()
  
  # Helper for timing blocks
  time_log <- function(label, start_t) {
    dt <- round(as.numeric(difftime(Sys.time(), start_t, units = "secs")), 3)
    message(sprintf("⏱ %s took %.3f sec", label, dt))
  }
  
  # --- 1. Filter activity data ---
  t1 <- Sys.time()
  acts <- activity_data_raw[user_id == uid & timestamp %between% c(window_start, window_end)]
  time_log("Activity filtering", t1)
  if (nrow(acts) == 0) return(NULL)
  
  # --- 2. Time Bin ---
  t1 <- Sys.time()
  time_bin <- get_time_bin(window_start)
  time_log("Time bin assignment", t1)
  
  # --- 3. GPS / Landuse ---
  t1 <- Sys.time()
  gps_row <- gps_landuse_df[user_id == uid &
                             start_time == window_start &
                             end_time == window_end]
  if (nrow(gps_row) > 1L) {
    stop(sprintf("Multiple GPS rows for user %s at %s", uid, window_start))
  }
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
  screen_acts <- activity_data_raw[user_id == uid & activityName == "SCREEN" &
                                   timestamp >= window_start - screen_lookback_seconds &
                                   timestamp <= window_end]
  screen_state_durations <- preprocessing_screen_window(
    screen_acts, window_start, window_end, lookback_seconds = screen_lookback_seconds
  )
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
  
  # Preserve equal weighting of unique title/artist tracks, not play counts/duration.
  spotify_means <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = spotify_numeric_cols]
  genius_means  <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = genius_numeric_cols]
  topic_means   <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = topic_numeric_cols]
  liwc_means    <- all_matched_features[, lapply(.SD, mean, na.rm = TRUE), .SDcols = liwc_numeric_cols]
  
  spotify_means <- ensure_cols(spotify_means, spotify_numeric_features, fill = NA_real_)
  genius_means  <- ensure_cols(genius_means,  genius_numeric_features,  fill = NA_real_)
  topic_means   <- ensure_cols(topic_means,   topic_numeric_features,   fill = NA_real_)
  liwc_means    <- ensure_cols(liwc_means,    liwc_numeric_features,    fill = NA_real_)
  
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
        "VALIDATION FAILED: Feature table '%s' for user %s at %s has %d rows (expected 1). Skipping window to prevent NAs.",
        tbl_name, uid, window_start, nrow(feature_tables_to_check[[tbl_name]])
      ))
      return(NULL)
    }
  }
  
  time_log("Feature table validation", t1)
  final_features <- cbind(
    data.table(user_id = uid, start_time = window_start, end_time = window_end, time_bin = time_bin),
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
  message(sprintf("Window processed for user %s (%.3f sec total)", uid, total_dt))
  
  return(final_features)
}

# --- 10. Main Execution Block ---
offset <- 0L
checkpoint_schema_version <- 2L
output_dir <- "data/results/participant_safe_v2"
output_file <- file.path(output_dir, "features_all_windows_participant_safe_v2.rds")
checkpoint_file <- file.path(output_dir, "features_checkpoint_v2.rds")
if (length(offset) != 1L || !is.numeric(offset) || !is.finite(offset) ||
    offset < 0 || offset != floor(offset) || offset > nrow(music_windows)) {
  stop("offset must be an integer between zero and the number of music windows")
}
if (!dir.exists(output_dir) && !dir.create(output_dir, recursive = TRUE)) {
  stop(sprintf("Cannot create output directory: %s", output_dir))
}
window_indices <- seq_len(nrow(music_windows))
window_indices <- window_indices[window_indices > offset]
window_keys <- as.data.frame(music_windows[window_indices, .(user_id, start_time, end_time)])

message(sprintf("Starting sequential feature extraction for %d windows...", length(window_indices)))

# Never resume legacy caches: their participant filters and list indices are unsafe.
all_features_list <- vector("list", length(window_indices))
last_processed <- 0L
if (file.exists(checkpoint_file)) {
  checkpoint <- readRDS(checkpoint_file)
  if (!is.list(checkpoint) ||
      !identical(checkpoint$schema_version, checkpoint_schema_version) ||
      !identical(checkpoint$window_indices, window_indices) ||
      !identical(checkpoint$window_keys, window_keys) ||
      length(checkpoint$last_processed) != 1L || !is.numeric(checkpoint$last_processed) ||
      !is.finite(checkpoint$last_processed) || checkpoint$last_processed < 0 ||
      checkpoint$last_processed != floor(checkpoint$last_processed) ||
      checkpoint$last_processed > length(window_indices) ||
      !is.list(checkpoint$features) || is.data.frame(checkpoint$features) ||
      length(checkpoint$features) != length(window_indices)) {
    stop("Invalid feature checkpoint: schema, window selection or progress does not match")
  }
  if (any(vapply(checkpoint$features, function(x) {
    !is.null(x) && (!is.data.table(x) || nrow(x) != 1L)
  }, logical(1L))) || any(!vapply(
    checkpoint$features[seq_along(window_indices) > checkpoint$last_processed], is.null, logical(1L)
  ))) {
    stop("Invalid feature checkpoint: malformed results or unprocessed slots are not empty")
  }
  all_features_list <- checkpoint$features
  last_processed <- as.integer(checkpoint$last_processed)
  message(sprintf("Resuming after window %d", last_processed))
}
start_index <- last_processed + 1L
pending_indices <- if (start_index <= length(window_indices)) {
  seq.int(start_index, length(window_indices))
} else integer()

save_checkpoint <- function() {
  checkpoint <- list(
    schema_version = checkpoint_schema_version, last_processed = last_processed,
    window_indices = window_indices, window_keys = window_keys, features = all_features_list
  )
  tmp_file <- paste0(checkpoint_file, ".tmp")
  saveRDS(checkpoint, tmp_file)
  if (!file.rename(tmp_file, checkpoint_file)) stop("Cannot replace feature checkpoint")
  message(sprintf("Saved checkpoint through window %d to %s", last_processed, checkpoint_file))
}

# Start timer
start_time_all <- Sys.time()

# Sequential execution with live progress
for (i in pending_indices) {
  row <- music_windows[window_indices[i], ]

  # Try-catch to skip any failed iterations safely
  features <- tryCatch(
    extract_features_for_window(
      uid = row$user_id,
      window_start = row$start_time,
      window_end = row$end_time
    ),
    error = function(e) {
      message(sprintf("Error in window %d (%s - %s): %s",
                      i, row$start_time, row$end_time, e$message))
      return(NULL)
    }
  )

  if (!is.null(features) && (!is.data.table(features) || nrow(features) != 1L)) {
    warning(sprintf("Feature extraction returned an invalid result for window %d; rejecting window.", i))
    features <- NULL
  }
  # Single-bracket assignment retains skipped NULL slots and their window indices.
  all_features_list[i] <- list(features)
  last_processed <- i

  # Progress indicator every N windows
  if (i %% 50 == 0 || i == length(window_indices)) {
    elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
    pct <- round(100 * i / length(window_indices), 1)
    message(sprintf("Processed %d / %d windows (%.1f%%) - Elapsed: %.1f min",
                    i, length(window_indices), pct, as.numeric(elapsed)))
  }
  if (i %% 500L == 0L) save_checkpoint()
}
save_checkpoint()

# --- 11. Combine Results and Save ---
message("Combining results...")
valid_features <- Filter(Negate(is.null), all_features_list)
all_features <- rbindlist(valid_features, fill = TRUE)

# Ensure consistent column order before saving (optional but good practice)
fixed_column_names <- c(
  "user_id", "start_time", "end_time", "time_bin",
  gps_features,
  paste0("headset_", headset_states),
  paste0("detected_", detected_activity_keys),
  paste0("phone_", all_activity_names),
  paste0("app_", app_categories),
  paste0("screen_", screen_categories),
  spotify_numeric_features,
  genius_numeric_features,
  topic_numeric_features,
  liwc_numeric_features
)
final_cols <- intersect(fixed_column_names, names(all_features))
all_features <- all_features[, ..final_cols]

# Save final results
attr(all_features, "extraction_provenance") <- list(
  schema_version = checkpoint_schema_version, extractor = "participant_safe_v2",
  participant_filter = "exact_user_id", gps_match = "exact_user_id_start_end",
  track_aggregation = "unweighted_unique_title_artist", screen_lookback_seconds = 7200,
  last_processed = last_processed, window_indices = window_indices
)
saveRDS(all_features, output_file)

total_elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
message(sprintf("Feature extraction completed. Saved %d rows to %s (%.1f min total)",
                nrow(all_features), output_file, as.numeric(total_elapsed)))
