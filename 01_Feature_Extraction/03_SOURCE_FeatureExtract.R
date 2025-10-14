# library(DBI)
# library(dplyr)
# library(lubridate)
# library(tidyr)
# library(readr)
# library(tibble)
# library(jsonlite)
# library(purrr)
# library(osmdata)
# library(sf)
# library(data.table)
# library(stringr)
# library(future.apply)
# library(future)
# library(parallel)
# 
# setwd("/home/clemensschwarzmann/MusicInSituations/01_Feature_Extraction")
# 
# source("utils/label_usage_sessions.R")
# 
# # --- DB connection ---
# con <- dbConnect(
#   drv = RMariaDB::MariaDB(),
#   username = mariadb_user,
#   password = mariadb_pw,
#   host = "localhost",
#   port = 3306,
#   dbname = "live"
# )
# on.exit(dbDisconnect(con), add = TRUE)
# 
# music =  DBI::dbReadTable(con, "ps_music")
# snapshot <- dbGetQuery(con, "SELECT id, headsetState, detectedActivities FROM ps_snapshot")
# 
# # Load app categorization
# app_categorization <- read_csv("data/helper/app_categorisation_2020_v2.csv",
#                                col_types = cols(
#                                  App_name = col_character(),
#                                  Final_Rating = col_character()
#                                )) %>%
#   dplyr::rename(packageName = App_name) %>%
#   mutate(Final_Rating = ifelse(is.na(Final_Rating), "Unknown", Final_Rating))
# 
# # All possible app categories (Final_Rating)
# app_categories <- sort(unique(app_categorization$Final_Rating))
# 
# # Load music windows
# music_windows <- read_csv("data/results/music_windows_all.csv",
#                           col_types = cols(
#                             user_id = col_character(),
#                             start_time = col_datetime(format = ""),
#                             end_time = col_datetime(format = "")
#                           ))
# 
# gps_landuse_df <- readRDS("data/results/gps_landuse_by_window.rds")
# 
# # Load Spotify data once at the top level (outside function)
# spotify_data <- readRDS("data/helper/distinct_tracks_enriched.rds")
# 
# genius_features <- read.csv("data/helper/lyrics-features/genius_features.csv")
# topic_features <- read.csv("data/helper/lyrics-features/topic_features.csv")
# liwc_features <- read.csv("data/helper/lyrics-features/Lyrics_LIWC.csv")
# 
# genius_features <- genius_features %>% mutate(track_spotify_id = sub("lyrics:", "", lyricsID))
# topic_features <- topic_features %>% mutate(track_spotify_id = sub("lyrics:", "", lyricsID))
# liwc_features <- liwc_features %>% mutate(track_spotify_id = sub("lyrics:", "", key))
# 
# # Load preloaded activity data
# activity_data <- readRDS("data/helper/combined_activity_data.rds")
# 
# home_locations <- read.csv("data/helper/gps_home.csv")
# work_locations <- read.csv("data/helper/gps_work.csv")
# 
# # Columns to keep from Spotify data for numeric mean aggregation
# spotify_numeric_features <- c(
#   "track_danceability", "track_energy", "track_key", "track_loudness",
#   "track_mode_major", "track_speechiness", "track_acousticness",
#   "track_instrumentalness", "track_liveness", "track_valence", "track_tempo",
#   "track_non_music"
# )
# 
# genius_numeric_features <- c(
#   "fear", "anger", "trust", "surprise", "positive", "negative",
#   "sadness", "disgust", "joy", "anticipation", "lyric_len"
# )
# 
# topic_numeric_features <- paste0("Topic.", 0:29)
# 
# liwc_numeric_features <- c(
#   "WC", "Analytic", "Clout", "Authentic", "Tone", "WPS", "Sixltr", "Dic",
#   "function.", "pronoun", "ppron", "i", "we", "you_total", "you_sing", "you_plur",
#   "you_formal", "other", "shehe", "they", "ipron", "article", "prep", "auxverb",
#   "adverb", "conj", "negate", "verb", "adj", "compare", "interrog", "number",
#   "quant", "affect", "posemo", "negemo", "anx", "anger", "sad", "social",
#   "family", "friend", "female", "male", "cogproc", "insight", "cause", "discrep",
#   "tentat", "certain", "differ", "percept", "see", "hear", "feel", "bio", "body",
#   "health", "sexual", "ingest", "drives", "affiliation", "achiev", "power",
#   "reward", "risk", "focuspast", "focuspresent", "focusfuture", "relativ",
#   "motion", "space", "time", "work", "leisure", "home", "money", "relig",
#   "death", "informal", "swear", "netspeak", "assent", "nonflu", "filler",
#   "AllPunc", "Period", "Comma", "Colon", "SemiC", "QMark", "Exclam", "Dash",
#   "Quote", "Apostro", "Parenth", "OtherP", "Emoji"
# )
# 
# 
# all_activity_names <- c("AIRPLANE", "APPS", "BATTERYSAVINGMODE", "BLUETOOTH", "CAMERA", "GPS",
#                    "NOTIFICATION", "PHONE", "POWER", "SCREEN", "SMS")
# 
# # Filtered detected activity keys
# detected_activity_keys <- c(
#   "STILL", "WALKING", "RUNNING", "ON_BICYCLE", "ON_FOOT", "IN_VEHICLE",
#   "IN_ROAD_VEHICLE", "IN_RAIL_VEHICLE", "IN_FOUR_WHEELER_VEHICLE"
# )
# 
# screen_categories <- c("ON_UNLOCKED", "OFF_UNLOCKED", "OFF_LOCKED", "ON_LOCKED", "UNKNOWN")
# 
# headset_states <- c("PLUGGED", "UNPLUGGED")
# 
# safe_summarise <- function(df, col_names, fill = 0, prefix = "") {
#   if (nrow(df) == 0) {
#     # If the input DF is empty, create a new tibble with prefixed column names
#     prefixed_col_names <- paste0(prefix, col_names)
#     return(as_tibble(as.list(setNames(rep(fill, length(prefixed_col_names)), prefixed_col_names))))
#   }
#   
#   # Add missing columns with defaults and apply prefix
#   temp_df <- df
#   current_names <- names(temp_df)
#   for (name in col_names) {
#     if (!name %in% current_names) {
#       temp_df[[name]] <- fill
#     }
#   }
#   
#   # Select, then rename with prefix
#   temp_df <- temp_df %>%
#     select(all_of(col_names)) %>%
#     rename_with(~ paste0(prefix, .), all_of(col_names))
#   
#   return(temp_df)
# }
# 
# get_time_bin <- function(start_time, end_time) {
#   # Define time bin boundaries (in 24-hour format)
#   bins <- tibble(
#     bin_name = c("Morning", "Late Morning", "Noon", "Afternoon", "Evening", "Night"),
#     start_hour = c(5, 8, 11, 13, 17, 21),
#     end_hour = c(8, 11, 13, 17, 21, 24)
#   )
#   
#   # Convert to local time if needed, assuming UTC here
#   time_seq <- seq(from = start_time, to = end_time, by = "1 min")
#   hours <- lubridate::hour(time_seq)
#   
#   # Count minutes in each bin
#   bin_counts <- bins %>%
#     rowwise() %>%
#     mutate(
#       count = sum(hours >= start_hour & hours < end_hour)
#     )
#   
#   # Return the bin with max count
#   return(bin_counts$bin_name[which.max(bin_counts$count)])
# }
# 
# extract_features_for_window <- function(user_id, start_time, end_time, con, snapshot, music) {
# 
#   # --- 1. Filter activity data ---
#   acts <- activity_data %>%
#     filter(
#       user_id == !!user_id,
#       timestamp >= !!start_time,
#       timestamp <= !!end_time
#     )
# 
#   if (nrow(acts) == 0) {
#     message(sprintf("Skipping User %s: no activity data in %s to %s",
#                     user_id, format(start_time, "%Y-%m-%d %H:%M:%S"), format(end_time, "%Y-%m-%d %H:%M:%S")))
#     return(NULL)
#   }
#   acts <- acts %>% mutate(timestamp = as.POSIXct(timestamp, tz = "UTC"))
# 
#   # --- 2. Time Bin ---
#   time_bin <- get_time_bin(start_time, end_time)
# 
#   # --- 3. GPS / Landuse ---
#   gps_row <- gps_landuse_df %>%
#     filter(user_id == !!user_id,
#            start_time == !!start_time,
#            end_time == !!end_time)
# 
#   if (nrow(gps_row) == 0) {
#     # fallback if not found
#     gps_count <- 0
#     gps_first_latitude <- NA_real_
#     gps_first_longitude <- NA_real_
#     gps_landuse_type <- "Unknown"
#     gps_home <- 0
#     gps_work <- 0
#   } else {
#     gps_count <- gps_row$gps_count[1]
#     gps_first_latitude <- gps_row$gps_first_latitude[1]
#     gps_first_longitude <- gps_row$gps_first_longitude[1]
#     gps_landuse_type <- gps_row$gps_landuse_type[1]
#     gps_home <- gps_row$gps_home[1]
#     gps_work <- gps_row$gps_work[1]
#   }
# 
#   # --- 4. Headset summary ---
#   snapshot_ids <- unique(acts$snapshot_id)
#   snapshot_info <- snapshot %>% filter(id %in% snapshot_ids)
# 
#   headset_summary <- snapshot_info %>%
#     group_by(headsetState) %>%
#     dplyr::summarise(count = dplyr::n(), .groups = "drop") %>%
#     pivot_wider(names_from = headsetState, values_from = count, values_fill = 0)
# 
#   headset_summary <- safe_summarise(headset_summary, headset_states, prefix = "headset_", fill = 0)
# 
#   # --- 5. Detected Activities ---
#   detected_activity_summary <- snapshot_info %>%
#     pull(detectedActivities) %>%
#     discard(is.na) %>%
#     map(~ fromJSON(.) %>% as_tibble()) %>%
#     bind_rows()
# 
#   if (nrow(detected_activity_summary) > 0) {
#     detected_activity_summary <- detected_activity_summary %>%
#       dplyr::summarise(across(everything(), sum, na.rm = TRUE))
#   }
#   detected_activity_summary <- safe_summarise(detected_activity_summary, detected_activity_keys, prefix = "detected_", fill = 0)
# 
#   # --- 6. Phone Activities ---
#   activity_summary <- acts %>%
#     group_by(activityName) %>%
#     dplyr::summarise(count = dplyr::n(), .groups = "drop") %>%
#     pivot_wider(names_from = activityName, values_from = count, values_fill = 0) %>%
#     mutate(across(everything(), as.numeric))
# 
#   activity_summary <- safe_summarise(activity_summary, all_activity_names, prefix = "phone_", fill = 0)
# 
#   # --- 7. App Usage ---
#   app_usage_summary <- acts %>%
#     filter(!is.na(packageName)) %>%
#     left_join(app_categorization, by = "packageName") %>%
#     mutate(Final_Rating = ifelse(is.na(Final_Rating), "Unknown", Final_Rating)) %>%
#     dplyr::count(Final_Rating, name = "count") %>%
#     pivot_wider(names_from = Final_Rating, values_from = count, values_fill = 0)
# 
#   app_usage_summary <- safe_summarise(app_usage_summary, app_categories, prefix = "app_", fill = 0)
# 
#   # --- 8. Screen State ---
#   screen_state_durations <- preprocessing_screen_window(acts, start_time, end_time)
#   if (is.null(screen_state_durations) || nrow(screen_state_durations) == 0) {
#     screen_state_summary <- safe_summarise(tibble(), screen_categories, prefix = "screen_", fill = 0)
#   } else {
#     screen_state_summary <- as_tibble(screen_state_durations) %>%
#       rename(screen_event = event, screen_duration_sec = duration_sec) %>%
#       pivot_wider(names_from = screen_event, values_from = screen_duration_sec, values_fill = 0) %>%
#       safe_summarise(screen_categories, prefix = "screen_", fill = 0)
#   }
# 
#   # --- 9. Spotify / Music ---
#   music_events <- acts %>%
#     filter(activityName == "MUSIC" & !is.na(music_id)) %>%
#     select(music_id)
# 
#   if (nrow(music_events) > 0) {
#     music_window <- music %>%
#       filter(id %in% music_events$music_id) %>%
#       select(title, artist, album, duration, id) %>%
#       distinct()
# 
#     if (nrow(music_window) == 0) {
#       message(sprintf("Skipping User %s: missing music details", user_id))
#       return(NULL)
#     }
# 
#     matched_tracks <- left_join(
#       music_window,
#       spotify_data,
#       by = c("title" = "track", "artist" = "artist")
#     )
# 
#     if (nrow(matched_tracks) == 0) {
#       message(sprintf("Skipping User %s: no Spotify matches found", user_id))
#       return(NULL)
#     }
# 
#     spotify_numeric_cols <- names(Filter(is.numeric, matched_tracks))
#     spotify_means_raw <- matched_tracks %>%
#       select(all_of(spotify_numeric_cols)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     # spotify_means_raw <- matched_tracks %>%
#     #   select(any_of(spotify_numeric_features)) %>%
#     #   dplyr::summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
# 
#     spotify_means <- safe_summarise(spotify_means_raw, spotify_numeric_features, fill = NA_real_, prefix = "music_")
# 
#     # --- Genius / Topic / LIWC ---
#     # Genius
#     matched_genius <- left_join(matched_tracks, genius_features, by = "track_spotify_id")
#     genius_numeric_cols <- names(Filter(is.numeric, matched_genius))
#     genius_means_raw <- matched_genius %>%
#       select(all_of(genius_numeric_cols)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     # genius_numeric_features <- setdiff(names(genius_features), c("lyricsID", "spotify_id", "track_spotify_id"))
#     # genius_means_raw <- matched_genius %>%
#     #   select(any_of(genius_numeric_features)) %>%
#     #   dplyr::summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     genius_means <- safe_summarise(genius_means_raw, genius_numeric_features, fill = NA_real_, prefix = "genius_")
# 
#     # Topic
#     matched_topic <- left_join(matched_tracks, topic_features, by = "track_spotify_id")
#     topic_numeric_cols <- names(Filter(is.numeric, matched_topic))
#     topic_means_raw <- matched_topic %>%
#       select(all_of(topic_numeric_cols)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     # topic_numeric_features <- setdiff(names(topic_features), c("lyricsID", "spotify_id", "track_spotify_id"))
#     # topic_means_raw <- matched_topic %>%
#     #   select(any_of(topic_numeric_features)) %>%
#     #   dplyr::summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     topic_means <- safe_summarise(topic_means_raw, topic_numeric_features, fill = NA_real_, prefix = "topic_")
# 
#     # LIWC
#     matched_liwc <- left_join(matched_tracks, liwc_features, by = "track_spotify_id")
#     liwc_numeric_cols <- names(Filter(is.numeric, matched_liwc))
#     liwc_means_raw <- matched_liwc %>%
#       select(all_of(liwc_numeric_cols)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     # liwc_numeric_features <- setdiff(names(liwc_features), c("lyricsID", "spotify_id", "track_spotify_id"))
#     # liwc_means_raw <- matched_liwc %>%
#     #   select(any_of(liwc_numeric_features)) %>%
#     #   dplyr::summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     liwc_means <- safe_summarise(liwc_means_raw, liwc_numeric_features, fill = NA_real_, prefix = "liwc_")
# 
#   } else {
#     message(sprintf("Skipping User %s: missing music details", user_id))
#     return(NULL)
#   }
# 
#   # --- 10. Combine All Features ---
#   final_features <- tibble(
#     user_id = user_id,
#     start_time = start_time,
#     end_time = end_time,
#     time_bin = time_bin,
#     gps_count = gps_count,
#     gps_first_latitude = gps_first_latitude,
#     gps_first_longitude = gps_first_longitude,
#     gps_landuse_type = gps_landuse_type,
#     gps_home = gps_home,
#     gps_work = gps_work
#   ) %>%
#     bind_cols(headset_summary) %>%
#     bind_cols(detected_activity_summary) %>%
#     bind_cols(activity_summary) %>%
#     bind_cols(app_usage_summary) %>%
#     bind_cols(screen_state_summary) %>%
#     bind_cols(spotify_means) %>%
#     bind_cols(genius_means) %>%
#     bind_cols(topic_means) %>%
#     bind_cols(liwc_means)
# 
#   return(final_features)
# }
# 
# # All possible prefixed column names
# fixed_column_names <- c(
#   "user_id", "start_time", "end_time", "time_bin",
#   paste0("headset_", headset_states),
#   paste0("detected_", detected_activity_keys),
#   paste0("phone_", all_activity_names),
#   "gps_count", "gps_first_latitude", "gps_first_longitude",
#   "gps_landuse_type",
#   "gps_home", "gps_work",
#   paste0("app_", app_categories),
#   paste0("screen_", screen_categories),
#   paste0("music_", spotify_numeric_features),
#   paste0("genius_", genius_numeric_features),
#   paste0("topic_", topic_numeric_features),
#   paste0("liwc_", liwc_numeric_features)
# )
# 
# # --- Set offset and output file ---
# offset <- 0
# output_file <- "data/results/features_all_windows.rds"
# 
# # --- Load DB credentials ---
# invisible(capture.output(source('/local/.meta/dbcredentials.R')))
# 
# # --- Determine unprocessed windows ---
# window_indices <- seq((offset + 1), nrow(music_windows))
# 
# # Initialize list to store all features
# all_features_list <- vector("list", length(window_indices))
# 
# # --- Loop over windows ---
# for (i in window_indices) {
#   row <- music_windows[i, ]
#   
#   features <- extract_features_for_window(
#     user_id = row$user_id,
#     start_time = row$start_time,
#     end_time = row$end_time,
#     con = con,
#     snapshot = snapshot,
#     music = music
#   )
#   
#   if (!is.null(features)) {
#     features_ordered <- features %>% select(all_of(fixed_column_names))
#     all_features_list[[i]] <- features_ordered
#     message(sprintf("✓ Window %d processed (User %s)", i, row$user_id))
#   } else {
#     all_features_list[[i]] <- NULL  # skip row
#   }
# }
# 
# # --- Combine all features and save ---
# all_features <- bind_rows(all_features_list)
# saveRDS(all_features, output_file)
# 
# message("Feature extraction completed. Saved to RDS.")






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
all_activity_names <- c("AIRPLANE", "APPS", "BATTERYSAVINGMODE", "BLUETOOTH", "CAMERA", "GPS", "NOTIFICATION", "PHONE", "POWER", "SCREEN", "SMS")

spotify_numeric_features <- c("track_danceability", "track_energy", "track_key", "track_loudness", "track_mode_major", "track_speechiness", "track_acousticness", "track_instrumentalness", "track_liveness", "track_valence", "track_tempo", "track_non_music")
genius_numeric_features <- c("fear", "anger", "trust", "surprise", "positive", "negative", "sadness", "disgust", "joy", "anticipation", "lyric_len")
topic_numeric_features <- paste0("Topic.", 0:29)
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
  # t0 <- Sys.time()
  
  # # Helper for timing blocks
  # time_log <- function(label, start_t) {
  #   dt <- round(as.numeric(difftime(Sys.time(), start_t, units = "secs")), 3)
  #   message(sprintf("⏱ %s took %.3f sec", label, dt))
  # }
  
  # --- 1. Filter activity data ---
  # t1 <- Sys.time()
  acts <- activity_data_raw[user_id == user_id & timestamp %between% c(start_time, end_time)]
  # time_log("Activity filtering", t1)
  if (nrow(acts) == 0) return(NULL)
  
  # --- 2. Time Bin ---
  # t1 <- Sys.time()
  time_bin <- get_time_bin(start_time)
  # time_log("Time bin assignment", t1)
  
  # --- 3. GPS / Landuse ---
  # t1 <- Sys.time()
  gps_row <- gps_landuse_df[user_id == user_id & start_time == start_time & end_time == end_time]
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
    gps_features <- gps_row[, .(
      gps_count,
      gps_first_latitude,
      gps_first_longitude,
      gps_landuse_type,
      gps_home,
      gps_work
    )]
  }
  # time_log("GPS features", t1)
  
  # --- 4. Snapshot-based Features ---
  # t1 <- Sys.time()
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
    detected_activity_summary <- snapshot_activities[
      J(snapshot_ids),                              # keyed join for efficiency
      lapply(.SD, sum, na.rm = TRUE),
      .SDcols = detected_activity_keys,
      nomatch = 0L
    ]
  } else {
    detected_activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(detected_activity_keys)))
    setnames(detected_activity_summary, detected_activity_keys)
  }
  detected_activity_summary <- ensure_cols(detected_activity_summary, detected_activity_keys, fill = 0)
  setnames(detected_activity_summary, names(detected_activity_summary), paste0("detected_", names(detected_activity_summary)))
  # time_log("Snapshot-based features", t1)
  
  # --- 5. Phone Activities ---
  # t1 <- Sys.time()
  if (nrow(acts) == 0) {
    activity_summary <- data.table(matrix(0, nrow = 1, ncol = length(all_activity_names)))
    setnames(activity_summary, all_activity_names)
  } else {
    tmp_act <- acts[, .N, by = activityName]
    activity_summary <- dcast(tmp_act, . ~ activityName, value.var = "N", fill = 0)[, . := NULL]
    activity_summary <- ensure_cols(activity_summary, all_activity_names, fill = 0)
  }
  setnames(activity_summary, names(activity_summary), paste0("phone_", names(activity_summary)))
  # time_log("Phone activities", t1)
  
  # --- 6. App Usage ---
  # t1 <- Sys.time()
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
  # time_log("App usage", t1)
  
  # --- 7. Screen State ---
  # t1 <- Sys.time()
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
  # time_log("Screen state", t1)
  
  # --- 8. Music Features (Corrected & Robust) ---
  # t1 <- Sys.time()
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
  setnames(genius_means,  paste0("genius_", names(genius_means)))
  setnames(topic_means,   paste0("topic_", names(topic_means)))
  setnames(liwc_means,    paste0("liwc_", names(liwc_means)))
  # time_log("Music features", t1)
  
  # --- 9. Combine All Features ---
  # t1 <- Sys.time()
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
  # time_log("Final combine", t1)
  # 
  # total_dt <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 3)
  # message(sprintf("✅ Window processed for user %s (%.3f sec total)", user_id, total_dt))
  
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
output_file <- "data/results/features_all_windows_optimized.rds"
window_indices <- seq((offset + 1), nrow(music_windows))

message(sprintf("Starting sequential feature extraction for %d windows...", length(window_indices)))

# --- Load previous intermediate results if any ---
intermediate_files <- list.files("data/results", pattern = "^features_partial_\\d+\\.rds$", full.names = TRUE)
all_features_list <- vector("list", length(window_indices))

if (length(intermediate_files) > 0) {
  message("🔄 Found intermediate files, resuming from previous progress...")
  # Load the most recent one
  latest_file <- intermediate_files[which.max(as.numeric(gsub(".*features_partial_(\\d+)\\.rds", "\\1", intermediate_files)))]
  prev_features <- readRDS(latest_file)
  all_features_list[seq_along(prev_features)] <- prev_features
  start_index <- sum(sapply(prev_features, function(x) !is.null(x))) + 1
  message(sprintf("✅ Resuming from window %d", start_index))
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
  
  all_features_list[[i]] <- features
  
  # Progress indicator every N windows
  if (i %% 50 == 0 || i == length(window_indices)) {
    elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
    pct <- round(100 * i / length(window_indices), 1)
    message(sprintf("✓ Processed %d / %d windows (%.1f%%) — Elapsed: %.1f min",
                    i, length(window_indices), pct, as.numeric(elapsed)))
    
    # Intermediate save every 500 windows
    if (i %% 500 == 0 || i == length(window_indices)) {
      tmp_file <- sprintf("data/results/features_partial_%d.rds", i)
      saveRDS(all_features_list, tmp_file)
      message(sprintf("💾 Saved intermediate results to %s", tmp_file))
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
message(sprintf("✅ Feature extraction completed. Saved %d rows to %s (%.1f min total)",
                nrow(all_features), output_file, as.numeric(total_elapsed)))










# extract_features_for_window <- function(user_id, start_time, end_time, con, snapshot, music) {
#   # Filter activity data for user and time window
#   acts <- activity_data %>%
#     filter(
#       user_id == !!user_id,
#       timestamp >= !!start_time,
#       timestamp <= !!end_time
#     )
# 
#   if (nrow(acts) == 0) {
#     message(sprintf("User %s has no activity data in %s to %s", user_id, format(start_time, "%Y-%m-%d %H:%M:%S"), format(end_time, "%Y-%m-%d %H:%M:%S")))
#     return(NULL)
#   }
#   acts <- acts %>% mutate(timestamp = as.POSIXct(timestamp, tz = "UTC"))
# 
#   # === Time Bin Summary ===
#   time_bin <- get_time_bin(start_time, end_time)
# 
#   # === GPS / Landuse summary: use precomputed table ===
#   gps_row <- gps_landuse_df %>%
#     filter(user_id == !!user_id,
#            start_time == !!start_time,
#            end_time == !!end_time)
# 
#   if (nrow(gps_row) == 0) {
#     # fallback if not found
#     gps_count <- 0
#     gps_first_latitude <- NA_real_
#     gps_first_longitude <- NA_real_
#     gps_landuse_type <- "Unknown"
#     gps_home <- 0
#     gps_work <- 0
#   } else {
#     gps_count <- gps_row$gps_count[1]
#     gps_first_latitude <- gps_row$gps_first_latitude[1]
#     gps_first_longitude <- gps_row$gps_first_longitude[1]
#     gps_landuse_type <- gps_row$gps_landuse_type[1]
#     gps_home <- gps_row$gps_home[1]
#     gps_work <- gps_row$gps_work[1]
#   }
# 
#   # === Headset State ===
#   snapshot_ids <- unique(acts$snapshot_id)
#   snapshot_info <- snapshot %>% filter(id %in% snapshot_ids)
# 
#   headset_summary <- snapshot_info %>%
#     group_by(headsetState) %>%
#     summarise(count = n(), .groups = "drop") %>%
#     pivot_wider(names_from = headsetState, values_from = count, values_fill = 0)
#   headset_summary <- safe_summarise(headset_summary, headset_states, prefix = "headset_", fill = 0)
# 
#   # === Detected Activities ===
#   detected_activity_summary <- snapshot_info %>%
#     pull(detectedActivities) %>%
#     discard(is.na) %>%
#     map(~ fromJSON(.) %>% as_tibble()) %>%
#     bind_rows()
#   if (nrow(detected_activity_summary) > 0) {
#     detected_activity_summary <- detected_activity_summary %>%
#       summarise(across(everything(), sum, na.rm = TRUE))
#   }
#   detected_activity_summary <- safe_summarise(detected_activity_summary, detected_activity_keys, prefix = "detected_", fill = 0)
# 
#   # === Phone Activities ===
#   activity_summary <- acts %>%
#     group_by(activityName) %>%
#     summarise(count = n(), .groups = "drop") %>%
#     pivot_wider(names_from = activityName, values_from = count, values_fill = 0) %>%
#     mutate(across(everything(), as.numeric))
#   activity_summary <- safe_summarise(activity_summary, all_activity_names, prefix = "phone_", fill = 0)
# 
#   # === App Usage ===
#   app_usage_summary <- acts %>%
#     filter(!is.na(packageName)) %>%
#     left_join(app_categorization, by = "packageName") %>%
#     mutate(Final_Rating = ifelse(is.na(Final_Rating), "Unknown", Final_Rating)) %>%
#     count(Final_Rating, name = "count") %>%
#     pivot_wider(names_from = Final_Rating, values_from = count, values_fill = 0)
#   app_usage_summary <- safe_summarise(app_usage_summary, app_categories, prefix = "app_", fill = 0)
# 
#   # === Screen State ===
#   screen_state_durations <- preprocessing_screen_window(acts, start_time, end_time)
#   if (is.null(screen_state_durations) || nrow(screen_state_durations) == 0) {
#     screen_state_summary <- safe_summarise(tibble(), screen_categories, prefix = "screen_", fill = 0)
#   } else {
#     screen_state_summary <- as_tibble(screen_state_durations) %>%
#       rename(screen_event = event, screen_duration_sec = duration_sec) %>%
#       pivot_wider(names_from = screen_event, values_from = screen_duration_sec, values_fill = 0) %>%
#       safe_summarise(screen_categories, prefix = "screen_", fill = 0)
#   }
# 
#   # === Spotify Feature Aggregation from Preloaded music ===
#   music_events <- acts %>%
#     filter(activityName == "MUSIC" & !is.na(music_id)) %>%
#     select(music_id)
# 
#   if (nrow(music_events) > 0) {
#     music_window <- music %>%
#       filter(id %in% music_events$music_id) %>% # this long
#       select(title, artist, album, duration, id) %>%
#       distinct()
# 
#     matched_tracks <- left_join(
#       music_window,
#       spotify_data,
#       by = c("title" = "track", "artist" = "artist")
#     )
# 
#     spotify_means_raw <- matched_tracks %>%
#       select(any_of(spotify_numeric_features)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
#     
#     if (nrow(spotify_means_raw) == 0 || all(is.na(spotify_means_raw))) {
#       message(sprintf("User %s Spotify data matched, but no numeric features available.", user_id))
#       return(NULL)
#     }
# 
#     spotify_means <- safe_summarise(spotify_means_raw, spotify_numeric_features, fill = NA_real_, prefix = "music_")
#     
#     # === Prepare Genius, Topic and LIWC features with spotify_id ===
#     genius_features <- genius_features %>%
#       mutate(track_spotify_id = sub("lyrics:", "", lyricsID))
# 
#     topic_features <- topic_features %>%
#       mutate(track_spotify_id = sub("lyrics:", "", lyricsID))
# 
#     liwc_features <- liwc_features %>%
#       mutate(track_spotify_id = sub("lyrics:", "", key))
# 
#     # === Match with Genius features ===
#     matched_genius <- left_join(
#       matched_tracks,
#       genius_features,
#       by = "track_spotify_id"
#     )
# 
#     genius_numeric_features <- setdiff(names(genius_features), c("lyricsID", "spotify_id"))
# 
#     genius_means_raw <- matched_genius %>%
#       select(any_of(genius_numeric_features)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
# 
#     genius_means <- safe_summarise(
#       genius_means_raw,
#       genius_numeric_features,
#       fill = NA_real_,
#       prefix = "genius_"
#     )
# 
#     # === Match with Topic features ===
#     matched_topic <- left_join(
#       matched_tracks,
#       topic_features,
#       by = "track_spotify_id"
#     )
# 
#     topic_numeric_features <- setdiff(names(topic_features), c("lyricsID", "spotify_id"))
# 
#     topic_means_raw <- matched_topic %>%
#       select(any_of(topic_numeric_features)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
# 
#     topic_means <- safe_summarise(
#       topic_means_raw,
#       topic_numeric_features,
#       fill = NA_real_,
#       prefix = "topic_"
#     )
# 
#     # === Match with LIWC features ===
#     matched_liwc <- left_join(
#       matched_tracks,
#       liwc_features,
#       by = "track_spotify_id"
#     )
# 
#     liwc_numeric_features <- setdiff(names(liwc_features), c("lyricsID", "spotify_id"))
# 
#     liwc_means_raw <- matched_liwc %>%
#       select(any_of(liwc_numeric_features)) %>%
#       summarise(across(everything(), ~ mean(.x, na.rm = TRUE)))
# 
#     liwc_means <- safe_summarise(
#       liwc_means_raw,
#       names(liwc_means_raw),
#       fill = NA_real_,
#       prefix = "liwc_"
#     )
# 
#   } else {
#     message(sprintf("User %s has no music data available.", user_id))
#     return(NULL)
#   }
# 
#   # === Combine All Features ===
#   final_features <- tibble(
#     user_id = user_id,
#     start_time = start_time,
#     end_time = end_time,
#     time_bin = time_bin,
#     gps_count = gps_summary$gps_count[1],
#     gps_first_latitude = gps_summary$first_latitude[1],
#     gps_first_longitude = gps_summary$first_longitude[1],
#     gps_landuse_type = landuse_type_val,
#     gps_home = gps_home,
#     gps_work = gps_work
#   ) %>%
#     bind_cols(headset_summary) %>%
#     bind_cols(detected_activity_summary) %>%
#     bind_cols(activity_summary) %>%
#     bind_cols(app_usage_summary) %>%
#     bind_cols(screen_state_summary) %>%
#     bind_cols(spotify_means) %>%
#     bind_cols(genius_means) %>%
#     bind_cols(topic_means) %>%
#     bind_cols(liwc_means)
# 
#   return(final_features)
# }





# get_landuse_type <- function(lat, lon, buffer_m = 60) {
#   # Create point
#   point <- st_sfc(st_point(c(lon, lat)), crs = 4326)
# 
#   # Create buffer
#   buffer <- point %>%
#     st_transform(3857) %>%
#     st_buffer(buffer_m) %>%
#     st_transform(4326)
# 
#   # Bounding box
#   bbox <- st_bbox(buffer)
# 
#   # OSM query
#   query <- opq(bbox = bbox) %>%
#     add_osm_feature(key = "!null")
# 
#   osm <- tryCatch(osmdata_sf(query), error = function(e) return(NULL))
#   if (is.null(osm)) return("query_failed")
# 
#   # Combine features
#   all_features <- list(
#     osm$osm_points,
#     osm$osm_lines,
#     osm$osm_polygons,
#     osm$osm_multilines,
#     osm$osm_multipolygons
#   ) %>%
#     keep(~ !is.null(.) && nrow(.) > 0) %>%
#     bind_rows()
# 
#   if (nrow(all_features) == 0) return("no_features")
# 
#   # CRS align
#   point_transformed <- st_transform(point, st_crs(all_features))
# 
#   # Find containing features
#   contains_point <- st_contains(all_features, point_transformed, sparse = FALSE)[,1]
#   features_containing <- all_features[contains_point, ]
# 
#   if (nrow(features_containing) == 0) return("outside")
# 
#   # Rank by area
#   features_ranked <- features_containing %>%
#     mutate(
#       area = ifelse(
#         st_geometry_type(.) %in% c("POLYGON", "MULTIPOLYGON"),
#         as.numeric(st_area(st_transform(., 3857))),
#         NA_real_
#       )
#     ) %>%
#     arrange(area)  # smallest = finest
# 
#   # Prioritize tag keys
#   priority_keys <- c(
#     "amenity", "shop", "building", "landuse", "highway", "natural", "leisure",
#     "place", "tourism", "man_made", "office"
#   )
# 
#   # Loop through features from finest to coarsest
#   for (i in seq_len(nrow(features_ranked))) {
#     tags <- features_ranked[i, ] %>%
#       st_drop_geometry() %>%
#       select(where(~ any(!is.na(.) & . != "")))
# 
#     for (key in priority_keys) {
#       if (!is.null(tags[[key]]) && !is.na(tags[[key]]) && tags[[key]] != "") {
#         return(paste0(key, ":", tags[[key]]))
#       }
#     }
#   }
#   cat("could not classify!!")
#   return("unclassified")
# }

# get_landuse_type <- function(lat, lon, buffer_m = 60) {
#   # Temporarily disable s2 for invalid geometries
#   old_s2 <- sf::sf_use_s2(FALSE)
# 
#   on.exit(sf::sf_use_s2(old_s2))  # Restore s2 setting on function exit
# 
#   # Create point
#   point <- st_sfc(st_point(c(lon, lat)), crs = 4326)
# 
#   # Create buffer
#   buffer <- point %>%
#     st_transform(3857) %>%
#     st_buffer(buffer_m) %>%
#     st_transform(4326)
# 
#   # Bounding box
#   bbox <- st_bbox(buffer)
# 
#   # OSM query
#   query <- opq(bbox = bbox) %>%
#     add_osm_feature(key = "!null")
# 
#   osm <- tryCatch(osmdata_sf(query), error = function(e) return(NULL))
#   if (is.null(osm)) return("query_failed")
# 
#   # Combine features
#   all_features <- list(
#     osm$osm_points,
#     osm$osm_lines,
#     osm$osm_polygons,
#     osm$osm_multilines,
#     osm$osm_multipolygons
#   ) %>%
#     keep(~ !is.null(.) && nrow(.) > 0) %>%
#     bind_rows()
# 
#   if (nrow(all_features) == 0) return("no_features")
# 
#   # CRS align
#   point_transformed <- st_transform(point, st_crs(all_features))
# 
#   # Find containing features
#   contains_point <- st_contains(all_features, point_transformed, sparse = FALSE)[,1]
#   features_containing <- all_features[contains_point, ]
# 
#   if (nrow(features_containing) == 0) return("outside")
# 
#   # Rank by area
#   features_ranked <- features_containing %>%
#     mutate(
#       area = ifelse(
#         st_geometry_type(.) %in% c("POLYGON", "MULTIPOLYGON"),
#         as.numeric(st_area(st_transform(., 3857))),
#         NA_real_
#       )
#     ) %>%
#     arrange(area)  # smallest = finest
# 
#   # Prioritize tag keys
#   priority_keys <- c(
#     "amenity", "shop", "building", "landuse", "highway", "natural", "leisure",
#     "place", "tourism", "man_made", "office"
#   )
# 
#   # Loop through features from finest to coarsest
#   for (i in seq_len(nrow(features_ranked))) {
#     tags <- features_ranked[i, ] %>%
#       st_drop_geometry() %>%
#       select(where(~ any(!is.na(.) & . != "")))
# 
#     for (key in priority_keys) {
#       if (!is.null(tags[[key]]) && !is.na(tags[[key]]) && tags[[key]] != "") {
#         return(paste0(key, ":", tags[[key]]))
#       }
#     }
#   }
#   cat("could not classify!!\n")
#   return("unclassified")
# }

# query_activity <- sprintf("
#   SELECT *
#   FROM ps_activity
#   WHERE user_id = '%s'
#   AND timestamp >= '%s'
#   AND timestamp <= '%s'
# ",
#                           user_id,
#                           format(start_time, "%Y-%m-%d %H:%M:%S"),
#                           format(end_time, "%Y-%m-%d %H:%M:%S"))
# 
# acts <- dbGetQuery(con, query_activity)


# is_near_location <- function(user_lon, user_lat, ref_lon, ref_lat, tolerance_m = 15) {
#   if (any(is.na(c(user_lon, user_lat, ref_lon, ref_lat)))) return(FALSE)
#   
#   # Create sf points and transform to metric CRS (EPSG:3857)
#   user_sf <- st_sfc(st_point(c(user_lon, user_lat)), crs = 4326) %>%
#     st_transform(3857)
#   ref_sf <- st_sfc(st_point(c(ref_lon, ref_lat)), crs = 4326) %>%
#     st_transform(3857)
#   
#   # Calculate distance
#   distance <- st_distance(user_sf, ref_sf)
#   return(as.numeric(distance) <= tolerance_m)
# }



# # === GPS Summary ===
# gps_summary <- tibble(gps_count = 0, first_latitude = NA_real_, first_longitude = NA_real_)
# landuse_type_val <- "Unknown"
# location_ids <- unique(acts$location_id)
# gps_home <- 0
# gps_work <- 0
# if (length(location_ids) > 0) {
#   location_ids_str <- paste0("'", location_ids, "'", collapse = ",")
#   query_location <- sprintf("SELECT id, latitude, longitude FROM ps_location WHERE id IN (%s)", location_ids_str)
#   gps_points <- dbGetQuery(con, query_location)
#   if (nrow(gps_points) > 0) {
#     gps_summary <- tibble(
#       gps_count = nrow(gps_points),
#       first_latitude = gps_points$latitude[1],
#       first_longitude = gps_points$longitude[1]
#     )
#     if (!is.na(gps_summary$first_latitude) && !is.na(gps_summary$first_longitude)) {
#       # landuse_type_val <- get_landuse_type(gps_summary$first_latitude, gps_summary$first_longitude) # this takes long
#       time_taken <- system.time({
#         landuse_type_val <- get_landuse_type(gps_summary$first_latitude, gps_summary$first_longitude)
#       })
#       
#       print(paste0("get_landuse_type took ", round(time_taken["elapsed"], 2), " seconds"))
#       
#       # Home/work detection
#       user_home <- home_locations %>% filter(user_id == !!user_id)
#       user_work <- work_locations %>% filter(user_id == !!user_id)
#       gps_home <- as.integer(is_near_location(gps_summary$first_longitude, gps_summary$first_latitude, user_home$longitude[1], user_home$latitude[1]))
#       gps_work <- as.integer(is_near_location(gps_summary$first_longitude, gps_summary$first_latitude, user_work$longitude[1], user_work$latitude[1]))
#     }
#   }
# }





# # === Set offset for resuming ===
# offset <- 0  # Change this to a nonzero value to resume from that window index
# 
# # === Prepare output file ===
# output_file <- "data/results/features_per_music_window.csv"
# dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
# 
# if (offset == 0) {
#   # Initialize output with headers only
#   empty_header <- tibble::tibble(
#     !!!setNames(rep(list(NA), length(fixed_column_names)), fixed_column_names)
#   )
#   readr::write_csv(empty_header[0, ], output_file)
# }
# 
# # === Feature extraction loop with offset ===
# for (i in seq((offset + 1), nrow(music_windows))) {
#   row <- music_windows[i, ]
#   features <- extract_features_for_window(
#     user_id = row$user_id,
#     start_time = row$start_time,
#     end_time = row$end_time,
#     con = phonestudy,
#     snapshot = snapshot,
#     music = music
#   )
# 
#   if (!is.null(features)) {
#     features_ordered <- features %>% select(all_of(fixed_column_names))
# 
#     # Append results to file
#     write_csv(features_ordered, output_file, append = TRUE)
#     message(sprintf("Window %d of %d processed (User %s)", i, nrow(music_windows), row$user_id))
#   }
# }
# 
# message("Feature extraction completed and saved.")





# # --- Set offset and output directory ---
# offset <- 0
# output_dir <- "data/results/per_window_csvs"
# dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
# 
# # --- Load DB credentials ---
# invisible(capture.output(source('/local/.meta/dbcredentials.R')))
# 
# # --- Determine unprocessed windows ---
# window_indices <- seq((offset + 1), nrow(music_windows))
# 
# # Filter out already-processed files
# remaining_indices <- window_indices[!file.exists(file.path(
#   output_dir,
#   sprintf("features_user_%s_win_%d.csv", music_windows$user_id[window_indices], window_indices)
# ))]
# 
# # Exit early if everything is done
# if (length(remaining_indices) == 0) {
#   message("All windows already processed. Exiting.")
#   quit(save = "no")
# }
# 
# # Split remaining work across cores
# n_cores <- min(32, detectCores() - 1)
# chunks <- split(remaining_indices, cut(seq_along(remaining_indices), n_cores, labels = FALSE))
# 
# # --- Chunk processing function ---
# process_chunk <- function(indices) {
#   con <- dbConnect(
#     drv = RMariaDB::MariaDB(),
#     username = mariadb_user,
#     password = mariadb_pw,
#     host = "localhost",
#     port = 3306,
#     dbname = "live"
#   )
#   
#   on.exit(dbDisconnect(con), add = TRUE)
#   
#   for (i in indices) {
#     row <- music_windows[i, ]
#     
#     out_file <- file.path(output_dir, sprintf("features_user_%s_win_%d.csv", row$user_id, i))
#     if (file.exists(out_file)) {
#       next
#     }
#     
#     features <- tryCatch({
#       extract_features_for_window(
#         user_id = row$user_id,
#         start_time = row$start_time,
#         end_time = row$end_time,
#         con = con,
#         snapshot = snapshot,
#         music = music
#       )
#     }, error = function(e) {
#       message(sprintf("Error processing window %d (User %s): %s", i, row$user_id, e$message))
#       return(NULL)
#     })
#     
#     if (!is.null(features)) {
#       features_ordered <- features %>% select(all_of(fixed_column_names))
#       write_csv(features_ordered, out_file)
#       message(sprintf("✓ Window %d processed (User %s)", i, row$user_id))
#     }
#   }
#   
#   return(NULL)
# }
# 
# # --- Run in parallel ---
# mclapply(chunks, process_chunk, mc.cores = n_cores)
# 
# message("Feature extraction completed.")




# 
# # === Set offset for resuming ===
# offset <- 0  # Change this to a nonzero value to resume from that window index
# 
# # === Prepare output file ===
# output_file <- "data/results/features_per_music_window.csv"
# dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
# 
# if (offset == 0) {
#   # Initialize output with headers only
#   empty_header <- tibble::tibble(
#     !!!setNames(rep(list(NA), length(fixed_column_names)), fixed_column_names)
#   )
#   readr::write_csv(empty_header[0, ], output_file)
# }
# 
# options(future.globals.maxSize = 5 * 1024^3)  # 5 GiB
# 
# plan(multisession, workers = parallel::detectCores() - 1)
# 
# # Subset windows to process
# windows_to_process <- music_windows[(offset + 1):nrow(music_windows), ]
# 
# # Run in parallel and collect results
# features_list <- future_lapply(seq_len(nrow(windows_to_process)), function(idx) {
#   row <- windows_to_process[idx, ]
#   features <- extract_features_for_window(
#     user_id = row$user_id,
#     start_time = row$start_time,
#     end_time = row$end_time,
#     con = phonestudy,
#     snapshot = snapshot,
#     music = music
#   )
#   if (!is.null(features)) {
#     features_ordered <- features %>% select(all_of(fixed_column_names))
#     message(sprintf("Window %d of %d processed (User %s)", offset + idx, nrow(music_windows), row$user_id))
#     return(features_ordered)
#   } else {
#     return(NULL)
#   }
# })
# 
# # Remove NULL results
# features_list <- Filter(Negate(is.null), features_list)
# 
# # Combine all results into one tibble
# final_features <- dplyr::bind_rows(features_list)
# 
# # Append to CSV file
# if (nrow(final_features) > 0) {
#   readr::write_csv(final_features, output_file, append = TRUE)
# }
# 
# message("Feature extraction completed and saved.")
# 
# 
