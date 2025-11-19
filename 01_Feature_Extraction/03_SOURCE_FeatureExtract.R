# Feature Extraction Script
# This script has been refactored to use a modular structure for better maintainability
# For the new modular version, see: run_feature_extraction.R
#
# This file is kept for backward compatibility but now sources the modular components

# --- Load Required Libraries ---
library(DBI)
library(RMariaDB)
library(data.table)
library(lubridate)
library(jsonlite)
library(RcppSimdJson)
library(future)
library(future.apply)

# --- Source Configuration and Modules ---
source("config.R")
source("constants.R")
source("utils_feature_extraction.R")
source("data_loader.R")
source("feature_extractors.R")
source("feature_extraction_main.R")

# --- Configuration & Setup ---
# Set working directory (can be overridden)
if (file.exists("/home/clemensschwarzmann/music-situation-prediction/01_Feature_Extraction")) {
setwd("/home/clemensschwarzmann/music-situation-prediction/01_Feature_Extraction")
}

# Load screen preprocessing function
if (file.exists(PATHS$screen_utils)) {
  source(PATHS$screen_utils)
} else {
  warning("Screen preprocessing function not found at: ", PATHS$screen_utils)
  preprocessing_screen_window <- function(acts, start_time, end_time) {
    return(NULL)
  }
}

# --- Database Connection ---
message("Connecting to database...")
db_credentials <- load_db_credentials(DB_CREDENTIALS_PATH)
con <- connect_to_database(db_credentials, DB_HOST, DB_PORT, DB_NAME)
on.exit(DBI::dbDisconnect(con), add = TRUE)

# --- Load & Pre-process Data ---
message("Loading and pre-processing data...")

# Load raw data from database
db_data <- load_raw_database_data(con)
music_raw <- db_data$music_raw
snapshot_raw <- db_data$snapshot_raw

# Load helper data
helper_data <- load_helper_data(PATHS)
activity_data_raw <- helper_data$activity_data_raw
music_windows <- helper_data$music_windows
app_categorization <- helper_data$app_categorization
gps_landuse_df <- helper_data$gps_landuse_df
spotify_data <- helper_data$spotify_data
genius_features <- helper_data$genius_features
topic_features <- helper_data$topic_features
liwc_features <- helper_data$liwc_features

# Prepare data tables (convert to data.table and set keys)
all_data_list <- list(
  music_raw = music_raw,
  snapshot_raw = snapshot_raw,
  activity_data_raw = activity_data_raw,
  music_windows = music_windows,
  app_categorization = app_categorization,
  gps_landuse_df = gps_landuse_df,
  spotify_data = spotify_data,
  genius_features = genius_features,
  topic_features = topic_features,
  liwc_features = liwc_features
)

all_data_list <- prepare_data_tables(all_data_list)
music_raw <- all_data_list$music_raw
snapshot_raw <- all_data_list$snapshot_raw
activity_data_raw <- all_data_list$activity_data_raw
music_windows <- all_data_list$music_windows
app_categorization <- all_data_list$app_categorization
gps_landuse_df <- all_data_list$gps_landuse_df
spotify_data <- all_data_list$spotify_data
genius_features <- all_data_list$genius_features
topic_features <- all_data_list$topic_features
liwc_features <- all_data_list$liwc_features

# Preprocess snapshot activities
message("Pre-processing JSON activity data...")
snapshot_activities <- preprocess_snapshot_activities(snapshot_raw)

# Create master music features table
music_features_master <- create_music_features_master(
  spotify_data, genius_features, topic_features, liwc_features
)

# Initialize app categories
app_categories <- initialize_app_categories(app_categorization)

# --- Define Constants ---
headset_states <- HEADSET_STATES
screen_categories <- SCREEN_CATEGORIES
detected_activity_keys <- DETECTED_ACTIVITY_KEYS
all_activity_names <- ALL_ACTIVITY_NAMES
gps_features <- GPS_FEATURES
spotify_numeric_features <- SPOTIFY_NUMERIC_FEATURES
genius_numeric_features <- GENIUS_NUMERIC_FEATURES
topic_numeric_features <- TOPIC_NUMERIC_FEATURES
liwc_numeric_features <- LIWC_NUMERIC_FEATURES

# Pre-calculate numeric column names for music features
spotify_numeric_cols <- intersect(spotify_numeric_features, names(music_features_master))
genius_numeric_cols <- intersect(genius_numeric_features, names(music_features_master))
topic_numeric_cols <- intersect(topic_numeric_features, names(music_features_master))
liwc_numeric_cols <- intersect(liwc_numeric_features, names(music_features_master))

# --- Prepare Data Objects for Feature Extraction ---
data_objects <- list(
  activity_data_raw = activity_data_raw,
  gps_landuse_df = gps_landuse_df,
  snapshot_raw = snapshot_raw,
  snapshot_activities = snapshot_activities,
  app_categorization = app_categorization,
  music_raw = music_raw,
  music_features_master = music_features_master
)

constants_list <- list(
  app_categories = app_categories,
  headset_states = headset_states,
  screen_categories = screen_categories,
  detected_activity_keys = detected_activity_keys,
  all_activity_names = all_activity_names,
  gps_features = gps_features,
  spotify_numeric_features = spotify_numeric_features,
  genius_numeric_features = genius_numeric_features,
  topic_numeric_features = topic_numeric_features,
  liwc_numeric_features = liwc_numeric_features
)

# --- Main Execution: Parallel Processing with Resume Support ---
output_dir <- OUTPUT_CONFIG$output_dir
final_output_file <- OUTPUT_CONFIG$final_output_file
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

message(sprintf("Preparing to extract features for %d windows...", 
                nrow(music_windows)))

# Detect already processed windows
existing_files <- list.files(output_dir, pattern = "^window_\\d+\\.rds$", 
                             full.names = TRUE)
processed_indices <- as.integer(gsub(".*window_(\\d+)\\.rds", "\\1", existing_files))
remaining_indices <- setdiff(seq_len(nrow(music_windows)), processed_indices)

message(sprintf("Found %d processed windows, %d remaining to compute",
                length(processed_indices), length(remaining_indices)))

if (length(remaining_indices) == 0) {
  message("All windows already processed — skipping computation.")
} else {
  
  start_time_all <- Sys.time()
  
  # Sequential for loop
  for (i in remaining_indices) {
    row <- music_windows[i, ]
    
    out_file <- sprintf("%s/window_%05d.rds", output_dir, i)
    if (file.exists(out_file)) next  # Skip if already done
    
    start_time <- Sys.time()
    
    features <- tryCatch(
      extract_features_for_window(
        user_id = row$user_id,
        start_time = row$start_time,
        end_time = row$end_time,
        data_objects = data_objects,
        constants = constants_list,
        preprocessing_screen_window = preprocessing_screen_window,
        verbose = FALSE
      ),
      error = function(e) {
        message(sprintf("Error in window %d (%s - %s): %s",
                        i, row$start_time, row$end_time, e$message))
        return(NULL)
      }
    )
    
    if (!is.null(features) && nrow(features) == 1L) {
      saveRDS(features, out_file)
      elapsed <- round(as.numeric(difftime(Sys.time(), start_time, 
                                           units = "secs")), 2)
      if (i %% 50 == 0) {
        message(sprintf("Saved window %d (User %s) — %.2f sec", 
                        i, row$user_id, elapsed))
      }
    }
    
    gc()
  }
  
  total_elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
  message(sprintf("Extraction completed in %.1f minutes", 
                  as.numeric(total_elapsed)))
}

# if (length(remaining_indices) == 0) {
#   message("All windows already processed — skipping computation.")
# } else {
#   # Parallel setup
#   options(future.globals.maxSize = PROCESSING_CONFIG$future_globals_max_size)
#   plan(multisession, workers = PROCESSING_CONFIG$num_workers)
#   
#   # Run extraction in parallel
#   start_time_all <- Sys.time()
#   
#   future_lapply(remaining_indices, function(i) {
#     row <- music_windows[i, ]
#     
#     out_file <- sprintf("%s/window_%05d.rds", output_dir, i)
#     if (file.exists(out_file)) return(NULL)  # Skip if already done
#     
#     start_time <- Sys.time()
#     features <- tryCatch(
#       extract_features_for_window(
#         user_id = row$user_id,
#         start_time = row$start_time,
#         end_time = row$end_time,
#         data_objects = data_objects,
#         constants = constants_list,
#         preprocessing_screen_window = preprocessing_screen_window,
#         verbose = FALSE
#       ),
#       error = function(e) {
#         message(sprintf("Error in window %d (%s - %s): %s",
#                         i, row$start_time, row$end_time, e$message))
#         return(NULL)
#       }
#     )
#     
#     if (!is.null(features) && nrow(features) == 1L) {
#       saveRDS(features, out_file)
#       elapsed <- round(as.numeric(difftime(Sys.time(), start_time, 
#                                            units = "secs")), 2)
#       if (i %% 50 == 0) {
#         message(sprintf("Saved window %d (User %s) — %.2f sec", 
#                         i, row$user_id, elapsed))
#       }
#     }
#     gc()
#     NULL
#   })
#   
#   total_elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
#   message(sprintf("Parallel extraction completed in %.1f minutes", 
#                   as.numeric(total_elapsed)))
# }

# --- Combine All Per-Window Files ---
message("Combining all window results...")

all_files <- list.files(output_dir, pattern = "^window_\\d+\\.rds$", 
                        full.names = TRUE)
all_files <- all_files[order(as.numeric(
  gsub(".*window_(\\d+)\\.rds", "\\1", all_files)
))]

all_features_list <- lapply(all_files, function(f) {
  obj <- readRDS(f)
  if (is.null(obj) || nrow(obj) != 1L) return(NULL)
  as.data.table(obj)
})

valid_features <- Filter(Negate(is.null), all_features_list)

if (length(valid_features) == 0) {
  stop("No valid features found to combine!")
}

all_features <- rbindlist(valid_features, fill = TRUE)

# --- Ensure Consistent Column Order ---
fixed_column_names <- get_fixed_column_order(
  gps_features = constants_list$gps_features,
  app_categories = constants_list$app_categories,
  headset_states = constants_list$headset_states,
  detected_activity_keys = constants_list$detected_activity_keys,
  all_activity_names = constants_list$all_activity_names,
  screen_categories = constants_list$screen_categories,
  spotify_features = constants_list$spotify_numeric_features,
  genius_features = constants_list$genius_numeric_features,
  topic_features = constants_list$topic_numeric_features,
  liwc_features = constants_list$liwc_numeric_features
)

final_cols <- intersect(fixed_column_names, names(all_features))
all_features <- all_features[, ..final_cols]

# --- Save Final Combined Dataset ---
saveRDS(all_features, final_output_file)
message(sprintf("Combined and saved %d total rows to %s", 
                nrow(all_features), final_output_file))

message("Feature extraction completed successfully!")
