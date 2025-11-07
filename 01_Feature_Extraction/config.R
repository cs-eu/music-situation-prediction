# Configuration file for feature extraction
# This file contains all configuration settings that may vary across environments

# --- Directory Paths ---
BASE_DIR <- getwd()
DATA_DIR <- file.path(BASE_DIR, "data")
HELPER_DIR <- file.path(DATA_DIR, "helper")
RESULTS_DIR <- file.path(DATA_DIR, "results")
UTILS_DIR <- file.path(BASE_DIR, "utils")

# --- Database Configuration ---
# Database credentials should be loaded from a secure location
DB_CREDENTIALS_PATH <- '/local/.meta/dbcredentials.R'
DB_HOST <- "localhost"
DB_PORT <- 3306
DB_NAME <- "live"

# --- Data File Paths ---
PATHS <- list(
  activity_data = file.path(HELPER_DIR, "combined_activity_data.rds"),
  music_windows = file.path(RESULTS_DIR, "music_windows_all.csv"),
  app_categorization = file.path(HELPER_DIR, "app_categorisation_2020_v2.csv"),
  gps_landuse = file.path(RESULTS_DIR, "gps_landuse_by_window.rds"),
  spotify_data = file.path(HELPER_DIR, "distinct_tracks_enriched.rds"),
  genius_features = file.path(HELPER_DIR, "lyrics-features", "genius_features.csv"),
  topic_features = file.path(HELPER_DIR, "lyrics-features", "topic_features.csv"),
  liwc_features = file.path(HELPER_DIR, "lyrics-features", "Lyrics_LIWC.csv"),
  screen_utils = file.path(UTILS_DIR, "label_usage_sessions.R")
)

# --- Output Configuration ---
OUTPUT_CONFIG <- list(
  output_dir = file.path(RESULTS_DIR, "new"),
  output_file_pattern = "window_%05d.rds",
  final_output_file = file.path(RESULTS_DIR, "new", "features_all_windows_combined.rds")
)

# --- Processing Configuration ---
PROCESSING_CONFIG <- list(
  future_globals_max_size = 8 * 1024^3,  # 8 GB
  progress_report_interval = 50,  # Report progress every N windows
  intermediate_save_interval = 500,  # Save intermediate results every N windows
  num_workers = max(1, parallel::detectCores() - 1)
)

# --- Helper function to validate paths ---
validate_paths <- function() {
  required_dirs <- c(DATA_DIR, HELPER_DIR, RESULTS_DIR, UTILS_DIR)
  missing_dirs <- required_dirs[!dir.exists(required_dirs)]
  if (length(missing_dirs) > 0) {
    warning("The following directories do not exist: ", paste(missing_dirs, collapse = ", "))
  }
}

