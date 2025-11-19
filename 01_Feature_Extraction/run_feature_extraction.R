# Main execution script for feature extraction
# This script orchestrates the entire feature extraction process

# --- 1. Load Required Libraries ---
library(DBI)
library(RMariaDB)
library(data.table)
library(lubridate)
library(jsonlite)
library(RcppSimdJson)
library(future)
library(future.apply)

# --- 2. Source Configuration and Modules ---
source("config.R")
source("constants.R")
source("utils_feature_extraction.R")
source("data_loader.R")
source("feature_extractors.R")
source("feature_extraction_main.R")

# Load screen preprocessing function if available
if (file.exists(PATHS$screen_utils)) {
  source(PATHS$screen_utils)
} else {
  warning("Screen preprocessing function not found. Screen features may not work correctly.")
  preprocessing_screen_window <- function(acts, start_time, end_time) {
    return(NULL)
  }
}

# --- 3. Initialize Configuration ---
validate_paths()

# --- 4. Load Database Credentials and Connect ---
message("Connecting to database...")
db_credentials <- load_db_credentials(DB_CREDENTIALS_PATH)
con <- connect_to_database(
  db_credentials,
  DB_HOST,
  DB_PORT,
  DB_NAME
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

# --- 5. Load All Data ---
db_data <- load_raw_database_data(con)
helper_data <- load_helper_data(PATHS)

# Combine all data
all_data <- c(db_data, helper_data)
all_data <- prepare_data_tables(all_data)

# Preprocess snapshot activities
all_data$snapshot_activities <- preprocess_snapshot_activities(all_data$snapshot_raw)

# Create music features master table
all_data$music_features_master <- create_music_features_master(
  all_data$spotify_data,
  all_data$genius_features,
  all_data$topic_features,
  all_data$liwc_features
)

# Initialize app categories
app_categories <- initialize_app_categories(all_data$app_categorization)

# --- 6. Prepare Constants List ---
constants_list <- list(
  app_categories = app_categories,
  headset_states = HEADSET_STATES,
  screen_categories = SCREEN_CATEGORIES,
  detected_activity_keys = DETECTED_ACTIVITY_KEYS,
  all_activity_names = ALL_ACTIVITY_NAMES,
  gps_features = GPS_FEATURES,
  spotify_numeric_features = SPOTIFY_NUMERIC_FEATURES,
  genius_numeric_features = GENIUS_NUMERIC_FEATURES,
  topic_numeric_features = TOPIC_NUMERIC_FEATURES,
  liwc_numeric_features = LIWC_NUMERIC_FEATURES
)

# --- 7. Main Execution: Parallel Processing ---
message(sprintf("Preparing to extract features for %d windows...", 
                nrow(all_data$music_windows)))

# Create output directory
dir.create(OUTPUT_CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

# Detect already processed windows
existing_files <- list.files(
  OUTPUT_CONFIG$output_dir,
  pattern = "^window_\\d+\\.rds$",
  full.names = TRUE
)
processed_indices <- as.integer(
  gsub(".*window_(\\d+)\\.rds", "\\1", existing_files)
)
remaining_indices <- setdiff(seq_len(nrow(all_data$music_windows)), 
                             processed_indices)

message(sprintf("Found %d processed windows, %d remaining to compute",
                length(processed_indices), length(remaining_indices)))

if (length(remaining_indices) == 0) {
  message("All windows already processed — skipping computation.")
} else {
  # Parallel setup
  options(future.globals.maxSize = PROCESSING_CONFIG$future_globals_max_size)
  plan(multisession, workers = PROCESSING_CONFIG$num_workers)
  
  # Run extraction in parallel
  start_time_all <- Sys.time()
  
  future_lapply(remaining_indices, function(i) {
    row <- all_data$music_windows[i, ]
    
    out_file <- sprintf("%s/window_%05d.rds", OUTPUT_CONFIG$output_dir, i)
    if (file.exists(out_file)) return(NULL)  # Skip if already done
    
    start_time <- Sys.time()
    features <- tryCatch(
      extract_features_for_window(
        user_id = row$user_id,
        start_time = row$start_time,
        end_time = row$end_time,
        data_objects = all_data,
        constants = constants_list,
        preprocessing_screen_window = preprocessing_screen_window,
        verbose = FALSE  # Less verbose in parallel mode
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
    NULL
  })
  
  total_elapsed <- difftime(Sys.time(), start_time_all, units = "mins")
  message(sprintf("Parallel extraction completed in %.1f minutes", 
                  as.numeric(total_elapsed)))
}

# --- 8. Combine All Per-Window Files ---
message("Combining all window results...")

all_files <- list.files(
  OUTPUT_CONFIG$output_dir,
  pattern = "^window_\\d+\\.rds$",
  full.names = TRUE
)
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

# --- 9. Ensure Consistent Column Order ---
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

# --- 10. Save Final Combined Dataset ---
saveRDS(all_features, OUTPUT_CONFIG$final_output_file)
message(sprintf("Combined and saved %d total rows to %s", 
                nrow(all_features), OUTPUT_CONFIG$final_output_file))

message("Feature extraction completed successfully!")

