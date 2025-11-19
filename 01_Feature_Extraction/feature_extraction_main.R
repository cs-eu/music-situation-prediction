# Main feature extraction function
# This file contains the main orchestration function for extracting features

#' Extract all features for a single time window
#'
#' @param user_id User ID
#' @param start_time Window start time (POSIXct)
#' @param end_time Window end time (POSIXct)
#' @param data_objects List containing all required data objects
#' @param constants List containing all constant values
#' @param preprocessing_screen_window Function to preprocess screen window data
#' @param verbose Logical, whether to print timing messages
#' @return data.table with all features (one row) or NULL if no data
extract_features_for_window <- function(user_id, start_time, end_time,
                                        data_objects, constants,
                                        preprocessing_screen_window,
                                        verbose = TRUE) {
  time_log <- create_time_logger(verbose)
  t0 <- Sys.time()
  
  # Filter activity data
  t1 <- Sys.time()
  acts <- data_objects$activity_data_raw[
    user_id == user_id & timestamp %between% c(start_time, end_time)
  ]
  time_log("Activity filtering", t1)
  
  if (nrow(acts) == 0) {
    return(NULL)
  }
  
  # Time bin
  t1 <- Sys.time()
  time_bin <- get_time_bin(start_time)
  time_log("Time bin assignment", t1)
  
  # GPS features
  t1 <- Sys.time()
  gps_features <- extract_gps_features(
    user_id, start_time, end_time,
    data_objects$gps_landuse_df,
    constants$gps_features
  )
  time_log("GPS features", t1)
  
  # Snapshot-based features
  t1 <- Sys.time()
  snapshot_ids <- unique(as.integer(acts$snapshot_id))
  snapshot_info <- data_objects$snapshot_raw[
    J(snapshot_ids),
    nomatch = 0L,
    on = "id"
  ]
  
  headset_summary <- extract_headset_features(
    snapshot_info,
    constants$headset_states
  )
  
  detected_activity_summary <- extract_detected_activity_features(
    snapshot_ids,
    data_objects$snapshot_activities,
    constants$detected_activity_keys
  )
  time_log("Snapshot-based features", t1)
  
  # Phone activities
  t1 <- Sys.time()
  activity_summary <- extract_phone_activity_features(
    acts,
    constants$all_activity_names
  )
  time_log("Phone activities", t1)
  
  # App usage
  t1 <- Sys.time()
  app_usage_summary <- extract_app_usage_features(
    acts,
    data_objects$app_categorization,
    constants$app_categories
  )
  time_log("App usage", t1)
  
  # Screen state
  t1 <- Sys.time()
  screen_state_summary <- extract_screen_state_features(
    acts,
    start_time,
    end_time,
    constants$screen_categories,
    preprocessing_screen_window
  )
  time_log("Screen state", t1)
  
  # Music features
  t1 <- Sys.time()
  music_features <- extract_music_features(
    acts,
    data_objects$music_raw,
    data_objects$music_features_master,
    constants$spotify_numeric_features,
    constants$genius_numeric_features,
    constants$topic_numeric_features,
    constants$liwc_numeric_features
  )
  time_log("Music features", t1)
  
  if (is.null(music_features)) {
    return(NULL)
  }
  
  # Combine all features
  t1 <- Sys.time()
  
  # Validate feature tables
  feature_tables <- list(
    gps = gps_features,
    headset = headset_summary,
    detected_activity = detected_activity_summary,
    phone_activity = activity_summary,
    app_usage = app_usage_summary,
    screen_state = screen_state_summary
  )
  
  for (tbl_name in names(feature_tables)) {
    validate_feature_table(
      tbl_name,
      feature_tables[[tbl_name]],
      user_id,
      start_time
    )
  }
  
  time_log("Feature table validation", t1)
  
  final_features <- cbind(
    data.table(
      user_id = user_id,
      start_time = start_time,
      end_time = end_time,
      time_bin = time_bin
    ),
    gps_features,
    headset_summary,
    detected_activity_summary,
    activity_summary,
    app_usage_summary,
    screen_state_summary,
    music_features$spotify,
    music_features$genius,
    music_features$topic,
    music_features$liwc
  )
  
  time_log("Final combine", t1)
  
  if (verbose) {
    total_dt <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 3)
    message(sprintf(
      "Window processed for user %s (%.3f sec total)",
      user_id, total_dt
    ))
  }
  
  return(final_features)
}

