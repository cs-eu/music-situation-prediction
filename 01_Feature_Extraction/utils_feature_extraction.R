# Utility functions for feature extraction
# This file contains helper functions used throughout the feature extraction process

#' Get time bin based on hour of day
#'
#' @param start_time POSIXct timestamp
#' @return Character string indicating time bin
get_time_bin <- function(start_time) {
  h <- lubridate::hour(start_time)
  if (h >= 5 && h < 8) return("Morning")
  if (h >= 8 && h < 11) return("Late Morning")
  if (h >= 11 && h < 13) return("Noon")
  if (h >= 13 && h < 17) return("Afternoon")
  if (h >= 17 && h < 21) return("Evening")
  return("Night")
}

#' Ensure all specified columns exist in a data.table
#'
#' @param dt data.table to check
#' @param col_names Character vector of column names that should exist
#' @param fill Value to use for missing columns (default: 0)
#' @return data.table with all specified columns
ensure_cols <- function(dt, col_names, fill = 0) {
  missing_cols <- setdiff(col_names, names(dt))
  if (length(missing_cols) > 0) {
    dt[, (missing_cols) := fill]
  }
  return(dt)
}

#' Create empty summary data.table with specified columns
#'
#' @param col_names Character vector of column names
#' @param fill Value to fill the table with (default: 0)
#' @return data.table with one row and specified columns
create_empty_summary <- function(col_names, fill = 0) {
  dt <- data.table(matrix(fill, nrow = 1, ncol = length(col_names)))
  data.table::setnames(dt, col_names)
  return(dt)
}

#' Create summary table from counts with all required columns
#'
#' @param dt data.table with grouping variable and count
#' @param value_var Name of the count column
#' @param required_cols Character vector of all columns that should be present
#' @param fill Value to use for missing categories (default: 0)
#' @return data.table with one row and all required columns
create_count_summary <- function(dt, value_var, required_cols, fill = 0) {
  if (nrow(dt) == 0) {
    return(create_empty_summary(required_cols, fill))
  }
  
  summary_dt <- data.table::dcast(
    dt, . ~ get(names(dt)[1]),
    value.var = value_var,
    fill = fill
  )[, . := NULL]
  
  summary_dt <- ensure_cols(summary_dt, required_cols, fill)
  return(summary_dt)
}

#' Create timing log function
#'
#' @param verbose Logical, whether to print timing messages
#' @return Function that logs timing information
create_time_logger <- function(verbose = TRUE) {
  function(label, start_t) {
    if (!verbose) return(invisible(NULL))
    dt <- round(as.numeric(difftime(Sys.time(), start_t, units = "secs")), 3)
    message(sprintf("⏱ %s took %.3f sec", label, dt))
  }
}

#' Validate feature table has exactly one row
#'
#' @param tbl_name Name of the table (for error messages)
#' @param dt data.table to validate
#' @param user_id User ID (for error messages)
#' @param start_time Start time (for error messages)
#' @return Logical indicating if validation passed
validate_feature_table <- function(tbl_name, dt, user_id, start_time) {
  if (nrow(dt) != 1) {
    warning(sprintf(
      "⚠️ VALIDATION FAILED: Feature table '%s' for user %s at %s has %d rows (expected 1).",
      tbl_name, user_id, start_time, nrow(dt)
    ))
    return(FALSE)
  }
  return(TRUE)
}

#' Get column order for final output
#'
#' @param gps_features Character vector of GPS feature names
#' @param app_categories Character vector of app categories
#' @param headset_states Character vector of headset states
#' @param detected_activity_keys Character vector of detected activity keys
#' @param all_activity_names Character vector of all activity names
#' @param screen_categories Character vector of screen categories
#' @param spotify_features Character vector of Spotify feature names
#' @param genius_features Character vector of Genius feature names
#' @param topic_features Character vector of topic feature names
#' @param liwc_features Character vector of LIWC feature names
#' @return Character vector of column names in desired order
get_fixed_column_order <- function(gps_features, app_categories,
                                   headset_states, detected_activity_keys,
                                   all_activity_names, screen_categories,
                                   spotify_features, genius_features,
                                   topic_features, liwc_features) {
  c(
    "user_id", "start_time", "end_time", "time_bin",
    gps_features,
    paste0("headset_", headset_states),
    paste0("detected_", detected_activity_keys),
    paste0("phone_", all_activity_names),
    paste0("app_", app_categories),
    paste0("screen_", screen_categories),
    paste0("music_", spotify_features),
    paste0("genius_", genius_features),
    paste0("topic_", gsub("\\s+", "_", trimws(topic_features))),
    paste0("liwc_", liwc_features)
  )
}

