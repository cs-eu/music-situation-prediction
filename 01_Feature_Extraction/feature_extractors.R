# Feature extraction modules
# This file contains modular functions for extracting different types of features

#' Extract GPS features for a time window
#'
#' @param user_id User ID
#' @param start_time Window start time
#' @param end_time Window end time
#' @param gps_landuse_df data.table with GPS landuse data
#' @param gps_feature_names Character vector of GPS feature names
#' @return data.table with GPS features (one row)
extract_gps_features <- function(user_id, start_time, end_time,
                                 gps_landuse_df, gps_feature_names) {
  # Filter GPS data for the time window
  # Note: column names in gps_landuse_df are start_time and end_time
  window_start <- start_time
  window_end <- end_time
  gps_row <- gps_landuse_df[
    user_id == user_id &
      start_time <= window_end &
      end_time >= window_start
  ]
  
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
  
  return(gps_features)
}

#' Extract headset state features
#'
#' @param snapshot_info data.table with snapshot information
#' @param headset_states Character vector of headset states
#' @return data.table with headset features (one row)
extract_headset_features <- function(snapshot_info, headset_states) {
  if (nrow(snapshot_info) > 0 && "headsetState" %in% names(snapshot_info)) {
    tmp_headset <- snapshot_info[, .N, by = headsetState]
    headset_summary <- data.table::dcast(
      tmp_headset,
      . ~ headsetState,
      value.var = "N",
      fill = 0
    )[, . := NULL]
  } else {
    headset_summary <- create_empty_summary(headset_states, fill = 0)
  }
  
  headset_summary <- ensure_cols(headset_summary, headset_states, fill = 0)
  data.table::setnames(
    headset_summary,
    names(headset_summary),
    paste0("headset_", names(headset_summary))
  )
  
  return(headset_summary)
}

#' Extract detected activity features
#'
#' @param snapshot_ids Integer vector of snapshot IDs
#' @param snapshot_activities data.table with parsed activities
#' @param detected_activity_keys Character vector of activity keys
#' @return data.table with detected activity features (one row)
extract_detected_activity_features <- function(snapshot_ids, snapshot_activities,
                                               detected_activity_keys) {
  if (length(snapshot_ids) > 0 && nrow(snapshot_activities) > 0) {
    tmp <- snapshot_activities[
      J(snapshot_ids),
      .SD,
      .SDcols = detected_activity_keys,
      nomatch = 0L
    ]
    
    if (nrow(tmp) == 0) {
      detected_activity_summary <- create_empty_summary(
        detected_activity_keys,
        fill = 0
      )
    } else {
      detected_activity_summary <- tmp[, lapply(.SD, sum, na.rm = TRUE)]
    }
  } else {
    detected_activity_summary <- create_empty_summary(
      detected_activity_keys,
      fill = 0
    )
  }
  
  detected_activity_summary <- ensure_cols(
    detected_activity_summary,
    detected_activity_keys,
    fill = 0
  )
  data.table::setnames(
    detected_activity_summary,
    names(detected_activity_summary),
    paste0("detected_", names(detected_activity_summary))
  )
  
  return(detected_activity_summary)
}

#' Extract phone activity features
#'
#' @param acts data.table with activity data
#' @param all_activity_names Character vector of activity names
#' @return data.table with phone activity features (one row)
extract_phone_activity_features <- function(acts, all_activity_names) {
  if (nrow(acts) == 0) {
    activity_summary <- create_empty_summary(all_activity_names, fill = 0)
  } else {
    tmp_act <- acts[
      activityName %in% all_activity_names,
      .N,
      by = activityName
    ]
    activity_summary <- data.table::dcast(
      tmp_act,
      . ~ activityName,
      value.var = "N",
      fill = 0
    )[, . := NULL]
    activity_summary <- ensure_cols(activity_summary, all_activity_names, fill = 0)
  }
  
  data.table::setnames(
    activity_summary,
    names(activity_summary),
    paste0("phone_", names(activity_summary))
  )
  
  return(activity_summary)
}

#' Extract app usage features
#'
#' @param acts data.table with activity data
#' @param app_categorization data.table with app categorization
#' @param app_categories Character vector of app categories
#' @return data.table with app usage features (one row)
extract_app_usage_features <- function(acts, app_categorization, app_categories) {
  acts_with_pkg <- acts[!is.na(packageName) & packageName != ""]
  
  if (nrow(acts_with_pkg) == 0) {
    app_usage_summary <- create_empty_summary(app_categories, fill = 0)
  } else {
    app_usage <- app_categorization[acts_with_pkg, on = "packageName", nomatch = 0]
    
    if (nrow(app_usage) == 0) {
      app_usage_summary <- create_empty_summary(app_categories, fill = 0)
    } else {
      tmp_app <- app_usage[, .N, by = Final_Rating]
      app_usage_summary <- data.table::dcast(
        tmp_app,
        . ~ Final_Rating,
        value.var = "N",
        fill = 0
      )[, . := NULL]
      app_usage_summary <- ensure_cols(app_usage_summary, app_categories, fill = 0)
    }
  }
  
  data.table::setnames(
    app_usage_summary,
    names(app_usage_summary),
    paste0("app_", names(app_usage_summary))
  )
  
  return(app_usage_summary)
}

#' Extract screen state features
#'
#' @param acts data.table with activity data
#' @param start_time Window start time
#' @param end_time Window end time
#' @param screen_categories Character vector of screen categories
#' @param preprocessing_screen_window Function to preprocess screen window
#' @return data.table with screen state features (one row)
extract_screen_state_features <- function(acts, start_time, end_time,
                                          screen_categories,
                                          preprocessing_screen_window) {
  screen_state_durations <- preprocessing_screen_window(acts, start_time, end_time)
  
  if (is.null(screen_state_durations) || nrow(screen_state_durations) == 0) {
    screen_state_summary <- create_empty_summary(screen_categories, fill = 0)
  } else {
    data.table::setDT(screen_state_durations)
    tmp_screen <- screen_state_durations[
      , .(duration_sec = sum(duration_sec, na.rm = TRUE)),
      by = event
    ]
    screen_state_summary <- data.table::dcast(
      tmp_screen,
      . ~ event,
      value.var = "duration_sec",
      fill = 0
    )[, . := NULL]
    screen_state_summary <- ensure_cols(
      screen_state_summary,
      screen_categories,
      fill = 0
    )
  }
  
  data.table::setnames(
    screen_state_summary,
    names(screen_state_summary),
    paste0("screen_", names(screen_state_summary))
  )
  
  return(screen_state_summary)
}

#' Extract music features
#'
#' @param acts data.table with activity data
#' @param music_raw data.table with music data
#' @param music_features_master data.table with all music features
#' @param spotify_numeric_features Character vector of Spotify feature names
#' @param genius_numeric_features Character vector of Genius feature names
#' @param topic_numeric_features Character vector of topic feature names
#' @param liwc_numeric_features Character vector of LIWC feature names
#' @return List with music feature data.tables (spotify, genius, topic, liwc)
extract_music_features <- function(acts, music_raw, music_features_master,
                                   spotify_numeric_features,
                                   genius_numeric_features,
                                   topic_numeric_features,
                                   liwc_numeric_features) {
  # Get music IDs from activities
  music_ids <- acts[
    activityName == "MUSIC" & !is.na(music_id),
    as.integer(music_id)
  ]
  
  if (length(music_ids) == 0) {
    return(NULL)
  }
  
  # Get unique tracks
  music_window <- unique(
    music_raw[J(music_ids), .(title, artist), on = "id", nomatch = 0]
  )
  
  if (nrow(music_window) == 0) {
    return(NULL)
  }
  
  # Match with master features table
  all_matched_features <- music_features_master[
    music_window,
    on = .(track = title, artist = artist),
    nomatch = 0
  ]
  
  if (nrow(all_matched_features) == 0) {
    return(NULL)
  }
  
  # Calculate means for each feature set
  spotify_numeric_cols <- intersect(
    spotify_numeric_features,
    names(all_matched_features)
  )
  genius_numeric_cols <- intersect(
    genius_numeric_features,
    names(all_matched_features)
  )
  topic_numeric_cols <- intersect(
    topic_numeric_features,
    names(all_matched_features)
  )
  liwc_numeric_cols <- intersect(
    liwc_numeric_features,
    names(all_matched_features)
  )
  
  spotify_means <- all_matched_features[
    , lapply(.SD, mean, na.rm = TRUE),
    .SDcols = spotify_numeric_cols
  ]
  genius_means <- all_matched_features[
    , lapply(.SD, mean, na.rm = TRUE),
    .SDcols = genius_numeric_cols
  ]
  topic_means <- all_matched_features[
    , lapply(.SD, mean, na.rm = TRUE),
    .SDcols = topic_numeric_cols
  ]
  liwc_means <- all_matched_features[
    , lapply(.SD, mean, na.rm = TRUE),
    .SDcols = liwc_numeric_cols
  ]
  
  # Ensure all columns exist
  spotify_means <- ensure_cols(spotify_means, spotify_numeric_features, fill = NA_real_)
  genius_means <- ensure_cols(genius_means, genius_numeric_features, fill = NA_real_)
  topic_means <- ensure_cols(topic_means, topic_numeric_features, fill = NA_real_)
  liwc_means <- ensure_cols(liwc_means, liwc_numeric_features, fill = NA_real_)
  
  # Add prefixes to column names
  data.table::setnames(spotify_means, paste0("music_", names(spotify_means)))
  data.table::setnames(genius_means, paste0("genius_", names(genius_means)))
  data.table::setnames(
    topic_means,
    paste0("topic_", gsub("\\s+", "_", trimws(names(topic_means))))
  )
  data.table::setnames(liwc_means, paste0("liwc_", names(liwc_means)))
  
  return(list(
    spotify = spotify_means,
    genius = genius_means,
    topic = topic_means,
    liwc = liwc_means
  ))
}

