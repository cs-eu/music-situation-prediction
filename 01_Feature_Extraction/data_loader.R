# Data loading module for feature extraction
# This file contains functions to load and preprocess all required data

#' Load database credentials
#'
#' @param credentials_path Path to credentials file
#' @return List with database credentials
load_db_credentials <- function(credentials_path) {
  if (!file.exists(credentials_path)) {
    stop("Database credentials file not found: ", credentials_path)
  }
  invisible(capture.output(source(credentials_path)))
  list(
    user = mariadb_user,
    password = mariadb_pw
  )
}

#' Connect to database
#'
#' @param credentials List with database credentials (from load_db_credentials)
#' @param host Database host
#' @param port Database port
#' @param dbname Database name
#' @return Database connection object
connect_to_database <- function(credentials, host, port, dbname) {
  con <- DBI::dbConnect(
    drv = RMariaDB::MariaDB(),
    username = credentials$user,
    password = credentials$password,
    host = host,
    port = port,
    dbname = dbname
  )
  return(con)
}

#' Load raw data from database
#'
#' @param con Database connection
#' @return List containing raw data tables
load_raw_database_data <- function(con) {
  message("Loading raw data from database...")
  
  list(
    music_raw = DBI::dbReadTable(con, "ps_music"),
    snapshot_raw = DBI::dbGetQuery(
      con,
      "SELECT id, headsetState, detectedActivities FROM ps_snapshot"
    )
  )
}

#' Load helper data from files
#'
#' @param paths List of file paths (from config)
#' @return List containing helper data tables
load_helper_data <- function(paths) {
  message("Loading helper data from files...")
  
  list(
    activity_data_raw = readRDS(paths$activity_data),
    music_windows = data.table::fread(paths$music_windows),
    app_categorization = data.table::fread(paths$app_categorization),
    gps_landuse_df = readRDS(paths$gps_landuse),
    spotify_data = readRDS(paths$spotify_data),
    genius_features = data.table::fread(paths$genius_features),
    topic_features = data.table::fread(paths$topic_features),
    liwc_features = data.table::fread(paths$liwc_features)
  )
}

#' Convert data.frames to data.tables and set keys
#'
#' @param data_list List of data tables
#' @return List with data.tables and keys set
prepare_data_tables <- function(data_list) {
  message("Converting to data.table and setting keys...")
  
  # Convert to data.table
  data_list <- lapply(data_list, function(dt) {
    if (!data.table::is.data.table(dt)) {
      data.table::setDT(dt)
    }
    return(dt)
  })
  
  # Set keys for performance
  data.table::setkey(data_list$music_raw, id)
  data.table::setkey(data_list$snapshot_raw, id)
  data.table::setkey(data_list$activity_data_raw, user_id, timestamp)
  data.table::setkey(data_list$gps_landuse_df, user_id, start_time, end_time)
  data.table::setkey(data_list$app_categorization, App_name)
  data.table::setkey(data_list$spotify_data, track, artist)
  
  # Prepare lyrics features
  data_list$genius_features[, track_spotify_id := sub("lyrics:", "", lyricsID)]
  data.table::setkey(data_list$genius_features, track_spotify_id)
  
  data_list$topic_features[, track_spotify_id := sub("lyrics:", "", lyricsID)]
  data.table::setkey(data_list$topic_features, track_spotify_id)
  
  data_list$liwc_features[, track_spotify_id := sub("lyrics:", "", track_uri)]
  data.table::setkey(data_list$liwc_features, track_spotify_id)
  
  # Clean up app categorization
  data.table::setnames(data_list$app_categorization, "App_name", "packageName")
  data_list$app_categorization[is.na(Final_Rating), Final_Rating := "Unknown"]
  
  return(data_list)
}

#' Preprocess JSON activities from snapshot data
#'
#' @param snapshot_raw data.table with snapshot data
#' @return data.table with parsed activities
preprocess_snapshot_activities <- function(snapshot_raw) {
  message("Pre-processing JSON activity data...")
  
  # Filter out rows with no activity data
  snapshot_filtered <- snapshot_raw[
    !is.na(detectedActivities) & detectedActivities != "[]",
    .(id, detectedActivities)
  ]
  
  if (nrow(snapshot_filtered) == 0) {
    return(data.table(id = integer(0)))
  }
  
  # Parse JSON using RcppSimdJson
  parsed_list <- RcppSimdJson::fparse(snapshot_filtered$detectedActivities)
  names(parsed_list) <- snapshot_filtered$id
  
  # Combine into single data.table
  snapshot_activities <- data.table::rbindlist(
    parsed_list,
    idcol = "id",
    fill = TRUE
  )
  
  # Ensure id is integer
  snapshot_activities[, id := as.integer(id)]
  
  # Replace NAs with 0 for numeric columns
  for (col in names(snapshot_activities)) {
    if (is.numeric(snapshot_activities[[col]])) {
      data.table::set(
        snapshot_activities,
        which(is.na(snapshot_activities[[col]])),
        col,
        0
      )
    }
  }
  
  # Set key for fast joins
  data.table::setkey(snapshot_activities, id)
  
  return(snapshot_activities)
}

#' Create master music features table by joining all feature tables
#'
#' @param spotify_data data.table with Spotify features
#' @param genius_features data.table with Genius features
#' @param topic_features data.table with topic features
#' @param liwc_features data.table with LIWC features
#' @return data.table with all music features joined
create_music_features_master <- function(spotify_data, genius_features,
                                         topic_features, liwc_features) {
  message("Pre-joining all music feature tables into a master table...")
  
  # Join all lyrics features
  lyrics_features_all <- genius_features[topic_features, on = "track_spotify_id"][
    liwc_features, on = "track_spotify_id"
  ]
  
  # Join with Spotify data
  music_features_master <- spotify_data[lyrics_features_all, on = "track_spotify_id"]
  
  # Set key for fast joins
  data.table::setkey(music_features_master, track, artist)
  
  return(music_features_master)
}

#' Initialize app categories from app categorization data
#'
#' @param app_categorization data.table with app categorization
#' @return Character vector of sorted unique app categories
initialize_app_categories <- function(app_categorization) {
  sort(unique(app_categorization$Final_Rating))
}

