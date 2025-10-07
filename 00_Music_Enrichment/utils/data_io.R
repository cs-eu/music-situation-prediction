# load_data <- function() {
#   distinct_tracks <- read.csv("distinct_tracks.csv") %>%
#     rename(track = title) %>%
#     mutate(artist_clean = normalize_text(artist),
#            track_clean = normalize_text(track))
# 
#   track_data1 <- readRDS("track_data1.rds") %>%
#     mutate(artist_clean = normalize_text(artist),
#            track_clean = normalize_text(track)) %>%
#     group_by(artist_clean, track_clean) %>%
#     slice(1) %>%
#     ungroup()
# 
#   track_data2 <- readRDS("track_data2.rds")
# 
#   list(distinct_tracks = distinct_tracks,
#        track_data1 = track_data1,
#        track_data2 = track_data2)
# }
# 
# export_data <- function(data) {
#   saveRDS(data, "distinct_tracks_enriched.rds")
#   write.csv(data, "distinct_tracks_enriched.csv", row.names = FALSE)
# }


#' #' Load and preprocess track data from SQL database and Spotify datasets
#' #'
#' #' Connects to a local MariaDB database to load music listening logs (`ps_music`),
#' #' deduplicates entries based on normalized artist and title, and combines this with
#' #' two external Spotify datasets (a study dataset and the Kaggle 1.2M Spotify tracks dataset).
#' #'
#' #' @param study_track_data_file Path to the RDS file from the previous study (e.g., "track_data1.rds")
#' #' @param kaggle_spotify_data_file Path to the RDS file of the Kaggle Spotify dataset (e.g., "track_data2.rds")
#' #'
#' #' @return A named list with:
#' #' \describe{
#' #'   \item{distinct_tracks}{Unique tracks (title + artist) from database logs}
#' #'   \item{combined_track_data}{Merged and cleaned dataset from the study and Kaggle Spotify data}
#' #' }
#' #'
#' #' @examples
#' #' \dontrun{
#' #' data <- load_data("track_data1.rds", "track_data2.rds")
#' #' }
#' #'
#' #' @export
#' load_data <- function(study_track_data_file, kaggle_spotify_data_file) {
#'   # Load required libraries
#'   library(RMariaDB)
#'   library(DBI)
#'   library(dplyr)
#'   library(stringr)
#'   
#'   # Load DB credentials from a local file
#'   invisible(capture.output(source('/local/.meta/dbcredentials.R')))
#'   
#'   # Establish SQL connection
#'   phonestudy <- dbConnect(
#'     drv = RMariaDB::MariaDB(),
#'     username = mariadb_user,
#'     password = mariadb_pw,
#'     host = "localhost", 
#'     port = 3306,
#'     dbname = "live"
#'   )
#'   
#'   # Load music listening data from database
#'   music <- dbReadTable(phonestudy, "ps_music")
#'   
#'   # Clean and normalize music data from SQL
#'   distinct_tracks <- music %>%
#'     mutate(
#'       artist_clean = normalize_text(artist),
#'       track_clean = normalize_text(title)
#'     ) %>%
#'     distinct(artist_clean, track_clean, .keep_all = TRUE)
#'   
#'   # Drop unnecessary columns
#'   cols_to_remove <- c(
#'     "user_id", "event_id", "event", "packageName", "timestamp",
#'     "track_time_signature_0", "track_time_signature_1", "track_time_signature_3",
#'     "track_time_signature_4", "track_time_signature_5", "user_birth",
#'     "artist_power_distance", "artist_individualism", "artist_masculinity",
#'     "artist_uncertainty_avoidance", "artist_long_term", "artist_cultural_distance_4",
#'     "artist_cultural_distance_6", "artist_country_no_info", "artist_country",
#'     "artist_german_cluster", "time_to_next_event", "time_to_next_event_adj",
#'     "in_week_top", "date", "week", "track_is_explicit", "album_birth_yrs", 
#'     "is_music_incorrect", "is_music_correct", "observed_track_id", "track_spotify_uri",
#'     "track_href"
#'   )
#'   
#'   # Load study dataset and clean
#'   study_track_data <- readRDS(study_track_data_file) %>%
#'     mutate(
#'       artist_clean = normalize_text(artist),
#'       track_clean = normalize_text(track)
#'     ) %>%
#'     group_by(artist_clean, track_clean) %>%
#'     slice(1) %>%
#'     ungroup()
#'   study_track_data <- study_track_data %>%
#'     select(-all_of(intersect(cols_to_remove, names(study_track_data))))
#'   
#'   # Load and prepare Kaggle data
#'   kaggle_spotify_data <- readRDS(kaggle_spotify_data_file) %>%
#'     rename(track_spotify_id = id,
#'       track = name,
#'       artist = artists,
#'       track_danceability = danceability,
#'       track_energy = energy,
#'       track_key = key,
#'       track_loudness = loudness,
#'       track_mode_major = mode,
#'       track_speechiness = speechiness,
#'       track_acousticness = acousticness,
#'       track_instrumentalness = instrumentalness,
#'       track_liveness = liveness,
#'       track_valence = valence,
#'       track_tempo = tempo,
#'       track_duration_ms = duration_ms,
#'       track_is_explicit = explicit,
#'       album_release_date = release_date,
#'       album = album,
#'       artist_genres = artist_ids,
#'       album_tracks = album_id,
#'       album_year = year
#'     )  %>%
#'     mutate(
#'       artist_clean = normalize_text(artist),
#'       track_clean = normalize_text(track),
#'       track_found = track,
#'       artist_found = artist,
#'       album_found = album
#'     )
#'   
#'   # Add missing columns from study data to Kaggle data
#'   missing_cols <- setdiff(names(study_track_data), names(kaggle_spotify_data))
#'   for (col in missing_cols) {
#'     kaggle_spotify_data[[col]] <- NA
#'   }
#'   
#'   # Reorder columns
#'   kaggle_spotify_data <- kaggle_spotify_data[, names(study_track_data)]
#'   
#'   # Remove Kaggle entries already in study data
#'   kaggle_filtered <- kaggle_spotify_data %>%
#'     filter(!track_spotify_id %in% study_track_data$track_spotify_id)
#'   
#'   # Combine both datasets
#'   combined_track_data <- bind_rows(study_track_data, kaggle_filtered)
#'   
#'   combined_track_data <- combined_track_data %>%
#'     distinct(track_spotify_id, .keep_all = TRUE)
#'   
#'   # Return both processed datasets
#'   list(
#'     distinct_tracks = distinct_tracks,
#'     combined_track_data = combined_track_data
#'   )
#' }




#' Load and preprocess track data from SQL database and Spotify datasets
#'
#' Connects to a local MariaDB database to load music listening logs (`ps_music`),
#' deduplicates entries based on normalized artist and title, and combines this with
#' two external Spotify datasets (a study dataset and the Kaggle 1.2M Spotify tracks dataset).
#'
#' @param study_track_data_file Path to the RDS file from the previous study (e.g., "track_data1.rds")
#' @param kaggle_spotify_data_file Path to the RDS file of the Kaggle Spotify dataset (e.g., "track_data2.rds")
#'
#' @return A named list with:
#' \describe{
#'   \item{distinct_tracks}{Unique tracks (title + artist) from database logs}
#'   \item{combined_track_data}{Merged and cleaned dataset from the study and Kaggle Spotify data}
#' }
#'
#' @examples
#' \dontrun{
#' data <- load_data("track_data1.rds", "track_data2.rds")
#' }
#'
#' @export
load_data <- function(study_track_data_file, kaggle_spotify_data_file) {
  # Load required libraries
  library(RMariaDB)
  library(DBI)
  library(dplyr)
  library(stringr)
  
  # Load DB credentials from a local file
  invisible(capture.output(source('/local/.meta/dbcredentials.R')))
  
  # Establish SQL connection
  phonestudy <- dbConnect(
    drv = RMariaDB::MariaDB(),
    username = mariadb_user,
    password = mariadb_pw,
    host = "localhost", 
    port = 3306,
    dbname = "live"
  )
  
  # Define podcast / non-music keywords
  podcast_keywords <- c("teil", "kapitel", "episode", "folge", "podcast", "interview")
  podcast_pattern <- paste(podcast_keywords, collapse = "|")
  
  # ---- Load and preprocess DB music data ----
  music <- dbReadTable(phonestudy, "ps_music")
  
  distinct_tracks <- music %>%
    rename(track = title) %>%
    mutate(
      artist_clean = normalize_text(artist),
      track_clean = normalize_text(track),
      track_non_music = str_detect(track_clean, podcast_pattern)  # <- NEW COLUMN
    ) %>%
    distinct(artist_clean, track_clean, .keep_all = TRUE) %>%
    select(track, artist, track_clean, artist_clean, track_non_music)
  
  # ---- Define columns to remove ----
  cols_to_remove <- c(
    "user_id", "event_id", "event", "packageName", "timestamp",
    "track_time_signature_0", "track_time_signature_1", "track_time_signature_3",
    "track_time_signature_4", "track_time_signature_5", "user_birth",
    "artist_power_distance", "artist_individualism", "artist_masculinity",
    "artist_uncertainty_avoidance", "artist_long_term", "artist_cultural_distance_4",
    "artist_cultural_distance_6", "artist_country_no_info", "artist_country",
    "artist_german_cluster", "time_to_next_event", "time_to_next_event_adj",
    "in_week_top", "date", "week", "track_is_explicit", "album_birth_yrs",
    "is_music_incorrect", "is_music_correct", "observed_track_id", "track_spotify_uri",
    "track_href"
  )
  
  # ---- Load and preprocess study dataset ----
  study_track_data <- readRDS(study_track_data_file) %>%
    mutate(
      artist_clean = normalize_text(artist),
      track_clean = normalize_text(track),
      track_non_music = str_detect(track_clean, podcast_pattern)  # <- NEW COLUMN
    ) %>%
    select(-any_of(cols_to_remove))
  
  # ---- Load and preprocess Kaggle dataset ----
  kaggle_spotify_data <- readRDS(kaggle_spotify_data_file) %>%
    mutate(
      artist_clean = normalize_text(artists),
      track_clean = normalize_text(name),
      track_non_music = str_detect(track_clean, podcast_pattern)  # <- NEW COLUMN
    )
  
  # ---- Return processed datasets ----
  list(
    distinct_tracks     = distinct_tracks,
    study_track_data    = study_track_data,
    kaggle_spotify_data = kaggle_spotify_data
  )
}


#' Export enriched track data to files
#'
#' Saves a data frame to specified RDS and CSV files.
#'
#' @param data A data frame containing enriched track data.
#' @param rds_file File path to save the RDS version (e.g., "output.rds")
#' @param csv_file File path to save the CSV version (e.g., "output.csv")
#'
#' @details
#' - Writes the data frame to RDS and CSV formats
#' - The CSV file will not include row names
#'
#' @return
#' No return value. Saves data to disk.
#'
#' @examples
#' \dontrun{
#' export_data(data$distinct_tracks, "enriched.rds", "enriched.csv")
#' }
#'
#' @export
export_data <- function(data, rds_file, csv_file) {
  saveRDS(data, rds_file)
  write.csv(data, csv_file, row.names = FALSE)
}