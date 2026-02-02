#' Main Script to Load, Match, Enrich, and Export Spotify Track Data
#'
#' This script orchestrates the entire workflow:
#' - Loads preprocessed track data
#' - Identifies unmatched tracks
#' - Searches Spotify API for missing matches
#' - Enriches matched tracks with audio features
#' - Exports the final enriched dataset
#'
#' @note Ensure all required utility scripts are sourced and dependencies installed.
#' 

# adapt the working directory to the project root
setwd("")

# Load utility scripts for setup, data I/O, Spotify API helpers, and processing functions
source("utils/setup.R")
source("utils/data_io.R")
source("utils/spotify_helpers.R")
source("utils/processing.R")

# Load and prepare the data: returns a list containing distinct tracks and previous track datasets
data <- load_data(
  study_track_data_file = "data/helper/study_track_data.rds",
  kaggle_spotify_data_file = "data/helper/kaggle_spotify_data.rds"
)
distinct_tracks <- data$distinct_tracks
study_track_data <- data$study_track_data
kaggle_spotify_data <- data$kaggle_spotify_data

# Identify unmatched tracks by left joining known matches and filtering missing track_found
unmatched <- distinct_tracks %>%
  left_join(study_track_data, by = c("artist_clean", "track_clean")) %>%
  mutate(track_non_music = coalesce(track_non_music.y, track_non_music.x, 0)) %>%
  filter(is.na(track_spotify_id)) %>%
  select(artist_clean, track_clean, track_non_music) %>%
  distinct(artist_clean, track_clean, .keep_all = TRUE)

# Run Spotify search to find missing matches, starting from offset
run_search(unmatched, offset = 0, output_file = "data/results/matched_tracks.csv")

# Read the new matched tracks results from CSV
matched_tracks <- read_csv("data/results/matched_tracks.csv")

# Enrich all track data by combining original, known matches, and newly matched audio features
enriched <- match_and_enrich(distinct_tracks, study_track_data, kaggle_spotify_data, matched_tracks)

# Export the enriched data to a file (e.g., CSV or preferred format)
export_data(enriched, "data/results/distinct_tracks_enriched.rds", "data/results/distinct_tracks_enriched.csv")
