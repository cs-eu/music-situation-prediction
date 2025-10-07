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

# Load utility scripts for setup, data I/O, Spotify API helpers, and processing functions
source("utils/setup.R")
source("utils/data_io.R")
source("utils/spotify_helpers.R")
source("utils/processing.R")

# Load and prepare the data: returns a list containing distinct tracks and previous track datasets
data <- load_data()
distinct_tracks <- data$distinct_tracks
track_data1 <- data$track_data1
track_data2 <- data$track_data2

# Identify unmatched tracks by left joining known matches and filtering missing track_found
unmatched <- distinct_tracks %>%
  left_join(track_data1, by = c("artist_clean", "track_clean")) %>%
  filter(is.na(track_found)) %>%
  select(artist_clean, track_clean)

# Run Spotify search to find missing matches, starting from offset 0
run_search(unmatched, offset = 0)

# Read the new matched tracks results from CSV
matched_tracks <- read_csv("matched_tracks.csv")

# Enrich all track data by combining original, known matches, and newly matched audio features
enriched <- match_and_enrich(distinct_tracks, track_data1, track_data2, matched_tracks)

# Export the enriched data to a file (e.g., CSV or preferred format)
export_data(enriched)
