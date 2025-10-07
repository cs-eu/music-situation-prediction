#' Display Matching and Enrichment Statistics
#'
#' Computes and displays statistics about track matching and enrichment progress.
#'
#' @param distinct_tracks Data frame of all distinct tracks.
#' @param study_track_data Data frame of pre-existing matched tracks.
#' @param kaggle_spotify_data Data frame of Kaggle-provided audio features.
#' @param matched_tracks Data frame of newly matched tracks from Spotify API.
#' @param enriched_data Final enriched data frame after combining all sources.
#'
#' @return None; prints statistics to console.
#' @export
display_statistics <- function(distinct_tracks,
                               study_track_data,
                               kaggle_spotify_data,
                               matched_tracks,
                               enriched_data) {
  
  cat("\n========== Track Matching Statistics ==========\n")
  
  # 1. Total number of distinct tracks
  total_distinct <- nrow(distinct_tracks)
  cat("1. Total distinct tracks:               ", total_distinct, "\n")
  
  # 2. Number of tracks found in study_track_data
  found_in_study <- distinct_tracks %>%
    inner_join(study_track_data, by = c("artist_clean", "track_clean")) %>%
    nrow()
  cat("2. Tracks matched in study_track_data: ", found_in_study, "\n")
  
  # 3. Number of tracks found in kaggle_spotify_data
  found_in_kaggle <- distinct_tracks %>%
    inner_join(kaggle_spotify_data, by = c("track_clean" = "artist_clean")) %>% 
    nrow()
  cat("3. Tracks matched in kaggle_spotify_data: ", found_in_kaggle, "\n")
  
  # 4. Number of tracks classified as podcast / non-music
  non_music_count <- enriched_data %>%
    filter(track_non_music == 1) %>%
    nrow()
  cat("4. Tracks classified as non-music:     ", non_music_count, "\n")
  
  # 5. Number of tracks matched via Spotify API
  matched_via_api <- matched_tracks %>%
    filter(!is.na(spotify_id)) %>%
    nrow()
  cat("5. Tracks matched via Spotify API:     ", matched_via_api, "\n")
  
  # 6. Number of tracks that could not be matched at all
  unmatched_count <- enriched_data %>%
    filter(is.na(track_spotify) & is.na(track_spotify_id)) %>%
    nrow()
  cat("6. Tracks still unmatched:             ", unmatched_count, "\n")
  
  cat("==============================================\n\n")
}
