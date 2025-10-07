#' Match and Enrich Track Data
#'
#' Combines and enriches track information from multiple sources:
#' - Joins distinct tracks with known matches (study_track_data)
#' - Enriches newly matched tracks with audio features (kaggle_spotify_data)
#' - Merges all data into a single comprehensive data frame
#'
#' @param distinct_tracks Data frame of unique cleaned tracks with `artist_clean` and `track_clean`.
#' @param study_track_data Data frame containing previously found track metadata with columns 
#'   such as `artist_clean`, `track_clean`, `track_found`, `artist_found`, `album_found`, and `is_podcast`.
#' @param kaggle_spotify_data Data frame of Spotify audio features with `id` corresponding to `spotify_id`.
#' @param matched_tracks Data frame of newly matched tracks with Spotify IDs and podcast flag.
#'
#' @return A data frame combining original and enriched track data with audio features.
#'
#' @examples
#' \dontrun{
#' enriched_data <- match_and_enrich(distinct_tracks, study_track_data, kaggle_spotify_data, matched_tracks)
#' }
#' @export
match_and_enrich <- function(distinct_tracks, study_track_data, kaggle_spotify_data, matched_tracks) {
  
  # ---- 1. Join known matches to distinct tracks ----
  message("Processing ", nrow(distinct_tracks), " distinct tracks from logging data...")
  joined_tracks <- distinct_tracks %>%
    left_join(study_track_data, by = c("artist_clean", "track_clean")) %>%
    rename(
      track_spotify     = track_found,
      artist_spotify    = artist_found,
      album_spotify     = album_found
    ) %>%
    mutate(
      track  = coalesce(track.y, track.x),
      artist = coalesce(artist.y, artist.x),
      
      # Combine track_non_music flags from both datasets
      track_non_music = as.integer(coalesce(track_non_music.x, 0) | coalesce(track_non_music.y, 0))
    ) %>%
    select(-ends_with(".x"), -ends_with(".y")) %>%
    group_by(artist_clean, track_clean) %>%
    slice(1) %>%
    ungroup()
  
  matched_previously <- sum(!is.na(joined_tracks$track_spotify))
  message(matched_previously, " tracks could be matched in the preprocessed track data.")
  
  # ---- 2. Enrich matched tracks with audio features ----
  enriched_matches <- matched_tracks %>%
    left_join(kaggle_spotify_data, by = c("spotify_id" = "id")) %>%
    
    mutate(
      artist_clean = coalesce(artist_clean.x, artist_clean.y),
      track_clean  = coalesce(track_clean.x,  track_clean.y),
      track_non_music = as.integer(coalesce(track_non_music.x, 0) | coalesce(track_non_music.y, 0))
    ) %>%
    select(-artist_clean.x, -artist_clean.y,
           -track_clean.x,  -track_clean.y,
           -track_non_music.x,-track_non_music.y) %>%
    group_by(artist_clean, track_clean) %>%
    slice(1) %>%
    ungroup() %>%
    
    # Rename selected fields for consistency
    rename(
      track_spotify_id    = spotify_id,
      track_duration_ms   = duration_ms,
      album_release_date  = release_date,
      album_year          = year
    ) %>%
    
    # Prefix audio features for clarity
    rename_with(~ paste0("track_", .), starts_with("danceability"):starts_with("tempo")) %>%
    
    # Create binary time signature match flag
    mutate(across(starts_with("track_time_signature"), ~ as.integer(. == track_time_signature))) %>%
    
    # Keep only columns also present in joined_tracks to satisfy rows_update
    select(all_of(intersect(colnames(.), colnames(joined_tracks))))
  
  message(nrow(enriched_matches), " additional tracks enriched using Spotify API data.")
  
  # ---- 3. Update main data with enriched Spotify audio features ----
  enriched_matches_filtered <- semi_join(enriched_matches, joined_tracks, by = c("artist_clean", "track_clean"))
  
  full_data <- joined_tracks %>%
    rows_update(enriched_matches_filtered, by = c("artist_clean", "track_clean")) %>%
    filter(track_non_music == 1 | !is.na(track_danceability))
  
  # ---- 4. Ensure track_non_music is 1 if any dataset indicates non-music ----
  full_data <- full_data %>%
    mutate(track_non_music = as.integer(
      track_non_music == 1 |
        coalesce(matched_tracks$track_non_music[match(paste0(artist_clean, track_clean),
                                                      paste0(matched_tracks$artist_clean, matched_tracks$track_clean))], 0) == 1
    ))
  
  # ---- 5. Identify unmatched tracks ----
  full_unmatched_data <- anti_join(
    distinct_tracks,
    full_data,
    by = c("artist_clean", "track_clean")
  )
  
  write.csv(full_unmatched_data, "data/results/full_unmatched_data.csv", row.names = FALSE)
  message(nrow(full_unmatched_data), " tracks remain unmatched.")
  
  return(full_data)
}
