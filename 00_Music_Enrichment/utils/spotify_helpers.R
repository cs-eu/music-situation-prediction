#' Retrieve Spotify Track Data
#'
#' Searches Spotify for a track by its cleaned track and artist names, and returns detailed
#' information about the best matching track including artist genres. Handles podcast filtering.
#'
#' @param track_clean Character string of the cleaned track name.
#' @param artist_clean Character string of the cleaned artist name.
#' @param verbose Logical flag to print search information (default TRUE).
#'
#' @return A data frame with track and artist info, or NULL if no match found.
#'   If the track appears to be a podcast, returns a data frame with `track_is_podcast` set.
#'
#' @examples
#' \dontrun{
#' get_track_data("shape of you", "ed sheeran")
#' }
#' @export
get_track_data <- function(track_clean, artist_clean, track_non_music = FALSE, verbose = TRUE) {
  # Keywords that suggest the track is a podcast or spoken episode
  podcast_keywords <- c("teil", "kapitel", "episode", "folge", "podcast", "interview")
  podcast_pattern <- paste(podcast_keywords, collapse = "|")
  
  # Return NULL immediately if track name is empty
  if (nchar(track_clean) == 0) return(NULL)
  
  # Build Spotify search query string
  search_str <- if (nchar(artist_clean) > 0) {
    sprintf('track:"%s" artist:"%s"', track_clean, artist_clean)
  } else {
    sprintf('track:"%s"', track_clean)
  }
  
  if (verbose) cat("Search:", search_str, "\n")
  
  # Try to search Spotify for tracks
  results <- tryCatch(
    search_spotify(search_str, type = "track", limit = 10),
    error = function(e) NULL
  )
  
  # Process results if any are found
  if (!is.null(results) && nrow(results) > 0) {
    best <- results[1, ]
    artist_id <- best$artists[[1]]$id[1]
    
    genres <- NA
    if (!is.null(artist_id)) {
      artist_info <- tryCatch(get_artist(artist_id), error = function(e) NULL)
      if (!is.null(artist_info)) {
        genres_json <- jsonlite::toJSON(artist_info$genres)
        genres <- gsub('"', "'", as.character(genres_json))
        if (genres == "[]") genres <- NA
      }
    }
    
    # Update track_non_music if Spotify flags it or title contains keywords
    is_non_music <- track_non_music ||
      (isTRUE(best$album$type == "podcast")) ||  # Spotify flag
      str_detect(track_clean, podcast_pattern)
    
    return(data.frame(
      track_clean      = track_clean,
      artist_clean     = artist_clean,
      track_spotify    = best$name,
      artist_spotify   = best$artists[[1]]$name[1],
      album_spotify    = best$album.name,
      spotify_id       = best$id,
      track_non_music  = as.integer(is_non_music),
      artist_genres    = genres,
      stringsAsFactors = FALSE
    ))
  }
  
  # If no results found, preserve original track_non_music
  return(data.frame(
    track_clean      = track_clean,
    artist_clean     = artist_clean,
    track_spotify    = NA,
    artist_spotify   = NA,
    album_spotify    = NA,
    spotify_id       = NA,
    track_non_music  = as.integer(track_non_music),
    artist_genres    = NA,
    stringsAsFactors = FALSE
  ))
}

#' Batch Run Spotify Track Searches and Save Results
#'
#' Iterates over a data frame of cleaned track and artist names, searches Spotify for each,
#' and appends the found matches to a CSV file. Supports resuming via offset.
#'
#' @param tracks Data frame with columns `track_clean` and `artist_clean`.
#' @param offset Integer row index to start processing from (default 0).
#' @param output_file Character string specifying the CSV filename to save results (default "matched_tracks.csv").
#'
#' @return No return value; writes matched track data to the specified CSV file.
#'
#' @examples
#' \dontrun{
#' run_search(tracks_df, offset = 10, output_file = "matched_tracks.csv")
#' }
#' @export
run_search <- function(tracks, offset = 0, output_file = "matched_tracks.csv") {
  # Skip all non-music tracks right away
  tracks <- tracks %>% filter(!track_non_music)
  
  first <- offset == 0
  
  for (id in seq_len(nrow(tracks))) {
    if (id <= offset) next
    
    cat("Processing ID:", id, "\n")
    
    row <- tracks[id, ]
    result <- get_track_data(
      track_clean      = row$track_clean,
      artist_clean     = row$artist_clean,
      track_non_music  = row$track_non_music   # pass the precomputed flag
    )
    
    if (!is.null(result)) {
      readr::write_csv(result, output_file, append = !first)
      first <- FALSE
    }
  }
}

