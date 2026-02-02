#' Setup environment for Spotify feature processing
#'
#' Loads required packages, sets working directory, and initializes Spotify API credentials.
#' Also defines a helper function for normalizing text inputs.
#'
#' @details
#' - Loads libraries: dplyr, stringr, readr, jsonlite, spotifyr
#' - Sets working directory to the project folder
#' - Configures Spotify API credentials from environment variables
#' - Defines \code{normalize_text} function for text preprocessing
#'
#' @return
#' No return value; sets up environment and defines helper function.
#'
#' @examples
#' \dontrun{
#' # After sourcing this script, you can call:
#' cleaned_text <- normalize_text("My Song (Official Music Video)")
#' }
#'
#' @export
# Load packages
library(dplyr)
library(stringr)
library(readr)
library(jsonlite)
library(spotifyr)

# Replace with your actual Spotify Client ID and Secret
Sys.setenv(SPOTIFY_CLIENT_ID = '')
Sys.setenv(SPOTIFY_CLIENT_SECRET = '')
access_token <- get_spotify_access_token()

#' Normalize text for matching
#'
#' Cleans and standardizes text strings by removing common noise like brackets,
#' special characters, casing, and common non-informative keywords.
#'
#' @param text A character string or vector to be normalized.
#'
#' @return A cleaned character string or vector.
#' @examples
#' normalize_text("My Song (Official Music Video)")
#' # Returns: "my song"
#' @export
normalize_text <- function(text) {
  # Handle non-character or empty input
  text <- ifelse(is.na(text) | trimws(text) == "", "", text)

  # 1. Normalize Unicode (convert accents, fancy dashes, quotes)
  text <- iconv(text, to = "ASCII//TRANSLIT")   # transliterate to basic ASCII
  

  # 2. Replace underscores with spaces
  text <- gsub("_", " ", text)
  

  # 3. Lowercase everything for consistency
  text <- tolower(text)
  

  # 4. Remove leading track numbers / counters (e.g. "01 - ", "3.", "07_")
  text <- gsub("^\\s*\\d{1,3}\\s*[-\\.:–—]\\s*", "", text)
  

  # 5. Remove noisy suffixes after dashes (e.g., "Song - Live at Wembley")
  # text <- sub("\\s*[\\u2012-\\u2015\\u2212\\-–—]\\s*.*$", "", text)
  text <- sub("\\s*[-–—]\\s+[^-–—]+$", "", text)
  

  # 6. Remove bracketed or parenthesized content if it’s meta-info
  text <- gsub("\\(feat\\.?\\s*.*?\\)", "", text, ignore.case = TRUE)
  text <- gsub("\\[(live|mix|edit|remaster|feat|version|ost|soundtrack).*?\\]", "", text, ignore.case = TRUE)
  text <- gsub("\\((live|mix|edit|remaster|feat|version|ost|soundtrack).*?\\)", "", text, ignore.case = TRUE)
  

  # 7. Remove known noisy phrases
  noise_patterns <- c(
    "official\\s*music\\s*video",
    "audio\\s*only",
    "hd\\s*remaster(ed)?",
    "hq\\s*audio",
    "lyrics?",
    "cover\\s*version",
    "karaoke",
    "<unknown>",
    "8d\\s*audio",
    "remastered?\\s*\\d{2,4}",
    "radio\\s*edit",
    "live(\\s+at.*)?",
    "feat\\.?\\s*.*",
    "from\\s+the\\s+motion\\s+picture.*"
  )

  for (pattern in noise_patterns) {
    text <- gsub(pattern, "", text, ignore.case = TRUE, perl = TRUE)
  }
  

  # 8. Remove extra quotes and leftover symbols
  text <- gsub('[\"“”\'`]', "", text)
  text <- gsub("[\\[\\]<>!@#$&/]", "", text)
  

  # 9. Collapse multiple whitespaces
  text <- gsub("\\s+", " ", text)
  

  # 10. Remove trailing punctuation
  text <- gsub("[.,;:!?]+$", "", text)
  

  # 11. Final trim
  text <- stringr::str_trim(text)
  

  return(text)
}
