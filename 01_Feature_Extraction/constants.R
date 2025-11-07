# Constants file for feature extraction
# This file contains all constant values used throughout the feature extraction process

# --- Activity Categories ---
APP_CATEGORIES <- NULL  # Will be populated from data
HEADSET_STATES <- c("PLUGGED", "UNPLUGGED")
SCREEN_CATEGORIES <- c("ON_UNLOCKED", "OFF_UNLOCKED", "OFF_LOCKED", "ON_LOCKED", "UNKNOWN")
DETECTED_ACTIVITY_KEYS <- c(
  "STILL", "WALKING", "RUNNING", "ON_BICYCLE", "ON_FOOT",
  "IN_VEHICLE", "IN_ROAD_VEHICLE", "IN_RAIL_VEHICLE", "IN_FOUR_WHEELER_VEHICLE"
)
ALL_ACTIVITY_NAMES <- c(
  "AIRPLANE", "APPS", "BATTERYSAVINGMODE", "BLUETOOTH", "CAMERA",
  "CALENDAR", "GPS", "NOTIFICATION", "PHONE", "POWER", "SCREEN", "SMS"
)

# --- GPS Features ---
GPS_FEATURES <- c(
  "gps_count", "gps_first_latitude", "gps_first_longitude",
  "gps_landuse_type", "gps_home", "gps_work"
)

# --- Music Feature Sets ---
SPOTIFY_NUMERIC_FEATURES <- c(
  "track_danceability", "track_energy", "track_key", "track_loudness",
  "track_mode_major", "track_speechiness", "track_acousticness",
  "track_instrumentalness", "track_liveness", "track_valence",
  "track_tempo", "track_non_music"
)

GENIUS_NUMERIC_FEATURES <- c(
  "fear", "anger", "trust", "surprise", "positive", "negative",
  "sadness", "disgust", "joy", "anticipation", "lyric_len"
)

TOPIC_NUMERIC_FEATURES <- paste0("Topic ", 0:29)

LIWC_NUMERIC_FEATURES <- c(
  "WC", "Analytic", "Clout", "Authentic", "Tone", "WPS", "Sixltr", "Dic",
  "function.", "pronoun", "ppron", "i", "we", "you_total", "you_sing",
  "you_plur", "you_formal", "other", "shehe", "they", "ipron", "article",
  "prep", "auxverb", "adverb", "conj", "negate", "verb", "adj", "compare",
  "interrog", "number", "quant", "affect", "posemo", "negemo", "anx",
  "anger", "sad", "social", "family", "friend", "female", "male", "cogproc",
  "insight", "cause", "discrep", "tentat", "certain", "differ", "percept",
  "see", "hear", "feel", "bio", "body", "health", "sexual", "ingest",
  "drives", "affiliation", "achiev", "power", "reward", "risk", "focuspast",
  "focuspresent", "focusfuture", "relativ", "motion", "space", "time",
  "work", "leisure", "home", "money", "relig", "death", "informal", "swear",
  "netspeak", "assent", "nonflu", "filler", "AllPunc", "Period", "Comma",
  "Colon", "SemiC", "QMark", "Exclam", "Dash", "Quote", "Apostro",
  "Parenth", "OtherP", "Emoji"
)

# --- Time Bin Definitions ---
TIME_BINS <- list(
  Morning = c(5, 8),
  LateMorning = c(8, 11),
  Noon = c(11, 13),
  Afternoon = c(13, 17),
  Evening = c(17, 21),
  Night = NULL  # Everything else
)

