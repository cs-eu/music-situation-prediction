# 01_MusicWindowExtract_DirectCSV.R
library(dplyr)
library(lubridate)
library(DBI)
library(future)
library(future.apply)
library(readr)

# Parameters
inactivity_threshold <- 10
output_file <- "data/results/music_windows_all.csv"
dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

# Load user IDs from the studies of interest
users <- studymanagement %>%
  filter(study_id %in% c(9, 10)) %>%
  pull(user_id) %>%
  as.character()

# Function to extract music windows
preprocessing_music_window <- function(sensing_data, inactivity_threshold = 10) {
  music_events <- sensing_data %>%
    arrange(timestamp.corrected)
  
  if (nrow(music_events) == 0) return(NULL)
  
  music_events <- music_events %>%
    mutate(
      prev_time = lag(timestamp.corrected),
      diff_mins = as.numeric(difftime(timestamp.corrected, prev_time, units = "mins")),
      new_session = is.na(diff_mins) | diff_mins > inactivity_threshold
    )
  
  music_events$session_id <- cumsum(music_events$new_session)
  
  session_bounds <- music_events %>%
    group_by(session_id) %>%
    summarise(
      user_id = first(user_id),
      start_time = min(timestamp.corrected),
      raw_end_time = max(timestamp.corrected),
      n_events = n(),
      .groups = "drop"
    ) %>%
    mutate(
      # Ensure end_time is at least inactivity_threshold minutes after start_time
      end_time = if_else(
        as.numeric(difftime(raw_end_time, start_time, units = "mins")) < inactivity_threshold,
        start_time + minutes(inactivity_threshold),
        raw_end_time
      )
    ) %>%
    select(user_id, start_time, end_time, n_events)
  
  return(session_bounds)
}

# Step 1: Query all MUSIC data in one batch
message("Fetching all MUSIC data from database...")
all_music_data <- tbl(phonestudy, "ps_activity") %>%
  filter(user_id %in% users, activityName == "MUSIC") %>%
  collect() %>%
  mutate(
    timestamp = as.character(timestamp),
    created_at = as.character(created_at),
    updated_at = as.character(updated_at)
  )

# Prepare timestamps
all_music_data <- all_music_data %>%
  mutate(
    user_id = as.character(user_id),
    timestamp.corrected = as.POSIXct(timestamp, tz = "UTC")
  )

# all_music_data <- all_music_data %>%
#   mutate(
#     timestamp.corrected = timestamp
#   )

# Split by user
music_by_user <- split(all_music_data, all_music_data$user_id)

# Setup parallel processing
options(future.globals.maxSize = 2 * 1024^3)
plan(multisession, workers = 16)

# Step 2: Create empty output file with headers
write_csv(tibble(
  user_id = character(),
  start_time = as.POSIXct(character()),
  end_time = as.POSIXct(character()),
  n_events = integer()
), file = output_file)

# Step 3: Process each user and append to CSV
invisible(future_lapply(names(music_by_user), function(user) {
  tryCatch({
    user_data <- music_by_user[[user]]
    
    if (nrow(user_data) == 0) return(NULL)
    
    music_windows <- preprocessing_music_window(user_data, inactivity_threshold)
    
    if (!is.null(music_windows) && nrow(music_windows) > 0) {
      write_csv(music_windows, output_file, append = TRUE)
      message(paste("Music windows added for user:", user))
    } else {
      message(paste("No music sessions for user:", user))
    }
  }, error = function(e) {
    msg <- paste0(Sys.time(), ": ERROR: ", conditionMessage(e), " --> User: ", user)
    write(msg, file = "MusicWindowErrorlog.txt", append = TRUE)
    message(msg)
  })
}))

# Step 4: Shutdown parallel backend
plan(sequential)
message(paste("Finished writing all music windows to:", output_file))
