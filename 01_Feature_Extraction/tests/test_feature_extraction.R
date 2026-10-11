# Run with Rscript; only data.table is required, never database access or real data.
library(data.table)

script_arg <- commandArgs(FALSE)[startsWith(commandArgs(FALSE), "--file=")]
test_file <- normalizePath(sub("^--file=", "", script_arg[1L]))
extraction_dir <- dirname(dirname(test_file))
source_code <- parse(file.path(extraction_dir, "03_SOURCE_FeatureExtract.R"))
source(file.path(extraction_dir, "utils", "label_usage_sessions.R"))

assignment_name <- function(expr) {
  if (is.call(expr) && identical(expr[[1L]], as.name("<-")) && is.symbol(expr[[2L]])) {
    as.character(expr[[2L]])
  } else ""
}
assignment_names <- vapply(source_code, assignment_name, character(1L))
setup_start <- match("spotify_feature_cols", assignment_names)
setup_end <- which(vapply(source_code, function(expr) {
  is.call(expr) && identical(expr[[1L]], as.name("setkey")) &&
    identical(expr[[2L]], as.name("music_features_master"))
}, logical(1L)))
stopifnot(length(setup_end) == 1L, setup_start < setup_end)

expect_error <- function(expr, pattern) {
  error <- tryCatch({ force(expr); NULL }, error = identity)
  stopifnot(inherits(error, "error"), grepl(pattern, conditionMessage(error)))
}
t0 <- as.POSIXct("2020-01-01 12:00:00", tz = "UTC")
t1 <- t0 + 600

# Evaluate only constants, pure table joins and function definitions from the
# parsed source, not its libraries, credentials, connection or execution loop.
make_environment <- function(duplicate_lyrics = FALSE, duplicate_track = FALSE) {
  env <- new.env(parent = globalenv())
  env$app_categorization <- data.table(packageName = "known", Final_Rating = "Test")
  env$spotify_data <- data.table(
    track_spotify_id = c("1", "2", NA_character_, "4"), track = c("A", "B", "C", "D"),
    artist = "Artist", track_energy = c(0.1, 0.9, 0.6, 0.4), track_key = c(0, 11, 4, 8)
  )
  env$genius_features <- data.table(track_spotify_id = c("1", "2", "4"), anger = c(0.2, 0.4, 0.8))
  env$topic_features <- data.table(track_spotify_id = c("1", "2"), `Topic 0` = c(0.3, 0.7))
  env$liwc_features <- data.table(track_spotify_id = c("1", "2", NA_character_), anger = c(7, 9, 999))
  if (duplicate_lyrics) env$liwc_features <- rbind(env$liwc_features, env$liwc_features[1L])
  if (duplicate_track) env$spotify_data <- rbind(env$spotify_data, env$spotify_data[1L])
  constants <- c(
    "app_categories", "headset_states", "screen_categories", "detected_activity_keys",
    "all_activity_names", "gps_features", "spotify_numeric_features",
    "genius_numeric_features", "topic_numeric_features", "liwc_numeric_features"
  )
  for (name in constants) eval(source_code[[match(name, assignment_names)]], env)
  for (i in seq.int(setup_start, setup_end)) eval(source_code[[i]], env)
  for (name in c("get_time_bin", "ensure_cols", "extract_features_for_window")) {
    eval(source_code[[match(name, assignment_names)]], env)
  }
  # A base-R equivalent isolates the unrelated lubridate dependency.
  env$hour <- function(time) as.POSIXlt(time)$hour
  env$activity_data_raw <- data.table(
    user_id = c(rep("u1", 7), "u2", "u2", "u3"),
    timestamp = t0 + c(0, 10, 20, 30, 100, -60, 300, 0, -30, 0),
    activityName = c(rep("MUSIC", 4), "PHONE", "SCREEN", "SCREEN", "MUSIC", "SCREEN", "MUSIC"),
    music_id = c(1L, 1L, 1L, 2L, NA, NA, NA, 3L, NA, 4L),
    snapshot_id = c(1L, rep(NA_integer_, 9)),
    packageName = c(rep(NA_character_, 4), "known", rep(NA_character_, 5)),
    event = c(rep(NA_character_, 5), "OFF_LOCKED", "ON_UNLOCKED", NA, "ON_LOCKED", NA)
  )
  env$music_raw <- data.table(id = 1:4, title = c("A", "B", "C", "D"), artist = "Artist")
  env$snapshot_raw <- data.table(id = 1L, headsetState = "PLUGGED")
  env$snapshot_activities <- data.table(id = 1L)
  for (name in env$detected_activity_keys) set(env$snapshot_activities, j = name, value = 1)
  setkey(env$snapshot_activities, id)
  env$gps_landuse_df <- data.table(
    user_id = c("u1", "u1", "u1", "u2", "u3"),
    start_time = t0 + c(0, -10, 300, 0, 0), end_time = t1 + c(0, 20, 300, 0, 0),
    gps_count = c(11, 999, 888, 22, 33), gps_first_latitude = 1:5,
    gps_first_longitude = 6:10, gps_landuse_type = "Test", gps_home = 0L, gps_work = 0L
  )
  # Deliberately stale global row: extraction must use its arguments, not this.
  env$row <- list(user_id = "u3", start_time = t0, end_time = t1)
  env
}

env <- make_environment()
master <- env$music_features_master
stopifnot(nrow(master) == 4L, !anyDuplicated(names(master)))
stopifnot(master[track == "A", genius_anger] == 0.2, master[track == "A", liwc_anger] == 7)
stopifnot(master[track == "C", is.na(genius_anger) & is.na(liwc_anger) & is.na(topic_Topic_0)])
stopifnot(master[track == "D", genius_anger] == 0.8, master[track == "D", is.na(liwc_anger)])
expect_error(make_environment(duplicate_lyrics = TRUE), "one row per track_spotify_id")
expect_error(make_environment(duplicate_track = TRUE), "one row per track/artist")

extract <- env$extract_features_for_window
u1 <- suppressMessages(extract("u1", t0, t1))
u2 <- suppressMessages(extract("u2", t0, t1))
stopifnot(nrow(u1) == 1L, nrow(u2) == 1L, u1$user_id == "u1", u2$user_id == "u2")
stopifnot(u1$gps_count == 11, u2$gps_count == 22)
# Three plays of A and one of B still have equal unique-track weights.
stopifnot(abs(u1$music_track_energy - 0.5) < 1e-12, u2$music_track_energy == 0.6)
stopifnot(abs(u1$genius_anger - 0.3) < 1e-12, u1$liwc_anger == 8, u1$topic_Topic_0 == 0.5)
stopifnot(is.na(u2$genius_anger), is.na(u2$liwc_anger), is.na(u2$topic_Topic_0))
stopifnot(u1$phone_PHONE == 1, u2$phone_PHONE == 0, u1$app_Test == 1, u2$app_Test == 0)
stopifnot(u1$screen_OFF_LOCKED == 300, u1$screen_ON_UNLOCKED == 300, u2$screen_ON_LOCKED == 600)
stopifnot(!"music_track_key" %in% names(u1), !any(grepl("^(music_music_|genius_genius_|liwc_liwc_|topic_topic_)", names(u1))))
screen_cols <- paste0("screen_", env$screen_categories)
stopifnot(sum(unlist(u1[, ..screen_cols])) == 600, sum(unlist(u2[, ..screen_cols])) == 600)
eval(source_code[[match("fixed_column_names", assignment_names)]], env)
stopifnot(setequal(names(u1), intersect(env$fixed_column_names, names(u1))))
rm("row", envir = env)
stopifnot(suppressMessages(extract("u1", t0, t1))$gps_count == 11)
stopifnot(is.null(extract("absent", t0, t1)), is.null(extract("u1", t0, t0)),
          is.null(extract("u1", t0, t0 - 1)), is.null(extract("u1", t0, as.POSIXct(NA))),
          is.null(extract("u1", as.POSIXct(character()), t1)),
          is.null(extract("u1", t0, as.POSIXct(Inf, origin = "1970-01-01", tz = "UTC"))))
env$gps_landuse_df <- rbind(env$gps_landuse_df, env$gps_landuse_df[1L])
expect_error(suppressMessages(extract("u1", t0, t1)), "Multiple GPS rows")

events <- function(offsets, states) data.table(timestamp = t0 + offsets, activityName = "SCREEN", event = states)
check_durations <- function(input, expected, lookback_seconds = 7200) {
  original <- copy(input)
  result <- preprocessing_screen_window(input, t0, t1, lookback_seconds = lookback_seconds)
  stopifnot(identical(input, original), all(result$duration_sec >= 0), sum(result$duration_sec) == 600)
  observed <- setNames(result$duration_sec, result$event)
  stopifnot(setequal(names(observed), names(expected)), all(observed[names(expected)] == expected))
}
check_durations(events(c(400, -120, -60, 100, 600),
                       c("OFF_UNLOCKED", "ON_LOCKED", "OFF_LOCKED", "ON_UNLOCKED", "ON_LOCKED")),
                c(OFF_LOCKED = 100, ON_UNLOCKED = 300, OFF_UNLOCKED = 200))
check_durations(events(100, "ON_UNLOCKED"), c(UNKNOWN = 100, ON_UNLOCKED = 500))
check_durations(events(-1, "OFF_LOCKED"), c(OFF_LOCKED = 600))
check_durations(events(-7200, "OFF_LOCKED"), c(OFF_LOCKED = 600))
check_durations(events(-7201, "OFF_LOCKED"), c(UNKNOWN = 600))
check_durations(events(-60, "OFF_LOCKED"), c(UNKNOWN = 600), lookback_seconds = 30)
check_durations(events(c(-1, 0), c("OFF_LOCKED", "ON_UNLOCKED")), c(ON_UNLOCKED = 600), lookback_seconds = 0)
check_durations(events(c(0, 0), c("OFF_LOCKED", "ON_UNLOCKED")), c(ON_UNLOCKED = 600))
check_durations(events(c(0, 100, NA), c("ON_UNLOCKED", NA, "OFF_LOCKED")), c(ON_UNLOCKED = 100, UNKNOWN = 500))
check_durations(events(0, "unsupported"), c(UNKNOWN = 600))
check_durations(data.table(), c(UNKNOWN = 600))
check_durations(events(NA_real_, "ON_UNLOCKED"), c(UNKNOWN = 600))
stopifnot(nrow(preprocessing_screen_window(NULL, t0, t0)) == 0L,
          nrow(preprocessing_screen_window(NULL, t1, t0)) == 0L,
          nrow(preprocessing_screen_window(NULL, as.POSIXct(NA), t1)) == 0L,
          nrow(preprocessing_screen_window(NULL, as.POSIXct(character()), t1)) == 0L,
          nrow(preprocessing_screen_window(NULL, t0, as.POSIXct(Inf, origin = "1970-01-01", tz = "UTC"))) == 0L)
expect_error(preprocessing_screen_window(NULL, "2020-01-01 12:00:00", t1), "explicit source timezone")
expect_error(preprocessing_screen_window(data.table(timestamp = "2020-01-01 12:00:00", activityName = "SCREEN", event = "OFF_LOCKED"), t0, t1), "explicit source timezone")
expect_error(preprocessing_screen_window(NULL, t0, t1, lookback_seconds = -1), "nonnegative")
# Preserve supplied instants across a daylight-saving transition without guessing zones.
dst_start <- as.POSIXct("2020-03-08 01:30:00", tz = "America/New_York")
dst_result <- preprocessing_screen_window(
  data.table(timestamp = dst_start, activityName = "SCREEN", event = "OFF_LOCKED"),
  dst_start, dst_start + 7200
)
stopifnot(dst_result$duration_sec == 7200)

# Exercise the actual main expressions with synthetic windows and a fake
# extractor. Override only configuration assignments, never evaluate DB setup.
main_start <- match("offset", assignment_names)
test_root <- tempfile("feature-checkpoint-tests-")
stopifnot(dir.exists(dirname(test_root)), dir.create(test_root))
test_windows <- data.table(
  user_id = c("success1", "skip", "success3"),
  start_time = t0 + c(0, 1000, 2000), end_time = t1 + c(0, 1000, 2000)
)
run_main <- function(output_dir, windows = test_windows, offset = 0L) {
  main_env <- make_environment()
  main_env$music_windows <- copy(windows)
  main_env$called <- character()
  main_env$extract_features_for_window <- function(uid, window_start, window_end) {
    main_env$called <- c(main_env$called, uid)
    if (uid == "skip") return(NULL)
    if (uid == "error") stop("Synthetic extraction failure")
    result <- data.table(user_id = uid, start_time = window_start, end_time = window_end,
                         time_bin = "Noon", music_track_energy = 0.5)
    if (uid == "multi") result <- rbind(result, result)
    result
  }
  suppressMessages(for (i in seq.int(main_start, length(source_code))) {
    if (assignment_names[i] == "offset") {
      main_env$offset <- offset
    } else if (assignment_names[i] == "output_dir") {
      main_env$output_dir <- output_dir
    } else eval(source_code[[i]], main_env)
  })
  main_env
}

full_dir <- file.path(test_root, "full")
full <- run_main(full_dir)
full_checkpoint <- readRDS(full$checkpoint_file)
full_output <- readRDS(full$output_file)
stopifnot(identical(full$called, test_windows$user_id), length(full$all_features_list) == 3L,
          length(full_checkpoint$features) == 3L, full_checkpoint$last_processed == 3L,
          full_checkpoint$features[[1L]]$user_id == "success1",
          is.null(full_checkpoint$features[[2L]]), full_checkpoint$features[[3L]]$user_id == "success3",
          identical(full_output$user_id, c("success1", "success3")),
          attr(full_output, "extraction_provenance")$schema_version == 2L)
completed <- run_main(full_dir)
stopifnot(length(completed$called) == 0L, length(completed$pending_indices) == 0L,
          identical(readRDS(completed$output_file)$user_id, full_output$user_id))

# The most recently processed slot is a skipped window, not the success count.
partial_dir <- file.path(test_root, "partial")
stopifnot(dir.create(partial_dir))
partial_checkpoint <- full_checkpoint
partial_checkpoint$last_processed <- 2L
partial_checkpoint$features[3L] <- list(NULL)
saveRDS(partial_checkpoint, file.path(partial_dir, "features_checkpoint_v2.rds"))
resumed <- run_main(partial_dir)
stopifnot(identical(resumed$called, "success3"), resumed$start_index == 3L,
          length(resumed$all_features_list) == 3L, is.null(resumed$all_features_list[[2L]]),
          identical(readRDS(resumed$output_file)$user_id, full_output$user_id))

# Neither formerly used directory is scanned, even with a high-progress cache.
for (legacy_dir in c("data/new/results", "data/results/new")) {
  legacy_dir <- file.path(test_root, legacy_dir)
  stopifnot(dir.create(legacy_dir, recursive = TRUE))
  saveRDS(list(data.table(user_id = "unsafe_legacy")), file.path(legacy_dir, "features_partial_999.rds"))
}
legacy_ignored <- run_main(file.path(test_root, "data/results/participant_safe_v2"))
stopifnot(identical(legacy_ignored$called, test_windows$user_id),
          identical(readRDS(legacy_ignored$output_file)$user_id, full_output$user_id))

invalid_dir <- file.path(test_root, "invalid")
stopifnot(dir.create(invalid_dir))
invalid_checkpoints <- list(
  legacy_payload = full_checkpoint$features,
  old_schema = modifyList(full_checkpoint, list(schema_version = 1L)),
  wrong_indices = modifyList(full_checkpoint, list(window_indices = 3:1)),
  excessive_progress = modifyList(full_checkpoint, list(last_processed = 4L)),
  missing_progress = modifyList(full_checkpoint, list(last_processed = NA_integer_)),
  fractional_progress = modifyList(full_checkpoint, list(last_processed = 1.5))
)
bad_keys <- full_checkpoint
bad_keys$window_keys$user_id[1L] <- "changed_participant"
invalid_checkpoints$wrong_window_keys <- bad_keys
bad_length <- full_checkpoint
bad_length$features <- bad_length$features[1:2]
invalid_checkpoints$wrong_list_length <- bad_length
bad_rows <- full_checkpoint
bad_rows$features[1L] <- list(rbind(bad_rows$features[[1L]], bad_rows$features[[1L]]))
invalid_checkpoints$multiple_rows <- bad_rows
unprocessed_result <- full_checkpoint
unprocessed_result$last_processed <- 2L
invalid_checkpoints$nonempty_pending_slot <- unprocessed_result
for (checkpoint in invalid_checkpoints) {
  saveRDS(checkpoint, file.path(invalid_dir, "features_checkpoint_v2.rds"))
  expect_error(run_main(invalid_dir), "Invalid feature checkpoint")
}

multiple_windows <- copy(test_windows)
multiple_windows$user_id[2L] <- "multi"
warning_messages <- character()
multiple <- withCallingHandlers(run_main(file.path(test_root, "multiple"), multiple_windows),
  warning = function(w) {
    warning_messages <<- c(warning_messages, conditionMessage(w))
    invokeRestart("muffleWarning")
  }
)
stopifnot(any(grepl("rejecting window", warning_messages)),
          length(multiple$all_features_list) == 3L, is.null(multiple$all_features_list[[2L]]),
          identical(readRDS(multiple$output_file)$user_id, full_output$user_id))
error_windows <- copy(test_windows)
error_windows$user_id[2L] <- "error"
failed <- run_main(file.path(test_root, "failed"), error_windows)
stopifnot(length(failed$all_features_list) == 3L, is.null(failed$all_features_list[[2L]]))

offset_run <- run_main(file.path(test_root, "offset"), offset = 1L)
stopifnot(identical(offset_run$window_indices, 2:3), identical(offset_run$called, c("skip", "success3")),
          length(offset_run$all_features_list) == 2L, offset_run$last_processed == 2L)
offset_complete <- run_main(file.path(test_root, "offset_complete"), offset = 3L)
empty <- run_main(file.path(test_root, "empty"), test_windows[0L])
stopifnot(length(offset_complete$called) == 0L, length(offset_complete$window_indices) == 0L,
          length(empty$called) == 0L, length(empty$all_features_list) == 0L,
          nrow(readRDS(empty$output_file)) == 0L, readRDS(empty$checkpoint_file)$last_processed == 0L)
for (bad_offset in list(-1, 4, 0.5, NA_real_, Inf, numeric(), c(0, 1), "1")) {
  expect_error(run_main(file.path(test_root, "bad_offset"), offset = bad_offset), "offset must")
}
unlink(test_root, recursive = TRUE)

cat("Feature extraction, screen and checkpoint regression tests passed.\n")
