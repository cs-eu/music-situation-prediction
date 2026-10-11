preprocessing_screen_window <- function(sensing_data, window_start, window_end, lookback_seconds = 7200) {
  empty_result <- data.table(event = character(), duration_sec = numeric())
  if (length(window_start) != 1L || length(window_end) != 1L ||
      is.na(window_start) || is.na(window_end)) return(empty_result)
  if (!inherits(window_start, "POSIXt") || !inherits(window_end, "POSIXt")) {
    stop("Window timestamps must be POSIXt; parse them with an explicit source timezone")
  }
  window_seconds <- as.numeric(difftime(window_end, window_start, units = "secs"))
  if (!is.finite(window_seconds) || window_seconds <= 0) return(empty_result)
  if (length(lookback_seconds) != 1L || !is.numeric(lookback_seconds) ||
      !is.finite(lookback_seconds) || lookback_seconds < 0) {
    stop("lookback_seconds must be a finite nonnegative number")
  }
  unknown_result <- data.table(event = "UNKNOWN", duration_sec = window_seconds)
  if (is.null(sensing_data) || nrow(sensing_data) == 0L) return(unknown_result)

  # Select into a new table so sorting/normalization never mutates the caller.
  screen_events <- as.data.table(sensing_data)[activityName == "SCREEN", .(timestamp, event)]
  if (nrow(screen_events) == 0L || all(is.na(screen_events$timestamp))) return(unknown_result)
  if (!inherits(screen_events$timestamp, "POSIXt")) {
    stop("Screen timestamps must be POSIXt; parse them with an explicit source timezone")
  }
  screen_events <- screen_events[!is.na(timestamp) &
                                   timestamp >= window_start - lookback_seconds &
                                   timestamp <= window_end]
  if (nrow(screen_events) == 0L) return(unknown_result)
  screen_events[, event := trimws(as.character(event))]
  screen_events[is.na(event) | !event %in% c("ON_UNLOCKED", "OFF_UNLOCKED", "OFF_LOCKED", "ON_LOCKED"),
                event := "UNKNOWN"]
  setorder(screen_events, timestamp)
  # For simultaneous events, the last input event determines subsequent state.
  screen_events <- screen_events[!duplicated(timestamp, fromLast = TRUE)]
  prior_events <- screen_events[timestamp <= window_start]
  initial_event <- if (nrow(prior_events) > 0L) prior_events$event[nrow(prior_events)] else "UNKNOWN"
  screen_events <- rbindlist(list(
    data.table(timestamp = window_start, event = initial_event),
    screen_events[timestamp > window_start & timestamp < window_end]
  ))

  # Include uncovered leading time and carry the final state to the window end.
  screen_events[, interval_end := shift(timestamp, type = "lead", fill = window_end)]
  screen_events[, duration_sec := as.numeric(difftime(interval_end, timestamp, units = "secs"))]
  screen_events[, .(duration_sec = sum(duration_sec)), by = event][order(-duration_sec)]
}
