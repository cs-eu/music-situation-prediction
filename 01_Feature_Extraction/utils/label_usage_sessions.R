preprocessing_screen_window <- function(sensing_data, start_time, end_time) {
  # Ensure data.table format
  setDT(sensing_data)
  sensing_data[, timestamp := as.POSIXct(timestamp)]
  
  # Sort by time
  setorder(sensing_data, timestamp)
  
  # Step 1: Filter screen events + add a buffer for state determination
  buffer_seconds <- 7200  # Add 2 hour of lookback
  window_start <- as.POSIXct(start_time)
  window_end <- as.POSIXct(end_time)
  buffer_start <- window_start - buffer_seconds
  
  screen_events <- sensing_data[
    activityName == "SCREEN" &
      timestamp >= buffer_start &
      timestamp <= window_end
  ][, .(timestamp = timestamp, event)]
  
  if (nrow(screen_events) == 0) {
    return(data.table(event = "UNKNOWN", duration_sec = as.numeric(difftime(window_end, window_start, units = "secs"))))
  }
  
  # Step 2: Build screen state intervals
  screen_events[, end_time := shift(timestamp, type = "lead")]
  screen_events <- screen_events[!is.na(end_time)]
  
  # Step 3: Clip intervals to the desired window
  screen_events[, `:=`(
    interval_start = pmax(timestamp, window_start),
    interval_end = pmin(end_time, window_end)
  )]
  
  # Filter to intervals that overlap the window
  screen_events <- screen_events[interval_start < interval_end]
  
  # Step 4: Aggregate duration by screen event/state
  screen_events[, duration_sec := as.numeric(difftime(interval_end, interval_start, units = "secs"))]
  result <- screen_events[, .(duration_sec = sum(duration_sec)), by = event][order(-duration_sec)]
  
  return(result)
}

# }