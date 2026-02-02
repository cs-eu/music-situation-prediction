#' 1 Sensing Timestamp Preprocessing Steps per user
#' 
#' @author R. Schoedel
#' @family Preprocessing function
#' @import dplyr
#' @import lubridate
#' @import helper_variables.R
#' @details data is a subset from ps_activity 
#' @description this function corrects the timestamps in a dataframe extracted from ps_activity and generates some useful time variables (e.g. weekday). 
#' Please be aware that the timestamp.corrected variable has still a UTC format but actually has already been converted to the correct timezone off the user.
#' That means, you can use timestamp.corrected as it is without any need to converge to a different timezone (i.e., please ignore the UTC specification in timestamp.corrected)
#' @return dataset with new variable timestamp.corrected that should be used for further preprocessing steps
#' @export

ps_activity_preproc_timestamps = function(data) {
  
  # Step 1: Order rows according to logging timestamps (chronologically)
  data <- dplyr::arrange(data, lubridate::ymd_hms(data$timestamp))
  
  # Step 2: Impute missing values in timezoneOffset (only where NA values are present)
  data$timezoneOffset[data$timezoneOffset == 0] <- NA
  
  # Convert timezoneOffset to matrix for imputation
  offset_matrix <- matrix(data$timezoneOffset, ncol = 1)
  
  # Perform KNN imputation
  imputed <- impute.knn(offset_matrix, k = 10)
  
  # Assign the imputed values back
  data$timezoneOffset <- imputed[, 1]
  
  # Step 3: Correct timestamp using timezoneOffset (vectorized)
  timestamp <- lubridate::ymd_hms(data$timestamp) 
  data$timestamp.corrected <- timestamp + lubridate::hours(data$timezoneOffset / (60 * 60 * 1000))
  
  # Step 4: Create useful time-related variables in a vectorized manner
  data$weekday <- lubridate::wday(data$timestamp.corrected, label = TRUE, week_start = 1, locale = "en_US.UTF-8")
  data$date <- lubridate::date(data$timestamp.corrected)
  data$time <- format(data$timestamp.corrected, "%H:%M:%S")
  data$year <- lubridate::year(data$timestamp.corrected)
  
  # Quality check: Filter data for year 2020
  data <- data %>% filter(year == 2020)
  
  # Quality check: Remove duplicate events (distinct by client_db_id)
  data <- data %>% dplyr::distinct(client_db_id, .keep_all = TRUE)
  
  # Create continuous time variables
  data$time_to_hours <- lubridate::hour(data$timestamp.corrected) + 
    lubridate::minute(data$timestamp.corrected) / 60 + 
    lubridate::second(data$timestamp.corrected) / 3600
  data$time_to_sec <- data$time_to_hours * 3600
  
  return(data)
}







#' 2 Experience Sampling Timestamp Preprocessing Steps per user
#' @author R. Schoedel
#' @family Preprocessing function
#' @import dplyr
#' @import lubridate
#' @import helper_variables.R
#' @details ema.data is the output of ema_general_preprocessing.R but filtered for one single user_id
#' @details sensing.data is the corresponding sensing data set that was recorded during the time of the ema wave.
#' @description this function corrects the timestamps in a dataframe extracted from ps_esanswers and generates some useful study variables (e.g. StudyDay, weekday). 
#' Please be aware that the timestamp.corrected variables have still a UTC format but actually have already been converted to the correct timezone off the user.
#' That means, you can use all timestamp.corrected variables as they are without any need to converge to a different timezone (i.e., please ignore the UTC specification in timestamp.corrected)
#' @return ema dataset with new variables timestamp.corrected that should be used for further preprocessing steps.
#' @export


ema_preproc_timestamps = function(ema.data, sensing.data){
  # Step 1: order rows according to logging timestamps/ order timestamps chronologically 
  ema.data = ema.data %>% arrange(lubridate::ymd_hms(ema.data$notificationTimestamp))
  
  # Step 2: look for the mode in the timezoneOffset variable in the timeperiod (+-30min) around the notification timestamp for ema in ps_activity 
  ema.data$notificationTimestamp.corrected = NA
  ema.data$questionnaireStartedTimestamp.corrected = NA
  ema.data$questionnaireEndedTimestamp.corrected = NA
  
  for(i in 1:nrow(ema.data)){
    df.timezone = sensing.data %>% filter(lubridate::ymd_hms(sensing.data$timestamp) > lubridate::ymd_hms(ema.data$notificationTimestamp)[i] - minutes(30) &
                                       lubridate::ymd_hms(sensing.data$timestamp) < lubridate::ymd_hms(ema.data$notificationTimestamp)[i] + minutes(30))
    timezoneoffset.ema = mode.knn(df.timezone$timezoneOffset)
    
    # if no timezone offset available for this specific timestamp, use the most frequent one in the es wave
    if(is.na(timezoneoffset.ema)){
      helper = sensing.data %>% group_by(timezoneOffset) %>% count()
      timezoneoffset.ema = helper$timezoneOffset[which(helper$n == max(helper$n, na.rm = TRUE))]
    }
    
    # Step 3: correct timestamps in ema data table
    ema.data$notificationTimestamp.corrected[i] = lubridate::ymd_hms(ema.data$notificationTimestamp)[i] + lubridate::hours(timezoneoffset.ema/(60*60*1000))
    ema.data$questionnaireStartedTimestamp.corrected[i] = lubridate::ymd_hms(ema.data$questionnaireStartedTimestamp)[i] + lubridate::hours(timezoneoffset.ema/(60*60*1000))
    ema.data$questionnaireEndedTimestamp.corrected[i] = lubridate::ymd_hms(ema.data$questionnaireEndedTimestamp)[i] + lubridate::hours(timezoneoffset.ema/(60*60*1000))
  }
  
  ema.data$notificationTimestamp.corrected = lubridate::as_datetime(ema.data$notificationTimestamp.corrected)
  ema.data$questionnaireStartedTimestamp.corrected = lubridate::as_datetime(ema.data$questionnaireStartedTimestamp.corrected)
  ema.data$questionnaireEndedTimestamp.corrected = lubridate::as_datetime(ema.data$questionnaireEndedTimestamp.corrected)

  
  # Step 4: extract further useful variables 
  ## create weekday
  ema.data$weekday = lubridate::wday(ema.data$questionnaireStartedTimestamp.corrected, label=TRUE, week_start=1,locale = "en_US.UTF-8")
  
  ##  create unique number for ema questionnaires 
  ema.data$nr = 1:nrow(ema.data) 
  
  return(ema.data)
}


#' 3 Keyboard data Timestamp Preprocessing Steps per user
# same procedure as for ema_preproc_timestamps(), description see above

keyboard_preproc_timestamps = function(keyboard.data, sensing.data){
  # Step 1: order rows according to logging timestamps/ order timestamps chronologically 
  keyboard.data = keyboard.data %>% arrange(lubridate::ymd_hms(keyboard.data$timestamp_type_start))
  
  # Step 2: look for the mode in the timezoneOffset variable in the timeperiod (+-1min) around the respective timestamp in ps_activity 
  keyboard.data$timestamp_type_start.corrected = NA
  keyboard.data$timestamp_type_end.corrected = NA
  
  for(i in 1:nrow(keyboard.data)){
    df.timezone = sensing.data %>% filter(lubridate::ymd_hms(sensing.data$timestamp) > lubridate::ymd_hms(keyboard.data$timestamp_type_start)[i] - minutes(1) &
                                            lubridate::ymd_hms(sensing.data$timestamp) < lubridate::ymd_hms(keyboard.data$timestamp_type_start)[i] + minutes(1))
    timezoneoffset.keyboard = mode.knn(df.timezone$timezoneOffset)
    
    # Step 3: correct timestamps in keyboard data table
    keyboard.data$timestamp_type_start.corrected[i] = lubridate::ymd_hms(keyboard.data$timestamp_type_start)[i] + lubridate::hours(timezoneoffset.keyboard/(60*60*1000))
    keyboard.data$timestamp_type_end.corrected[i] = lubridate::ymd_hms(keyboard.data$timestamp_type_end)[i] + lubridate::hours(timezoneoffset.keyboard/(60*60*1000))
  }
  
  keyboard.data$timestamp_type_start.corrected = lubridate::as_datetime(keyboard.data$timestamp_type_start.corrected)
  keyboard.data$timestamp_type_end.corrected = lubridate::as_datetime(keyboard.data$timestamp_type_end.corrected)
  
  return(keyboard.data)
}


