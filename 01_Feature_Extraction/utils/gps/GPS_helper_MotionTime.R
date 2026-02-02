## Motion Time Function 
#' Extract the proportion of time spent moving 
#' by R.Schoedel, F. Kunz

motionTime = function(sensing.data){
  helper = fill_info(sensing.data, var = "speed", var_desc = NULL)
  # Create new variable, labeling if person is "in motion" (speed > 1 km/h --> 0,277778 m/s)
  helper$motion = NA
  helper$motion[helper$speed >= 0.28] = 1
  helper$motion[helper$speed < 0.28] = 0
  
  df_helper = helper %>% group_by(es_questionnaire_id, speed_session) %>% count()
  df_helper$speed = helper %>% group_by(es_questionnaire_id, speed_session) %>% slice(1) %>% pull(speed)
  df_helper$motion = helper %>% group_by(es_questionnaire_id, speed_session) %>% slice(1) %>% pull(motion)
  df_helper$start.time = helper %>% group_by(es_questionnaire_id, speed_session) %>% slice(1) %>% pull(timestamp.corrected)
  df_helper = df_helper %>% arrange(start.time)
  
  df_helper$end.time = NA
  df_helper$end.time = lead(df_helper$start.time)
  df_helper = df_helper %>% dplyr::filter(!is.na(es_questionnaire_id))
  
  ## Handling of speed sessions that endure ema sessions (cut to 60 minutes)
  df_help = df_helper %>% group_by(es_questionnaire_id) %>% dplyr::count(es_questionnaire_id)
  for(k in df_help$es_questionnaire_id){
    frq = df_help$n[which(df_help$es_questionnaire_id == k)]
    df_helper$end.time[df_helper$es_questionnaire_id == k][frq] = df_helper$start.time[df_helper$es_questionnaire_id == k][1] + minutes(60)
  }
  
  df_helper$duration = NA
  df_helper$duration = difftime(df_helper$end.time, df_helper$start.time, units = "mins")
  df_helper = ungroup(df_helper)
  
  return(df_helper)
}




