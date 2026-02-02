## GEOHASHES
#' Create Location Geohashs: Apply Geohash-Algorithm and add "geohash" variable to sensing dataset (http://geohash.org/)
#' R-package "GeohashTools" is used: https://cran.r-project.org/web/packages/geohashTools/geohashTools.pdf
#' based on Gustavo Niemeyer (2008) Geohashing
#' by Fiona Kunz

library(geohashTools)

locationGeohashs = function(sensing.data, precision = 6L){
  
    # Create empty dataframe for results: GPS coordinates & respective Geohash
    geohash_df <- data.frame(user_id = unique(sensing.data$user_id)[!is.na(unique(sensing.data$user_id))])
    
    ## Keep only distinct GPS coordinates!
    df1 = sensing.data %>% dplyr::select(user_id, latitude, longitude) %>% dplyr::distinct() 
      
    # Apply rowwise
    for (i in 1:nrow(df1)) {
      ## Step 1: take GPS coordinates as character ("latitude" variable not "gps.latitude")
      curr_gps.latitude = df1[i, "latitude"] 
      curr_gps.longitude = df1[i, "longitude"]
      
      ## Step 2: Apply geohash-Algo row-wise and add "geohash"-variable indicating the current geohash 
      df1[i, "geohash"] <- gh_encode(curr_gps.latitude, curr_gps.longitude, precision = precision) 
    }
    
    # add column with geohash to initial dataframe
    df2 = dplyr::left_join(sensing.data, df1, by = c("user_id", "latitude", "longitude"))  
    ## fill up information
    df2 = fill_info(df2, var = "geohash", var_desc = NULL, on)
    
    # create summary table for later feature extraction (inter situm)
    df_geohash = df2 %>% group_by(geohash, geohash_session) %>% count()
    df_geohash$start.geohash = df2 %>% group_by(geohash, geohash_session) %>% slice(c(1)) %>% pull(timestamp.corrected)
    df_geohash = df_geohash %>% arrange(start.geohash) %>% filter(!is.na(geohash_session))
    df_geohash$duration = difftime(lead(df_geohash$start.geohash), df_geohash$start.geohash, units = "mins")
    summary_geohash = df_geohash %>% group_by(geohash) %>% summarise(num_visits = n(), dur_visits = sum(duration, na.rm = TRUE))

    return(list(df2, summary_geohash))
}


