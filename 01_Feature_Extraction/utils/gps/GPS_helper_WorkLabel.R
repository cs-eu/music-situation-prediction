## Find Work function 
#' Find location clusters with DBSCAN clustering using the Haversine metric.
#' eps (in meters) and minPts are the input parameters for DBSCAN.
#' Returns the mean point of the work cluster, i.e. the cluster where the user is most often at the given time +- the time tolerance.
#' The time tolerance is given in seconds, default is 60*60 seconds = 1 hour.
#' PARAMETER:
#' epsilon (“eps”) and minimum points (“MinPts”). The parameter eps defines the radius of neighborhood around a point x. It’s called called the ϵ-neighborhood of x. The parameter MinPts is the minimum number of neighbors within “eps” radius.
#' Any point x in the data set, with a neighbor count greater than or equal to MinPts, is marked as a core point. We say that x is border point, if the number of its neighbors is less than MinPts, but it belongs to the ϵ-neighborhood of some core point z. Finally, if a point is neither a core nor a border point, then it is called a noise point or an outlier.
#' findWork() returns the mean point of the work cluster
#' only takes a random sample of data points; otherwise computational overload!
#' by Quay Au (Stachl et al., 2020) and adapted by Fiona Kunz & Ramona Schoedel


findWork = function(sensing.data, eps = 30, minPts = 3, time = "12:00:00", timeTolerance = 4*60*60, randomSample = 5000){
  
  # create empty result dataframe
  res = data.frame(user_id = unique(sensing.data$user_id)[!is.na(unique(sensing.data$user_id))], 
                   gps.longitude = NA, gps.latitude = NA)
  
  sensing_GPS = sensing.data %>% dplyr::filter_all(dplyr::any_vars(!is.na(latitude))) %>% 
    dplyr::filter(longitude != 0 | latitude != 0) %>% distinct(user_id, latitude, longitude, timestamp.corrected, .keep_all =TRUE)
  
  # assumption: users are usually only at work during the week; therefore exclude weekend days
  sensing_GPS = sensing_GPS %>% filter(!weekday %in% c("Fri", "Sat", "Sun"))
  
  # take random sample of GPS poinst otherwise R crashes
  if(nrow(sensing_GPS) > randomSample){
    random_gps.sample = sample(1:nrow(sensing_GPS), randomSample, replace = FALSE)
    sensing_GPS = sensing_GPS[random_gps.sample, ]
  }
  
  # make sure that GPS data is numeric 
  sensing_GPS$latitude = as.numeric(as.character(sensing_GPS$latitude))
  sensing_GPS$longitude = as.numeric(as.character(sensing_GPS$longitude))
  
  # filter for times of Interest for detecting home (see time argument)
  timeInSec = sapply(strsplit(time, ":"), timeToSec)
  timesOfInterest = which(sensing_GPS$time_to_sec >= timeInSec - timeTolerance & sensing_GPS$time_to_sec <= timeInSec + timeTolerance)
  sensing_GPS = sensing_GPS[timesOfInterest, ]
  
  # scan for most often visited cluster during these times and identify mean point of home cluster
  if (nrow(sensing_GPS) == 0){
    print(paste("User", unique(sensing_GPS$user_id)[!is.na(unique(sensing_GPS$user_id))], "has no sufficient GPS data in given time frame."))
    res[1, "gps.longitude"] = NA
    res[1, "gps.latitude"] = NA
  }else{
    
    # error handling
    df1 = sensing_GPS %>% filter(longitude < 360 & latitude < 270)
    
    # Calculate distance matrix of all set of points (see "Great-circle distance") as input object for DBSCAN
    distMat = geosphere::distm(cbind(df1$longitude, df1$latitude), fun = geosphere::distHaversine)
    
    # DBSCAN Clustering: Fast implementation of the DBSCAN clustering algorithm using a kd-tree.
    clustering = dbscan::dbscan(distMat, eps, minPts)$cluster
    if (all(clustering == 0)) {
      print(paste("not enough GPS data for user", unique(res$user_id), ", everything labeled as noise"))
      res[1, "gps.longitude"] = NA
      res[1, "gps.latitude"] = NA
    }else{
      # choose the cluster that was visited most often
      # (noise points are marked with clustering = 0, so exclude those)
      homeCluster = which(tabulate(clustering[clustering != 0]) == max(tabulate(clustering[clustering != 0])))
      if (length(homeCluster) > 1) {
        print("There are two or more clusters in which the user is most often at the given time.
            The first cluster will be chosen.")
        homeCluster = homeCluster[1]
      }
      
      # return mean point of this cluster
      res[1, "gps.longitude"] <- mean(unlist(df1[clustering == homeCluster, "longitude"]))
      res[1, "gps.latitude"] <- mean(unlist(df1[clustering == homeCluster, "latitude"]))
    }
  }
  
  return(res)
}

