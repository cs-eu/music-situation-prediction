#' by Quay Au (Stachl et al., 2020), adapted by Ramona Schoedel
#' Add columns with information on specific places (e.g., HOME, WORK) to sensing data.
#'
#' This function takes a sensing logs dataset and adds a variable gps.atX that assigns to each row
#' the label "AT X" or "NOT AT X". Also, two columns gps.XLongitude and gps.XLatitude with the home GPS coordinates in each row are added.
#' The coordinates of the place of interest can for example be calculated with the helper function findHome() or findWork().
#'
#' @family location functions
#' @param sensing.data main input data frame that should be labeled with the place of interest 
#' @param gps_place data frame with coordinates of the place of interest 
#' @param place name of the place of interest (e.g., HOME, WORK)
#' @param eps GPS coordinates which have distance <= \code{eps} (in meters) from the home coordinates (= CLUSTER MEAN) are labeled as "AT X", the other ones as "NOT AT X".
#' @return Returns the input dataframe with three additional columns: gps.atX, gps.XLatitude and gps.XLongitude.
#' @export
#'
#' @examples

locationLabelPlace = function(sensing.data, gps_place, place = "X", eps = 30){
  
  sensing.data$latitude = as.numeric(as.character(sensing.data$latitude))
  sensing.data$longitude = as.numeric(as.character(sensing.data$longitude))
  
  # create empty new Variables
  df1 = sensing.data
  vars = c(paste0("gps.at", place), paste0("gps.", place, "Latitude"), paste0("gps.", place, "Longitude"))
  df1 = cbind(df1, setNames( lapply(vars, function(x) x=NA), vars))
  
  if(is.na(gps_place$gps.latitude)){
    print(paste("failed to find", place, "for user", unique(df1$user_id)))
    df0 = df1
  }else{
    #error handling (Fehler in .pointsToMatrix(x) : longitude > 360)
    df1$longitude[df1$longitude > 360] = NA
    df1$latitude[df1$latitude > 270] = NA 
    
    distFromHome = geosphere::distHaversine(p1 = cbind(df1$longitude, df1$latitude), p2 = cbind(gps_place$gps.longitude, gps_place$gps.latitude))
    gps.var = which(colnames(df1) == paste0("gps.at", place))
    df1[distFromHome <= eps & !is.na(distFromHome), gps.var] = paste0("AT ", place)
    df1[distFromHome > eps & !is.na(distFromHome), gps.var] = paste0("NOT AT ", place)
    df1[which(colnames(df1) == paste0("gps.", place, "Latitude"))] = gps_place$gps.latitude
    df1[which(colnames(df1) == paste0("gps.", place, "Longitude"))] = gps_place$gps.longitude
    df0 = df1
  }
  
  df0 = df0 %>% arrange(timestamp.corrected)
  
  return(df0)
}

