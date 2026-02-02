# Collection of diverse helper functions for other preprocessing scripts

# function to determine study day number since specidied date e.g., "2020-07-27" 
count.study.days = function(startdate, timestamp.raw){
  date.raw = lubridate::date(timestamp.raw)
  length.study = lubridate::as.period(lubridate::interval(startdate, date.raw)) %/% days()
  studyDay.nr = length.study+1
  return(studyDay.nr)
}


# calculate mode of a vector
mode.knn =  function(x){
  uniq.x = unique(x)
  uniq.x = uniq.x[which(!is.na(uniq.x))]
  knn = uniq.x[which.max(tabulate(match(x, uniq.x)))]
  return(knn)
}


# find [k] neigherst neigbors and impute by their mode 
impute.knn = function(y, k){
  t = which(is.na(y))
  if(length(t) == 0){
    return(y)
  }else{
    is = 1:length(t)
    for(i in is){
      if(i > k){
        look.at = y[(t[i]-k):(t[i]+k)]
        y[t[i]] = mode.knn(look.at)
      }
      if(i <= k){
        look.at = y[1:(t[i]+k)]
        y[t[i]] = mode.knn(look.at)
      }
    }
    return(y)
  }
}


# find pattern in data
occur <- function(patrn, x) {
  patrn.rev <- rev(patrn)
  w <- embed(x,length(patrn))
  which(apply(w, 1, function(r) all(r == patrn.rev)))
}


# helper functions for time
timeToSec = function(x) {
  x = as.numeric(x)
  3600*x[1] + 60*x[2] + x[3]
}


# helper to make script slower 
testit <- function(x){
  p1 <- proc.time()
  Sys.sleep(x)
  proc.time() - p1 # The cpu usage should be negligible
}


