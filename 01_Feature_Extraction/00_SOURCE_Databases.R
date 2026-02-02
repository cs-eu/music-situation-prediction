# 00_SOURCE_Databases.R (adapted with changes from Schoedel et al., 2023)

## NOTE. This script will not run on your computer because we cannot provide access to participants' raw logging data due to data privacy concerns.
## The raw logging data were stored in an SQL database on our Rstudio server.
## Nevertheless, we provide readers with our preprocessing code to make all steps in our data handling transparent.

# LOAD REQUIRED RESSOURCES

## Load packages
library(RMariaDB)
library(DBI)
library(plyr)
library(dbplyr)
library(tidyr)
library(lubridate)
library(stats)
options(scipen = 999)

invisible(capture.output(source('/local/.meta/dbcredentials.R')))

# GET CONNECTION to our SQL Database in which the raw sensing and experience sampling data are stored 
phonestudy = dbConnect(
  drv = RMariaDB::MariaDB(),
  username = mariadb_user,
  password = mariadb_pw,
  host = "localhost", 
  port = 3306,
  dbname = "live")

# SET WORKING DIRECTORY
setwd("")

#user-ids, starting-times, etc. 
studymanagement = DBI::dbReadTable(phonestudy, "ps_participant")
# music data 
music =  DBI::dbReadTable(phonestudy, "ps_music")
# snapshot data
snapshot <- dbGetQuery(phonestudy, "SELECT id, headsetState, detectedActivities FROM ps_snapshot")


# GET ALL COLUMNS

# Get all table names
tables <- dbListTables(phonestudy)
# Initialize a list to store column names per table
column_names <- list()
# Loop through each table and get column names
for (table in tables) {
  column_names[[table]] <- dbListFields(phonestudy, table)
}
# Print all columns by table
for (table in names(column_names)) {
  cat(paste0("Table: ", table, "\n"))
  print(column_names[[table]])
  cat("\n")
}
