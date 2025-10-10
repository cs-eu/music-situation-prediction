##### COMBINE THE DIFFERENT LYRICS-BASED FEATURES INTO ONE DF #####

# load packages
library(dplyr)
library(forcats)

# read in the different feature sets, which were produced with the respective Python scripts 
bert = read.csv("data/external data/Lyrics/single feature groups/features_genius.csv") # 768 features
nrc = read.csv("data/external data/Lyrics/single feature groups/LIWC_genius.csv") # 10 features
nrc = nrc[,c("lyricsID", "lyric_len", "positive", "negative", "fear", "anger", "sadness", "disgust" ,"joy", "trust", "surprise", "anticipation")]
topics = read.csv("data/external data/Lyrics/single feature groups/Topics_features_genius.csv") # 30 features

# combine files by the lyricsID
lyrics_data = left_join(nrc, topics, by = "lyricsID")
lyrics_data = left_join(lyrics_data, bert, by = "lyricsID")
rm(bert, nrc, topics)

# rename columns
lyrics_data <- lyrics_data %>% 
  rename(
    nrc_fear = fear,
    nrc_anger = anger,
    nrc_trust = trust,
    nrc_surprise = surprise,
    nrc_pos = positive,
    nerc_neg = negative,
    nrc_sadness = sadness,
    nrc_disgust = disgust,
    nrc_joy = joy,
    nrc_anticipation = anticipation,
    lyrics_length = lyric_len
  )

# remove irrelevant columns
lyrics_data$artist_name = NULL
lyrics_data$artist_uri = NULL

# insert NAs
lyrics_data <- lyrics_data %>% dplyr::na_if("")

# save datafram
#saveRDS(lyrics_data, file = "data/external data/Lyrics/lyrics_features.rds")