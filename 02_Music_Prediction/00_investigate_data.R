# ------------------------------------------------------------
# Basic setup
# ------------------------------------------------------------
library(data.table)
library(ggplot2)
library(dplyr)
library(reshape2)

setwd("/Users/hilfskraft/Downloads/rforest/")

unprocessed <- readRDS("features_partial_500.rds")

valid_features <- Filter(Negate(is.null), unprocessed)
dt <- rbindlist(valid_features, use.names = TRUE, fill = TRUE)
cat("Rows:", nrow(dt), "\n")
cat("Expected (list length):", length(valid_features), "\n")
stopifnot(nrow(dt) == length(valid_features))

str(dt)

# ------------------------------------------------------------
# 1. General structure
# ------------------------------------------------------------
cat("Number of rows:", nrow(dt), "\n")
cat("Number of columns:", ncol(dt), "\n")

# Overview of column types
sapply(dt, class)

# ------------------------------------------------------------
# 2. Missing data overview
# ------------------------------------------------------------

# Fraction of missing values per column
na_fraction <- colSums(is.na(dt)) / nrow(dt)
na_summary <- sort(na_fraction, decreasing = TRUE)

# Show top 20 columns with most missingness
head(na_summary, 20)

# Visualize missingness distribution
ggplot(data.frame(Column = names(na_summary), Missing = na_summary),
       aes(x = reorder(Column, -Missing), y = Missing)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  coord_flip() +
  labs(title = "Fraction of Missing Values per Column", y = "Fraction Missing", x = "")

# ------------------------------------------------------------
# 3. User-level missingness
# ------------------------------------------------------------

# For each user, proportion of rows with any NA
user_missing <- dt %>%
  mutate(has_na = apply(is.na(.), 1, any)) %>%
  group_by(user_id) %>%
  summarise(frac_na_rows = mean(has_na))

summary(user_missing$frac_na_rows)
