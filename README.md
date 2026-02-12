# Predicting Everyday Music Choice from Situational Contexts on Smartphones

This repository contains code for predicting music listening choices based on situational contexts extracted from smartphone sensing data. The project combines passive smartphone sensing, music audio features, and lyrical content analysis to model the relationship between environmental context and music preferences.

## Overview

This project uses machine learning to predict music choices from situational contexts captured through smartphone sensing. The analysis is based on data from the Smartphone Sensing Panel Study (SSPS), which collected longitudinal data from 850 participants in Germany between May and December 2020. The project integrates multiple data modalities:

- **Smartphone sensing data**: GPS location, screen state, app usage, physical activity
- **Music listening logs**: Track-level listening events aggregated into listening windows
- **Spotify audio features**: Low-level acoustic descriptors (valence, energy, danceability, tempo, etc.)
- **Lyrics analysis**: Linguistic features, LIWC categories, and topic modeling

## Project Structure

The repository is organized into three main stages:

### 00_Music_Enrichment/
Contains scripts for enriching music track data with Spotify audio features and lyrics analysis:

- `00_extract_song_features.R`: Main script for matching tracks with Spotify API and enriching with audio features
- `01_lyrics_retrieval.ipynb`: Retrieves song lyrics using the Genius API
- `02_language_processing.ipynb`: Language detection and translation of non-English lyrics
- `03_topic_features.ipynb`: Topic modeling and feature extraction from lyrics
- `utils/`: Helper functions for data I/O, Spotify API interaction, and text processing

### 01_Feature_Extraction/
Contains R scripts for extracting contextual and behavioral features from smartphone sensing data:

- `00_SOURCE_Databases.R`: Database connection and raw data loading (note: requires access to SSPS database)
- `01_SOURCE_WindowExtract.R`: Aggregates music listening events into temporally contiguous windows
- `02_precompute_GPS.R`: Precomputes GPS-based location features and land-use categories
- `03_SOURCE_FeatureExtract.R`: Main feature extraction script combining all data sources
- `utils/`: Helper functions for GPS processing, timestamp correction, and app session labeling

### 02_Music_Prediction/
Contains Python notebooks for machine learning model training and interpretation:

- `00_preprocess_data.ipynb`: Data preprocessing pipeline (standardization, encoding, imputation)
- `01_train_EN.ipynb`: Elastic-Net regression model training
- `01_train_RF.ipynb`: Random Forest regression model training
- `01_train_NN.ipynb`: Neural network model training
- `02_interpretation.ipynb`: Model interpretation and feature importance analysis
- `model/`: Saved model artifacts (organized by model type and target variable)
- `results/`: Model performance metrics and interpretation summaries

## Data Source

This project is based on data from the **Smartphone Sensing Panel Study (SSPS)** conducted by Ludwig-Maximilians-Universität München (Schoedel & Oldemeier, 2020). The study collected data from 850 participants using:

- Continuous passive smartphone sensing via the Android app PhoneStudy2
- Monthly online surveys
- Two intensive 14-day experience sampling phases

### Data Description

- **59,958 listening windows** aggregated from individual track-level listening events
- **10-minute cooldown period** used to define listening window boundaries
- Average window duration: ~24 minutes
- Average tracks per window: ~41
- **103,785 distinct tracks** in the raw listening logs
- **66,168 tracks** matched with preprocessed Spotify database
- **27,347 tracks** enriched via live Spotify API queries
- **50,185 tracks** with successfully retrieved lyrics

## Methodology

### 1. Listening Window Detection

Music listening events were aggregated into temporally contiguous windows. A listening window:
- Starts when a music track is logged
- Ends after 10 minutes of inactivity (no new tracks logged)
- This threshold balances sensitivity to interruptions with meaningful session capture

### 2. Contextual Feature Extraction

For each listening window, contextual features are extracted from smartphone sensing data:

- **Location features**: Home/work classification, land-use categories (via OpenStreetMap API)
- **Device state**: Headset connectivity, phone state variables
- **Physical activity**: Movement patterns inferred from Google Activity Recognition API
- **App usage**: Aggregated by category (communication, social media, time management, etc.)
- **Screen interaction**: Duration in each screen state (ON UNLOCKED, ON LOCKED, OFF, etc.)

### 3. Music Feature Enrichment

Music-related features are aggregated at the listening-window level:

#### Spotify Audio Features
- Low-level acoustic descriptors: valence, energy, danceability, tempo
- Structural properties: key, mode, time signature, loudness
- Enriched via Spotify Web API

#### Lyrics Features
- **Linguistic descriptors**: Basic structural properties (word count, sentence length, etc.)
- **LIWC categories**: Psycholinguistic features from Linguistic Inquiry and Word Count framework
- **Topic modeling**: 30 topic categories derived via probabilistic topic modeling
- Lyrics translated to English using Google Translate API when necessary (14,694 translations from 50+ languages)

### 4. Machine Learning Models

Three supervised learning approaches are implemented to predict music features from situational contexts:

#### Elastic-Net Regression
- Combines L1 (lasso) and L2 (ridge) penalties
- Hyperparameters: regularization strength (α) and L1 ratio
- Optimized via grid search with 5-fold user-blocked cross-validation
- Provides interpretable sparse coefficient estimates

#### Random Forest Regression
- Ensemble of decision trees with bootstrap aggregation
- Square-root feature subsampling
- Regularization via minimum leaf size
- Evaluated with 5-fold user-blocked cross-validation

#### Neural Networks
- Multi-layer perceptron with three hidden layers of decreasing width
- ReLU activations and dropout regularization
- Optimized with AdamW optimizer and smooth L1 loss
- Early stopping and learning rate scheduling

### 5. Model Evaluation

All models are evaluated using:
- **User-blocked cross-validation**: Prevents information leakage across participants
- **Performance metrics**: Root Mean Squared Error (RMSE) and R²
- **Feature importance**: 
  - Elastic-Net: Non-zero coefficients ranked by magnitude
  - Random Forest & Neural Networks: Permutation feature importance
- **Effect directions**: Estimated via partial dependence curves

## Setup and Installation

### Prerequisites

This project requires both R and Python environments.

### R Dependencies

Install required R packages:

```r
install.packages(c(
  "dplyr", "stringr", "readr", "jsonlite", "spotifyr",
  "DBI", "RMariaDB", "data.table", "lubridate",
  "future.apply"
))
```

### Python Dependencies

Install required Python packages:

```bash
pip install numpy pandas scikit-learn torch pyreadr matplotlib joblib
```

Or create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### API Credentials

To run the music enrichment scripts, you'll need API credentials:

1. **Spotify API**: Set environment variables or update `00_Music_Enrichment/utils/setup.R`:
   ```r
   Sys.setenv(SPOTIFY_CLIENT_ID = 'your_client_id')
   Sys.setenv(SPOTIFY_CLIENT_SECRET = 'your_client_secret')
   ```

2. **Genius API**: Configure in `00_Music_Enrichment/01_lyrics_retrieval.ipynb`

3. **Google Translate API**: Configure in `00_Music_Enrichment/02_language_processing.ipynb`

4. **Database Access**: The feature extraction scripts require access to the SSPS database (credentials stored in `/local/.meta/dbcredentials.R`)

### Working Directories

Update the working directory paths in the scripts:
- R scripts: Set `setwd("")` to your project root
- Python notebooks: Adjust paths relative to the notebook location

## Usage

### Workflow Overview

1. **Music Enrichment** (Stage 0):
   - Run `00_extract_song_features.R` to match and enrich tracks with Spotify features
   - Execute notebooks in sequence: `01_lyrics_retrieval.ipynb` → `02_language_processing.ipynb` → `03_topic_features.ipynb`

2. **Feature Extraction** (Stage 1):
   - Note: Requires access to SSPS database
   - Run scripts in order: `00_SOURCE_Databases.R` → `01_SOURCE_WindowExtract.R` → `02_precompute_GPS.R` → `03_SOURCE_FeatureExtract.R`

3. **Model Training** (Stage 2):
   - Preprocess data: `00_preprocess_data.ipynb`
   - Train models: `01_train_EN.ipynb`, `01_train_RF.ipynb`, `01_train_NN.ipynb`
   - Interpret results: `02_interpretation.ipynb`

### Running Individual Components

Each stage can be run independently if the required input data is available. The preprocessing notebooks and scripts are designed to be modular, but they expect specific input formats:

- Music enrichment expects: Track metadata (artist, title)
- Feature extraction expects: Raw SSPS database tables
- Model training expects: Preprocessed feature matrix (output from Stage 1 or `features_all_windows_combined.rds`)

## Data Privacy

**Important**: The raw smartphone sensing data from the SSPS study cannot be shared due to privacy concerns. The database connection scripts (`01_Feature_Extraction/00_SOURCE_Databases.R`) are provided for transparency but will not run without access to the SSPS database. Preprocessed intermediate data files may be available upon request, subject to data sharing agreements.

## Results

Model results are saved in `02_Music_Prediction/results/`:
- Performance metrics per target variable and model type
- Feature importance rankings
- Model interpretation summaries
- Cross-validation results

The models predict various music features from situational contexts, including:
- Spotify audio features (valence, energy, danceability, etc.)
- LIWC categories from lyrics
- Topic model proportions
- Genius-derived features

## Acknowledgments

- Data collection and study design: Smartphone Sensing Panel Study team, LMU Munich
- Spotify API for audio feature enrichment
- Genius API for lyrics retrieval
- OpenStreetMap contributors for geographic data
