# Music Situation Prediction

A research project for analyzing and predicting music listening situations based on contextual features and enriched music data. This project combines music audio features, lyrical analysis, and contextual data (GPS, activity, phone usage) to understand when and why people listen to music in different situations.

## Overview

This project investigates the relationship between music listening and contextual situations by:

1. **Enriching music tracks** with audio features (Spotify API), lyrics (Genius API), and language analysis (emotion detection, topic modeling, LIWC)
2. **Extracting contextual features** for time windows when music is playing, including:
   - GPS data (location, land use, home/work detection)
   - Physical activity (walking, running, in vehicle, etc.)
   - Phone activity (screen state, app usage, notifications)
   - Headset state (plugged/unplugged)
3. **Analyzing and predicting** music listening situations based on extracted features

## Project Structure

```
music-situation-prediction/
├── 00_Music_Enrichment/          # Music data enrichment module
│   ├── 00_extract_song_features.R
│   ├── 01_lyrics_retrieval.ipynb
│   ├── 02_language_processing.ipynb
│   ├── 03_topic_features.ipynb
│   └── utils/                     # Utility functions
│       ├── data_io.R
│       ├── processing.R
│       ├── setup.R
│       ├── spotify_helpers.R
│       └── statistics.R
│
├── 01_Feature_Extraction/        # Feature extraction module
│   ├── config.R                  # Configuration settings
│   ├── constants.R               # Constant values
│   ├── data_loader.R             # Data loading functions
│   ├── feature_extractors.R      # Modular feature extractors
│   ├── feature_extraction_main.R # Main orchestration
│   ├── run_feature_extraction.R  # Execution script
│   └── README.md                 # Module-specific documentation
│
├── 02_Music_Prediction/          # Prediction and analysis module
│   └── 00_investigate_data.R
│
└── LICENSE                        # MIT License
```

## Modules

### 00_Music_Enrichment

Enriches music tracks with:
- **Spotify audio features**: danceability, energy, tempo, valence, acousticness, etc.
- **Lyrics data**: Retrieved from Genius API
- **Language processing**: Emotion analysis, topic modeling (30 topics), LIWC features
- **Output**: Enriched track dataset with all features combined

### 01_Feature_Extraction

Extracts contextual features for music listening time windows:
- **GPS features**: Location, land use type, home/work detection
- **Activity features**: Detected physical activities (walking, running, in vehicle, etc.)
- **Phone activity**: Screen state, app usage, notifications, phone calls
- **Headset state**: Plugged/unplugged detection
- **Music features**: Combined Spotify, Genius, topic, and LIWC features
- **Output**: Feature vectors for each music listening window

### 02_Music_Prediction

Analyzes extracted features and builds predictive models for music listening situations.

## Prerequisites

### R Packages

The project requires the following R packages:

**00_Music_Enrichment:**
- `dplyr`
- `jsonlite`
- `httr` (for API calls)
- Other packages as specified in the module

**01_Feature_Extraction:**
- `DBI`
- `RMariaDB`
- `data.table`
- `lubridate`
- `jsonlite`
- `RcppSimdJson`
- `future`
- `future.apply`

**02_Music_Prediction:**
- `data.table`
- `ggplot2`
- `dplyr`
- `reshape2`

### Python (for Jupyter notebooks)

Required for notebooks in `00_Music_Enrichment/`:
- Python 3.x
- Jupyter Notebook
- Packages as specified in the notebooks

### API Access

- **Spotify API**: Required for music audio features
- **Genius API**: Required for lyrics retrieval

### Database

- MySQL/MariaDB database connection (configured in `01_Feature_Extraction/config.R`)
- Database credentials file at the path specified in config

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd music-situation-prediction
```

2. Install R dependencies:
```R
# Install required packages
install.packages(c("DBI", "RMariaDB", "data.table", "lubridate", 
                   "jsonlite", "RcppSimdJson", "future", "future.apply",
                   "dplyr", "ggplot2", "reshape2"))
```

3. Configure database credentials:
   - Create database credentials file at the path specified in `01_Feature_Extraction/config.R`
   - Default path: `/local/.meta/dbcredentials.R`

4. Set up API credentials:
   - Configure Spotify API credentials for `00_Music_Enrichment/`
   - Configure Genius API credentials for lyrics retrieval

5. Prepare data directories:
   - Ensure required data directories exist (as specified in config files)
   - Place helper data files in the appropriate directories

## Usage

### Music Enrichment

1. Navigate to the music enrichment module:
```bash
cd 00_Music_Enrichment
```

2. Run the enrichment script:
```R
source("00_extract_song_features.R")
```

3. Process lyrics and language features using the Jupyter notebooks:
   - `01_lyrics_retrieval.ipynb`: Retrieve lyrics from Genius
   - `02_language_processing.ipynb`: Process lyrics for emotion analysis
   - `03_topic_features.ipynb`: Extract topic modeling features

### Feature Extraction

1. Navigate to the feature extraction module:
```bash
cd 01_Feature_Extraction
```

2. Configure paths and settings in `config.R`

3. Run feature extraction:
```R
source("run_feature_extraction.R")
```

The script will:
- Load data from database and helper files
- Extract features for each music listening window
- Save individual window files and combined output
- Support resume functionality (skips already processed windows)
- Use parallel processing by default

### Music Prediction

1. Navigate to the prediction module:
```bash
cd 02_Music_Prediction
```

2. Run analysis scripts as needed

## Configuration

### Feature Extraction Configuration

Edit `01_Feature_Extraction/config.R` to configure:
- Directory paths
- Database connection settings
- Data file paths
- Output locations
- Processing parameters (parallel workers, progress intervals)

### Data Paths

Ensure the following data files exist:
- Activity data: `data/helper/combined_activity_data.rds`
- Music windows: `data/results/music_windows_all.csv`
- GPS land use: `data/results/gps_landuse_by_window.rds`
- Spotify data: `data/helper/distinct_tracks_enriched.rds`
- Lyrics features: `data/helper/lyrics-features/*.csv`

## Output

### Music Enrichment
- Enriched track dataset: `data/results/distinct_tracks_enriched.rds`
- Lyrics features: `data/helper/lyrics-features/*.csv`

### Feature Extraction
- Individual window files: `data/results/new/window_XXXXX.rds`
- Combined output: `data/results/new/features_all_windows_combined.rds`

## Features Extracted

### Music Features
- **Spotify**: danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo
- **Genius**: Emotion scores (fear, anger, trust, surprise, positive, negative, sadness, disgust, joy, anticipation), lyric length
- **Topics**: 30 topic modeling features
- **LIWC**: 90+ linguistic features (word count, emotional tone, social processes, cognitive processes, etc.)

### Contextual Features
- **GPS**: Location coordinates, land use type, home/work indicators
- **Activity**: Detected activities (still, walking, running, cycling, in vehicle, etc.)
- **Phone**: Screen state, app usage, notifications, phone calls, SMS
- **Headset**: Plugged/unplugged state
- **Time**: Time bin, day of week, hour of day

## Notes

- The feature extraction script supports resume functionality - it will skip already processed windows
- Parallel processing is used by default (configurable in `config.R`)
- Progress is reported periodically during execution
- The script handles missing data gracefully

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Citation

If you use this project in your research, please cite appropriately.

## Contact

For questions or issues, please open an issue on the repository.

