# Feature Extraction Module

### Configuration Files
- **`config.R`**: Contains all configuration settings including file paths, database settings, and processing parameters
- **`constants.R`**: Defines all constant values used throughout feature extraction (feature names, categories, etc.)

### Core Modules
- **`data_loader.R`**: Functions for loading and preprocessing data from database and files
- **`utils_feature_extraction.R`**: Utility functions (time binning, column management, validation)
- **`feature_extractors.R`**: Modular functions for extracting different types of features:
  - GPS features
  - Headset state features
  - Detected activity features
  - Phone activity features
  - App usage features
  - Screen state features
  - Music features (Spotify, Genius, Topic, LIWC)

### Main Scripts
- **`feature_extraction_main.R`**: Main orchestration function that coordinates all feature extraction
- **`run_feature_extraction.R`**: Standalone execution script (recommended for new workflows)
- **`03_SOURCE_FeatureExtract.R`**: Refactored original file (maintains backward compatibility)

## Dependencies

- `DBI`
- `RMariaDB`
- `data.table`
- `lubridate`
- `jsonlite`
- `RcppSimdJson`
- `future`
- `future.apply`

## Configuration

Before running, ensure:
1. Database credentials file exists at the path specified in `config.R`
2. All data files exist at paths specified in `config.R`
3. Screen preprocessing utility exists (or will be created as a stub)

## Output

The script produces:
- Individual window files: `data/results/new/window_XXXXX.rds`
- Combined output: `data/results/new/features_all_windows_combined.rds`

## Notes

- The script supports resume functionality - it will skip already processed windows
- Parallel processing is used by default (configurable in `config.R`)
- Progress is reported periodically during execution

