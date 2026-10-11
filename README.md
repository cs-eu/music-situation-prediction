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
Contains the Python training application and regression models:

- `main.py`: Hydra entry point for training and evaluation
- `conf/config.yaml` and `conf/model/`: Authoritative experiment and model configuration
- `src/data/`: Raw-data loading, participant splitting, and fold-fitted preprocessing
- `src/models/`: Mean baseline, Elastic-Net, Random Forest, boosting, and neural models
- `src/training/`: Grouped tuning, validation-based neural stopping, evaluation, and complete predictor artifacts
- `tests/`: Leakage, splitting, model, metric, and persistence regression tests
- `outputs/models/`: Complete predictors, metrics, and experiment manifests

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

Supervised regression predicts aggregated music attributes from the situational sensing features of a listening window. This is not track identification or a causal forecast at the start of a window: several contextual measurements are aggregated over that same window. Participant identities, other music attributes, and music-derived key encodings are never predictors.

#### Elastic-Net Regression
- Combines L1 (lasso) and L2 (ridge) penalties
- Hyperparameters: regularization strength (α) and L1 ratio
- Regularization can be tuned by grid search with 5-fold participant-blocked cross-validation on development participants only
- Provides interpretable sparse coefficient estimates

#### Random Forest Regression
- Ensemble of decision trees with bootstrap aggregation
- Square-root feature subsampling
- Regularization via minimum leaf size
- Evaluated on held-out participants; optional parameter grids use participant-blocked cross-validation

#### Neural Networks
- Regularized tabular multi-layer perceptron
- ReLU activations and dropout regularization
- Optimized with AdamW and an RMSE-aligned mean squared error loss
- Early stopping and learning rate scheduling

### 5. Model Evaluation

All models use participant-disjoint splits, with 20% of participants reserved for final testing and 10% for neural/boosting validation by default. These are participant fractions, not window fractions; listening activity is highly imbalanced across participants. Non-early-stopped classical models combine the remaining development partitions, and parameter grids are selected using grouped folds without looking at test outcomes. LightGBM and neural models retain separate validation participants for early stopping. Fixed classical models also report a pre-refit validation score. Test scoring is disabled by default and requires `evaluate_test=true` after development-based model selection. Selected CV and best-validation scores are selection scores, not unbiased final performance estimates or confidence intervals.

- **Fold-safe preprocessing**: Feature imputation, category encoding, and feature/target scaling are learned from training observations only
- **Observed outcomes only**: Missing target values are excluded per target, never imputed; targets with no training labels are explicitly recorded as skipped
- **Original-unit metrics**: RMSE, MAE, R², participant-macro RMSE, and MSE skill relative to the training-mean baseline
- **Neural validation**: Participant-disjoint early stopping and restoration of the best validation checkpoint
- **Reproducibility**: Seeds, selected parameters, split counts, feature schema, library versions, source/split checksums, run identities, and resolved configuration are saved

The supplied raw prediction artifact has 58,489 windows from 394 participants. Of its outcome columns, 117 continuous targets have observations and 33 LIWC targets are entirely missing. The legacy `preprocessed_features.csv` is unsuitable for unbiased evaluation: it was globally scaled and some target values were median-imputed before splitting. Training rejects that artifact; use the raw RDS or an untouched CSV instead. No model can guarantee perfect predictions of unseen participants' music choices.

The supplied artifact also contains 149 windows that are identical across different participant IDs and a `liwc_anger` column that exactly duplicates `genius_anger`. Training logs and quarantines all ambiguous copies and that target, leaving 58,340 windows and 116 observed continuous targets; it does not alter the raw file. The extraction code is corrected, but existing corrupted aggregates cannot be repaired without regenerating them from the private source records. Performance measured on this artifact is therefore **provisional**, and undetected upstream contamination may remain. Calendar features follow the recorded timestamps; verify UTC/local clock provenance before interpreting them as local circadian effects. Current outcomes are unweighted means over matched unique tracks, not listening-time-weighted preferences.

Corrected extraction writes `data/results/participant_safe_v2/features_all_windows_participant_safe_v2.rds` relative to its R working directory. Versioned checkpoints preserve skipped-window positions and validated progress; legacy intermediate files are not reused. Point training at the regenerated output with a `data_path` override instead of continuing to use the old combined artifact.

The optional Transformer uses contextual-feature tokens rather than treating separate windows in a batch as a sequence. GNN training is disabled for this tabular dataset because no defensible graph or leakage-safe edges are supplied. TabPFN is optional and requires suitable hardware, explicit model-weight access, and a documented training-size policy; it is not a default full-data CPU model.

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

For the prediction stage, install the authoritative stage-specific dependencies:

```bash
python -m pip install -r 02_Music_Prediction/requirements.txt
```

Or create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r 02_Music_Prediction/requirements.txt
```

The root `requirements.txt` describes the older enrichment/NLP environment. Use a separate environment for that stage rather than combining its legacy pins with the prediction dependencies.

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
    - Run `02_Music_Prediction/main.py` using raw features; preprocessing is part of each training fold
    - Select the model and targets through Hydra overrides
    - Inspect saved metrics and manifests before interpreting fitted predictors

### Prediction Commands

Run these commands from the repository root:

```bash
# Honest reference predictor for a small target set.
python 02_Music_Prediction/main.py model=baseline 'target_columns=[music_track_energy,music_track_valence]'

# Group-tuned linear model; omit target_columns to train all available outcomes.
python 02_Music_Prediction/main.py model=elastic_net 'target_columns=[music_track_energy,music_track_valence]'

# Nonlinear tabular comparison.
python 02_Music_Prediction/main.py model=random_forest 'target_columns=[music_track_energy,music_track_valence]'

# Only after development-based model selection: opt in to final test scoring.
python 02_Music_Prediction/main.py model=elastic_net 'target_columns=[music_track_energy,music_track_valence]' evaluate_test=true

# Regression tests.
python -m pytest 02_Music_Prediction/tests
```

Neural models require the optional `neural` installation extra, for example `python -m pip install -e './02_Music_Prediction[neural]'`. Optional external experiment logging is disabled by default; opt in with `wandb.mode=offline` or `wandb.mode=online`. Relative data/output paths resolve from `02_Music_Prediction`, not the shell working directory.

Saved `.joblib` predictors include the fitted preprocessing, feature ordering, target scaler, model, data-quality flags, and run/source identity. The manifest lists only artifacts from its own run; older target files may remain in the output directory, so compare the predictor's `run_metadata["run_id"]` with the manifest before combining results. In a Python environment with `02_Music_Prediction` installed, load a **trusted** artifact with `joblib.load(path)` and call `predictor.predict(raw_context_frame)` to obtain predictions in the original target units. Never unpickle artifacts from untrusted sources.

### Running Individual Components

Each stage can be run independently if the required input data is available. The preprocessing notebooks and scripts are designed to be modular, but they expect specific input formats:

- Music enrichment expects: Track metadata (artist, title)
- Feature extraction expects: Raw SSPS database tables
- Model training expects: Untransformed contextual features and observed music attributes from Stage 1, such as `features_all_windows_combined.rds`

## Data Privacy

**Important**: The raw smartphone sensing data from the SSPS study cannot be shared due to privacy concerns. The database connection scripts (`01_Feature_Extraction/00_SOURCE_Databases.R`) are provided for transparency but will not run without access to the SSPS database. Preprocessed intermediate data files may be available upon request, subject to data sharing agreements.

## Results

Model results are saved in `02_Music_Prediction/outputs/models/`:
- `<model>_<target>.joblib`: Complete original-unit predictor
- `<model>_metrics.json`: Development scores, any grouped tuning results, and opt-in test/baseline metrics
- `<model>_manifest.json`: Configuration, versions, feature schema, split counts, and data-quality exclusions

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
