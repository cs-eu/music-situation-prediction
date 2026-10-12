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
- Configurable feature subsampling (70% of features by default)
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

The Transformer uses contextual-feature tokens rather than treating separate windows in a batch as a sequence. The experimental GNN builds a separate feature graph for each window: feature nodes connect to sensor-modality hubs, which connect to a context hub. Feature identities come from the training-fitted preprocessing schema; graphs never connect separate windows or participants and never use outcome labels to construct edges. This is a domain-defined modeling prior, not an inferred causal network. TabPFN requires compatible licensed regression weights and an explicit resource policy; it is not a default full-data CPU model.

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

From the repository root, install the prediction package and all model backends:

```bash
# First install a CUDA-enabled PyTorch build appropriate for the target GPU/driver.
# Use https://pytorch.org/get-started/locally/; this project requires Torch >= 2.6.
python -m pip install -e './02_Music_Prediction[all,test]'
```

Training examples below target a **GPU-enabled CUDA machine**, not the local CPU validation environment. Verify that PyTorch can actually access the deployment GPU:

```bash
python -c 'import torch; assert torch.cuda.is_available(), "CUDA-enabled PyTorch/GPU unavailable"; print(torch.cuda.get_device_name(0)); print("Native bf16:", torch.cuda.is_bf16_supported(including_emulation=False))'
export CUBLAS_WORKSPACE_CONFIG=:4096:8
```

Set the cuBLAS workspace before any CUDA work for strict deterministic neural execution, especially when several backends share one sweep process. The CLI supplies this default when it is unset. Use `precision=bf16-mixed` on hardware with native bf16 support, `precision=16-mixed` for suitable older CUDA GPUs, or the portable default `precision=32-true`. True-half training (`16-true`/`bf16-true`) is unsupported because it compromises loss/metric accumulator stability. `64-true` is available for numerical checks, not recommended for GPU throughput.

Or create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
python -m pip install -e './02_Music_Prediction[all,test]'
```

The root `requirements.txt` describes the older enrichment/NLP environment. Use a separate environment for that stage rather than combining its legacy pins with the prediction dependencies.

For a smaller installation, `python -m pip install -e ./02_Music_Prediction` installs the baseline, Elastic-Net, and Random Forest dependencies. Add extras as needed:

| Model | Hydra name | Optional installation extra | Execution |
| --- | --- | --- | --- |
| Mean baseline | `baseline` | None | CPU |
| Elastic-Net | `elastic_net` | None | CPU |
| Random Forest | `random_forest` | None | CPU |
| LightGBM | `lightgbm` | `lightgbm` | CPU by default; GPU training requires a supported build |
| Multilayer perceptron | `neural_net` | `neural` | CUDA training and immediate evaluation |
| Feature-token Transformer | `transformer` | `neural` | CUDA training and immediate evaluation |
| Per-window feature-graph GNN | `gnn` | `graph` | CUDA training and immediate evaluation |
| TabPFN regressor | `tabpfn` | `tabpfn` | CUDA selected automatically when available |

For example, `python -m pip install -e './02_Music_Prediction[graph]'` installs GNN dependencies. The `all` extra installs every model backend but does not download TabPFN weights. TabPFN is constrained to the tested 9.x package API. Neural models select available hardware automatically; `accelerator=gpu devices=1` explicitly requires one deployment GPU. Scikit-learn's baseline, Elastic-Net, and Random Forest remain CPU algorithms even on a GPU host. Running all backends on a GPU machine does not imply that all eight algorithms use CUDA.

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

All commands below run from the repository root. They use the bundled raw RDS by default; add `data_path=/absolute/path/to/regenerated_features.rds` to use corrected data. No separate global preprocessing step is needed. Relative data, model-output, and `tabpfn_checkpoint` paths resolve from `02_Music_Prediction`.

Start with a small target set. Every target gets its own fitted model; omit `target_columns` to train all available valid targets, which can be substantially slower. Known unavailable/corrupted targets are skipped and recorded.

#### Individual Models

Each command selects one backend. Separate output directories avoid mixing experiments:

```bash
python 02_Music_Prediction/main.py model=baseline \
  'target_columns=[music_track_energy,music_track_valence]' output_dir=outputs/individual/baseline

python 02_Music_Prediction/main.py model=elastic_net \
  'target_columns=[music_track_energy,music_track_valence]' output_dir=outputs/individual/elastic_net

python 02_Music_Prediction/main.py model=random_forest \
  'target_columns=[music_track_energy,music_track_valence]' output_dir=outputs/individual/random_forest

python 02_Music_Prediction/main.py model=lightgbm \
  'target_columns=[music_track_energy,music_track_valence]' output_dir=outputs/individual/lightgbm

python 02_Music_Prediction/main.py model=neural_net \
  'target_columns=[music_track_energy,music_track_valence]' \
  accelerator=gpu devices=1 progress_bar=true output_dir=outputs/individual/neural_net

python 02_Music_Prediction/main.py model=transformer \
  'target_columns=[music_track_energy,music_track_valence]' \
  accelerator=gpu devices=1 progress_bar=true output_dir=outputs/individual/transformer

python 02_Music_Prediction/main.py model=gnn \
  'target_columns=[music_track_energy,music_track_valence]' \
  accelerator=gpu devices=1 progress_bar=true output_dir=outputs/individual/gnn
```

The GNN command uses the built-in per-window feature graph (`model.graph_input=false`), not graphs between people or listening windows. `graph_input=true` is reserved for direct Python consumers supplying explicit PyG `Data/Batch` objects; it is not a mode for the raw-table CLI.

Append `precision=bf16-mixed` to the neural commands on native-bf16 hardware. `batch_size` controls neural training/inference minibatches; reduce it if VRAM is constrained. GNN message aggregation can remain float32 under AMP, so do not assume mixed precision halves all activation memory.

LightGBM's default command above is intentionally CPU-compatible. To use its CUDA tree learner, install a CUDA-enabled LightGBM build on the deployment machine, with a compatible CUDA toolkit/NVCC and CMake:

```bash
python -m pip install --no-cache-dir --force-reinstall --no-deps --no-binary=lightgbm \
  --config-settings=cmake.define.USE_CUDA=ON lightgbm==4.7.0

python 02_Music_Prediction/main.py model=lightgbm \
  'target_columns=[music_track_energy,music_track_valence]' \
  model.device_type=cuda model.gpu_device_id=0 output_dir=outputs/individual/lightgbm_cuda
```

`model.device_type=gpu` selects LightGBM's separate OpenCL implementation and requires its corresponding build/driver support. Ordinary package installation is not proof of CUDA tree-learner support. GPU/backend errors are reported rather than silently falling back to CPU. LightGBM's GPU option accelerates training; it does not make scikit-learn models or ordinary tree inference CUDA algorithms.

#### TabPFN Weights and Training

TabPFN needs a **trusted, compatible, licensed local regression checkpoint**. Automatic weight downloads are disabled. The verified public v2 reference is [Prior-Labs/TabPFN-v2-reg](https://huggingface.co/Prior-Labs/TabPFN-v2-reg), revision `4972a65a1b30806315c6f92499959ffbfc69a673`. Read its model card and license before using or redistributing it; newer default weights can require separate authentication/license acceptance.

If using that reference checkpoint, this explicit unauthenticated download places it in the ignored data directory:

```bash
python -c 'from huggingface_hub import hf_hub_download; print(hf_hub_download(repo_id="Prior-Labs/TabPFN-v2-reg", filename="tabpfn-v2-regressor.ckpt", revision="4972a65a1b30806315c6f92499959ffbfc69a673", local_dir="02_Music_Prediction/data/weights/tabpfn-v2", token=False))'
```

The checkpoint SHA256 is `2ab5a07d5c41dfe6db9aa7ae106fc6de898326c2765be66505a07e2868c10736`. Alternatively, use an existing compatible licensed checkpoint and supply its absolute path.

Run a v2 GPU experiment with a shared cohort inside that checkpoint's supported workload:

```bash
python 02_Music_Prediction/main.py model=tabpfn \
  'target_columns=[music_track_energy,music_track_valence]' \
  dataset.max_rows=10000 \
  tabpfn_checkpoint=data/weights/tabpfn-v2/tabpfn-v2-regressor.ckpt \
  model.device=cuda model.inference_device=cuda output_dir=outputs/individual/tabpfn
```

`dataset.max_rows` explicitly samples the **whole cohort before participant splitting**, after data-quality quarantine. It is seed-controlled and recorded in the manifest. It is not silent TabPFN-only subsampling. The default application safeguards allow at most 1,000 CPU training rows, 10,000 general training rows, and 500 transformed features; these are conservative wrapper limits, not universal limits of every TabPFN checkpoint. Raising them requires a checkpoint/version and hardware that actually support the resulting workload. The full bundled dataset exceeds the default safeguards.

TabPFN defaults to `model.device=auto model.inference_device=auto`, selecting CUDA when available. Both can be set to `cuda` to require a GPU or `cpu` for a bounded correctness check; the neural `accelerator` does not control TabPFN. Keep the exact external foundation weights used during fitting: they remain necessary after reloading the predictor. TabPFN fitted artifacts contain training context, not a duplicated foundation checkpoint, and must be treated as sensitive data. The native context also requires the recorded TabPFN/sklearn/NumPy/Torch versions; version drift raises an error rather than silently changing predictions.

For full-cohort TabPFN, use a licensed local checkpoint whose documented row/column limits cover the workload, such as an appropriate later-generation regression checkpoint, and explicitly raise `model.max_train_rows` (for example, `60000` for this input). The public v2 example is limited to 10,000 training rows and 500 features; changing CUDA availability does not change its architecture limits. Larger-checkpoint licensing and VRAM availability remain deployment prerequisites. Do not blindly raise `model.max_cpu_rows` to run a GPU-sized workload on CPU.

The wrapper collects queries before calling TabPFN, so ordinary `batch_size` is **not** a TabPFN VRAM limit. Use an appropriate cohort, checkpoint, and supported native memory controls such as `+model.model_kwargs.memory_saving_mode=auto`; caching trades VRAM for inference speed. Validate the actual GPU workload before starting all targets. The root `precision` option controls Lightning models, not TabPFN's own inference precision.

TabPFN 9.x reads its own settings from the caller's `.env` when present. For isolated validation without project environment files, invoke `main.py` by its absolute path from a clean working directory; data and output paths still resolve as described above. The wrapper disables telemetry and browser authentication, and local-weight runs do not need authentication.

#### Run All Models Together

Hydra's built-in `--multirun` launches **independent model jobs sequentially** by default. This is a comparison sweep, not an ensemble. For all eight backends on the GPU machine with the same v2-compatible cohort, install `[all]`, verify CUDA, and obtain the checkpoint above first:

```bash
python 02_Music_Prediction/main.py --multirun \
  model=baseline,elastic_net,random_forest,lightgbm,neural_net,transformer,gnn,tabpfn \
  'target_columns=[music_track_energy,music_track_valence]' \
  accelerator=gpu devices=1 \
  dataset.max_rows=10000 \
  tabpfn_checkpoint=data/weights/tabpfn-v2/tabpfn-v2-regressor.ckpt \
  'hydra.sweep.dir=02_Music_Prediction/outputs/comparisons/${now:%Y-%m-%d_%H-%M-%S}' \
  'hydra.sweep.subdir=${hydra.job.num}-${model.name}' \
  'output_dir=${hydra:runtime.output_dir}'
```

Only TabPFN consumes `tabpfn_checkpoint`. Every backend uses the same sampled cohort, seed, and outer participant split. Early-stopped models retain validation participants; other classical models refit on the combined development partition. Limited-cohort results are not estimates of full-data performance. Each job has its own output directory and manifest, so model files and configuration do not overwrite other jobs.

For the seven non-foundation backends on the full available cohort, no weight file or row cap is needed:

```bash
python 02_Music_Prediction/main.py --multirun \
  model=baseline,elastic_net,random_forest,lightgbm,neural_net,transformer,gnn \
  'target_columns=[music_track_energy,music_track_valence]' \
  accelerator=gpu devices=1 \
  'hydra.sweep.dir=02_Music_Prediction/outputs/comparisons/${now:%Y-%m-%d_%H-%M-%S}' \
  'hydra.sweep.subdir=${hydra.job.num}-${model.name}' \
  'output_dir=${hydra:runtime.output_dir}'
```

Add `precision=bf16-mixed` to either sweep on native-bf16 hardware. To include CUDA LightGBM training as well, install its CUDA build and add `++model.device_type=cuda`; only LightGBM consumes that setting. The baseline/Elastic-Net/Random Forest still run on CPU.

For a quick execution smoke test on the GPU machine, use one target and add `dataset.max_rows=200 tuning.enabled=false ++model.max_epochs=2`. The epoch override only affects neural backends. This checks execution, not convergence or predictive quality. CPU correctness checks use `accelerator=cpu ++model.device=cpu ++model.inference_device=cpu`; they are not deployment performance benchmarks. For full-target experiments, omit `target_columns`; large multi-model/target runs can be expensive, especially Transformer/GNN and TabPFN inference.

#### Configuration and Final Evaluation

General defaults are in `02_Music_Prediction/conf/config.yaml`, and backend defaults are in `02_Music_Prediction/conf/model/<name>.yaml`. Example overrides are `batch_size=128`, `cpu_threads=4`, `accelerator=gpu devices=1`, and, for neural models, `model.max_epochs=100 model.early_stopping_patience=12`.

Test scoring is intentionally disabled by default. Compare `cv_RMSE` for tuned models and `validation_RMSE` for validation-based models. Only after selecting a model/configuration using development data, append `evaluate_test=true` to that same individual command. Do not repeatedly tune against final test scores.

Optional external experiment logging is disabled by default; install `[tracking]` and opt in with `wandb.mode=offline` or `wandb.mode=online`. Verify the model APIs, graph isolation, preprocessing, checkpoints, and CLI orchestration with:

```bash
python -m pytest 02_Music_Prediction/tests -q
```

Real TabPFN tests never download weights. To include its complete all-eight-backend CLI sweep, supply the trusted local checkpoint explicitly:

```bash
TABPFN_TEST_CHECKPOINT="$PWD/02_Music_Prediction/data/weights/tabpfn-v2/tabpfn-v2-regressor.ckpt" \
  python -m pytest 02_Music_Prediction/tests -q
```

On the deployment GPU, CUDA-specific training, mixed-precision inference, and portable artifact checks run automatically rather than being skipped. After installing CUDA LightGBM, opt into its actual GPU test as well:

```bash
export CUBLAS_WORKSPACE_CONFIG=:4096:8
LIGHTGBM_TEST_DEVICE=cuda \
TABPFN_TEST_CHECKPOINT="$PWD/02_Music_Prediction/data/weights/tabpfn-v2/tabpfn-v2-regressor.ckpt" \
  python -m pytest 02_Music_Prediction/tests/test_gpu.py 02_Music_Prediction/tests/test_lightgbm_gpu.py -q
```

Local CPU tests validate interfaces and bounded execution only; they do not establish CUDA throughput, VRAM capacity, or successful GPU execution on the deployment hardware.

Saved `.joblib` predictors include the fitted preprocessing, feature ordering, target scaler, model, data-quality flags, and run/source identity. The manifest lists only artifacts from its own run; older target files may remain in the output directory, so compare the predictor's `run_metadata["run_id"]` with the manifest before combining results. Neural models stay on the training device for immediate validation/test inference, then are saved on CPU for portability. In a Python environment with `02_Music_Prediction` installed, load a **trusted** artifact with `joblib.load(path)` and call `predictor.predict(raw_context_frame)` for original-unit predictions. For a reloaded neural predictor on the GPU machine, use `predictor.predict(raw_context_frame, device="cuda", precision="bf16-mixed")` on supported hardware, or `precision="32-true"`. TabPFN accepts `device="cuda"` too, but controls its precision through its own backend options. Never unpickle artifacts from untrusted sources.

These are inference artifacts, not resumable Lightning training checkpoints: optimizer, scheduler, and AMP-scaler state are not retained in the final joblib files. Runs currently use one GPU per model job (`devices=1`); the comparison sweep is not a distributed multi-GPU training launcher.

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
