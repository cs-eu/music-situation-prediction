from __future__ import annotations

import hashlib
import importlib
import json
import logging
import random
import re
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any

import joblib
import numpy as np
import pandas as pd
import torch
from sklearn.model_selection import GroupKFold, ParameterGrid
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

from data.preprocess import build_preprocessor, load_data
from data.split import DataSplitter
from utils.metrics import calculate_metrics
from utils.training import (
    get_config_value,
    get_feature_columns,
    get_group_column,
    get_target_columns,
    log_model_metrics,
    resolve_path,
)

LOGGER = logging.getLogger(__name__)
NEURAL_MODELS = {"neural_net", "transformer", "gnn"}


def _loader(features, targets=None, batch_size=256, shuffle=False, seed=42, workers=0, pin_memory=False):
    features = torch.as_tensor(np.asarray(features), dtype=torch.float32)
    if targets is None:
        targets = np.zeros((len(features), 1), dtype=np.float32)
    targets = torch.as_tensor(np.asarray(targets), dtype=torch.float32).reshape(-1, 1)
    return DataLoader(
        TensorDataset(features, targets), batch_size=batch_size, shuffle=shuffle,
        num_workers=workers, generator=torch.Generator().manual_seed(seed),
        pin_memory=pin_memory,
    )


@dataclass
class TrainedPredictor:
    """Complete, original-unit predictor. Load joblib artifacts only from trusted sources."""

    model: Any
    model_name: str
    preprocessor: Any
    target_scaler: StandardScaler
    feature_columns: list[str]
    target_column: str
    model_config: dict[str, Any]
    data_quality: dict[str, Any] = field(default_factory=dict)
    run_metadata: dict[str, Any] = field(default_factory=dict)

    def predict(self, frame: pd.DataFrame, batch_size: int = 256,
                device: str | torch.device | None = None, precision: str | None = None) -> np.ndarray:
        missing = set(self.feature_columns) - set(frame.columns)
        if missing:
            raise ValueError(f"Prediction data is missing context columns: {sorted(missing)}")
        if not len(frame):
            return np.empty(0, dtype=float)
        features = self.preprocessor.transform(frame[self.feature_columns])
        if self.model_name in NEURAL_MODELS:
            parameter = next(self.model.parameters())
            if device == "auto":
                device = "cuda" if torch.cuda.is_available() else "cpu"
            selected_device = torch.device(device) if device is not None else parameter.device
            if selected_device.type == "cuda" and not torch.cuda.is_available():
                raise ValueError("CUDA inference requested, but CUDA-enabled PyTorch/a GPU is unavailable.")
            effective_precision = precision or self.model_config.get("precision", "32-true")
            if precision is None and selected_device.type == "cpu" and effective_precision in {"16-mixed", "bf16-mixed"}:
                effective_precision = "32-true"
            if effective_precision not in {"32-true", "64-true", "16-mixed", "bf16-mixed"}:
                raise ValueError("Predictor precision must be 32-true, 64-true, 16-mixed, or bf16-mixed.")
            input_dtype = torch.float64 if effective_precision == "64-true" else torch.float32
            if parameter.dtype == input_dtype:
                self.model.to(selected_device).eval()
            else:
                self.model.to(selected_device, dtype=input_dtype).eval()
            loader = _loader(features, batch_size=batch_size, pin_memory=selected_device.type == "cuda")
            dtype = torch.bfloat16 if effective_precision == "bf16-mixed" else torch.float16
            with torch.inference_mode(), torch.autocast(selected_device.type, dtype=dtype, cache_enabled=False,
                                                        enabled=effective_precision in {"16-mixed", "bf16-mixed"}):
                batches = []
                for x, _ in loader:
                    values = self.model(x.to(selected_device, dtype=input_dtype, non_blocking=loader.pin_memory))
                    if values.dtype in {torch.float16, torch.bfloat16}:
                        values = values.float()
                    batches.append(values.cpu().reshape(-1))
                predictions = torch.cat(batches)
        else:
            loader = _loader(features, batch_size=batch_size)
            if self.model_name == "tabpfn" and device is not None:
                predictions = self.model.predict(loader, inference_device=device)
            else:
                if device is not None and str(device) not in {"cpu", "auto"}:
                    raise ValueError(f"{self.model_name} device is controlled by its backend configuration, not predict(device=...).")
                predictions = self.model.predict(loader)
        predictions = torch.as_tensor(predictions).detach().cpu().numpy().reshape(-1, 1)
        return self.target_scaler.inverse_transform(predictions).reshape(-1)


class Trainer:
    def __init__(self, config):
        self.config = config
        self.model = None
        self.seed = int(get_config_value(config, ["random_state"], 42))

    def train(self):
        model_name = self._get_model_name()
        if model_name == "gnn" and self._get_model_config().get("graph_input", False):
            raise ValueError(
                "graph_input=True requires explicit PyG Data/Batch inputs, not the raw window table. "
                "Use graph_input=False for the per-window modality feature graph in this trainer."
            )
        frame = self._load_data()
        self.data_quality = frame.attrs.get("data_quality", {})
        targets = get_target_columns(frame, self.config)
        quarantined = frame.attrs.get("data_quality", {}).get("quarantined_targets", [])
        requested = get_config_value(self.config, ["target_columns"], [])
        if requested and set(targets) & set(quarantined):
            raise ValueError(f"Requested targets require corrected source data: {sorted(set(targets) & set(quarantined))}")
        targets = [target for target in targets if target not in quarantined]
        if not targets:
            raise ValueError("No numeric music target columns were found in the data.")
        group = get_group_column(self.config)
        features = get_feature_columns(
            frame, targets, group,
            drop_columns=get_config_value(self.config, ["dataset", "drop_features"], []),
        )
        if not features:
            raise ValueError("No situational context feature columns were found.")
        train, val, test = self._split_data(frame, group)
        data_path = resolve_path(get_config_value(
            self.config, ["data_path"], "data/raw/features_all_windows_combined.rds"
        ))
        digest = hashlib.sha256()
        with data_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        split_identity = {
            name: sorted(str(value) for value in part[group].unique())
            for name, part in [("train", train), ("validation", val), ("test", test)]
            if part is not None
        }
        self.run_metadata = {
            "run_id": uuid.uuid4().hex,
            "data_path": str(data_path), "data_sha256": digest.hexdigest(),
            "split_sha256": hashlib.sha256(json.dumps(split_identity, sort_keys=True).encode()).hexdigest(),
            "seed": self.seed,
        }
        output = resolve_path(get_config_value(self.config, ["output_dir"], "outputs/models"))
        output.mkdir(parents=True, exist_ok=True)
        results = []
        skipped = [{"target": target, "reason": "source feature collision; regenerate labels"}
                   for target in quarantined]
        for target in targets:
            if not np.isfinite(pd.to_numeric(train[target], errors="coerce")).any():
                LOGGER.warning("Skipping %s: no observed training labels.", target)
                skipped.append({"target": target, "reason": "no observed training labels"})
                continue
            LOGGER.info("Training %s for %s", model_name, target)
            result = self._train_single_target(train, val, test, features, target)
            results.append(result)
            self._write_json(output / f"{model_name}_metrics.json", results)
        if not results:
            raise ValueError("No selected targets have observed training labels.")
        manifest = {
            **self.run_metadata,
            "seed": self.seed,
            "model": model_name,
            "data_path": str(resolve_path(get_config_value(
                self.config, ["data_path"], "data/raw/features_all_windows_combined.rds"
            ))),
            "feature_columns": features,
            "data_quality": frame.attrs.get("data_quality", {}),
            "skipped_targets": skipped,
            "split_counts": {
                name: {"rows": len(part), "participants": int(part[group].nunique())}
                for name, part in [("train", train), ("validation", val), ("test", test)]
                if part is not None
            },
            "config": self._plain_config(),
            "artifacts": [
                {"target": result["target"], "file": f"{model_name}_{re.sub(r'[^A-Za-z0-9_.-]', '_', result['target'])}.joblib",
                 "run_id": self.run_metadata["run_id"]}
                for result in results
            ],
            "versions": {name: importlib.import_module(name).__version__
                         for name in ["numpy", "pandas", "sklearn", "torch"]},
        }
        from importlib.metadata import version

        if model_name in NEURAL_MODELS:
            manifest["versions"]["lightning"] = version("lightning")
            if model_name == "gnn":
                manifest["versions"]["torch_geometric"] = version("torch-geometric")
        elif model_name in {"lightgbm", "tabpfn"}:
            manifest["versions"][model_name] = version(model_name)
        self._write_json(output / f"{model_name}_manifest.json", manifest)
        return results

    def _load_data(self) -> pd.DataFrame:
        path = resolve_path(get_config_value(
            self.config, ["data_path"], "data/raw/features_all_windows_combined.rds"
        ))
        if not path.exists():
            raise FileNotFoundError(f"Training data not found at {path}")
        if "preprocessed_features" in path.name:
            raise ValueError(
                "The legacy preprocessed_features artifact was fitted before splitting. "
                "Use the raw RDS or an untransformed CSV to avoid preprocessing leakage."
            )
        frame = load_data(path)
        if any(str(column).startswith(("num__", "cat__", "bin__", "pass__")) for column in frame):
            raise ValueError(
                "Legacy transformed column prefixes indicate preprocessing before splitting. "
                "Use untransformed raw observations, not a renamed processed artifact."
            )
        group = get_group_column(self.config)
        quality = {"input_rows": len(frame), "quarantined_rows": 0, "quarantined_targets": []}
        if group in frame and len(frame.columns) > 1:
            signatures = pd.util.hash_pandas_object(frame.drop(columns=[group]), index=False)
            users_per_signature = frame[group].groupby(signatures).nunique()
            ambiguous = signatures.isin(users_per_signature[users_per_signature > 1].index)
            quality["quarantined_rows"] = int(ambiguous.sum())
            if ambiguous.any():
                LOGGER.warning(
                    "Quarantining %d windows identical across different participants. "
                    "Regenerate features from the corrected extractor; other contamination "
                    "cannot be repaired from aggregated data.", int(ambiguous.sum()),
                )
                frame = frame.loc[~ambiguous].copy()
                quality["provisional_source_data"] = True
        if {"liwc_anger", "genius_anger"}.issubset(frame.columns):
            if frame["genius_anger"].nunique() > 1 and frame["liwc_anger"].equals(frame["genius_anger"]):
                quality["quarantined_targets"].append("liwc_anger")
                quality["provisional_source_data"] = True
                LOGGER.warning("Quarantining liwc_anger: it duplicates genius_anger; regenerate independent LIWC labels.")
        max_rows = get_config_value(self.config, ["dataset", "max_rows"])
        if max_rows is not None:
            if isinstance(max_rows, bool) or not isinstance(max_rows, (int, np.integer)) or max_rows < 1:
                raise ValueError("dataset.max_rows must be a positive integer or null.")
            available = len(frame)
            if available > max_rows:
                frame = frame.sample(n=int(max_rows), random_state=self.seed).sort_index()
                LOGGER.info("Using an explicit %d/%d-window cohort (seed=%d) before participant splitting.",
                            len(frame), available, self.seed)
            quality["sampling"] = {"max_rows": int(max_rows), "available_rows": available,
                                   "selected_rows": len(frame), "seed": self.seed}
        frame.attrs["data_quality"] = quality
        return frame

    def _split_data(self, frame, group):
        splitter = DataSplitter(
            test_size=float(get_config_value(self.config, ["test_size"], 0.2)),
            val_size=float(get_config_value(self.config, ["val_size"], 0.1)),
            random_state=self.seed,
        )
        return splitter.group_split(frame, group)

    def _observed(self, frame, target):
        if frame is None:
            return None
        frame = frame.copy()
        frame[target] = pd.to_numeric(frame[target], errors="coerce")
        return frame.loc[np.isfinite(frame[target])].copy()

    def _train_single_target(self, train_frame, val_frame, test_frame, feature_columns, target_column):
        model_name = self._get_model_name()
        config = self._get_model_config()
        validation_train = self._observed(train_frame, target_column)
        validation_frame = self._observed(val_frame, target_column)
        if model_name not in (NEURAL_MODELS | {"lightgbm"}) and val_frame is not None:
            train_frame = pd.concat([train_frame, val_frame], ignore_index=True)
            val_frame = None
        train = self._observed(train_frame, target_column)
        val = self._observed(val_frame, target_column)
        test = self._observed(test_frame, target_column)
        if len(train) < 2:
            raise ValueError(f"Target {target_column} requires at least two observed training labels.")
        if model_name in NEURAL_MODELS and (val is None or val.empty):
            raise ValueError(f"Target {target_column} requires observed validation labels for early stopping.")
        config, cv_metrics = self._select_config(train, feature_columns, target_column, config)
        validation_metrics = {}
        if (not cv_metrics and model_name not in (NEURAL_MODELS | {"lightgbm"})
                and validation_frame is not None and not validation_frame.empty):
            validation_predictor = self._fit_predictor(
                validation_train, None, feature_columns, target_column, config
            )
            validation_metrics = {
                f"validation_{key}": value
                for key, value in self._metrics(
                    validation_predictor.predict(validation_frame), validation_frame[target_column].to_numpy()
                ).items()
            }
        predictor = self._fit_predictor(train, val, feature_columns, target_column, config)
        self.model = predictor.model
        metrics = {"target": target_column, "n_train": len(train),
                   "n_validation": len(val) if val is not None else 0,
                   "n_test": len(test), **cv_metrics, **validation_metrics,
                   "test_evaluated": bool(get_config_value(self.config, ["evaluate_test"], False)) and not test.empty,
                   "provisional_source_data": bool(getattr(self, "data_quality", {}).get("provisional_source_data", False))}
        if model_name in NEURAL_MODELS:
            metrics["evaluation_device"] = str(next(predictor.model.parameters()).device)
        if val is not None and not val.empty:
            metrics.update({
                f"validation_{key}": value
                for key, value in self._metrics(predictor.predict(val), val[target_column].to_numpy()).items()
            })
        if validation_metrics or (val is not None and not val.empty):
            metrics["validation_n_samples"] = len(validation_frame)
        if model_name == "lightgbm":
            metrics["best_iteration"] = int(predictor.model.model.best_iteration or config.get("num_rounds", 300))
        if metrics["test_evaluated"] and not test.empty:
            predictions = predictor.predict(test)
            observed = test[target_column].to_numpy(dtype=float)
            metrics.update(self._metrics(predictions, observed))
            baseline = np.full(len(test), train[target_column].mean())
            baseline_metrics = self._metrics(baseline, observed)
            metrics.update({f"baseline_{key}": value for key, value in baseline_metrics.items()})
            baseline_mse = np.mean((baseline - observed) ** 2)
            metrics["MSE_skill"] = float(1 - np.mean((predictions - observed) ** 2) / baseline_mse) if baseline_mse else None
            errors = pd.DataFrame({"group": test[get_group_column(self.config)].to_numpy(),
                                   "sq_error": (predictions - observed) ** 2})
            metrics["participant_macro_RMSE"] = float(np.sqrt(errors.groupby("group")["sq_error"].mean()).mean())
        elif test.empty:
            LOGGER.warning("Target %s has no observed test labels; saving model without test scores.", target_column)
        log_model_metrics(model_name, metrics)
        safe_target = re.sub(r"[^A-Za-z0-9_.-]", "_", target_column)
        output = resolve_path(get_config_value(self.config, ["output_dir"], "outputs/models"))
        if model_name in NEURAL_MODELS:
            predictor.model.cpu().eval()
        joblib.dump(predictor, output / f"{model_name}_{safe_target}.joblib")
        return metrics

    def _select_config(self, train, features, target, config):
        grid = get_config_value(self.config, ["tuning", "param_grid"], {})
        if not grid:
            grid = config.get("param_grid", {})
        if not grid or not get_config_value(self.config, ["tuning", "enabled"], True):
            return config, {}
        if self._get_model_name() in NEURAL_MODELS:
            raise ValueError("Neural param_grid tuning is not supported; use the participant validation split.")
        group = get_group_column(self.config)
        folds = int(get_config_value(self.config, ["tuning", "cv_folds"], 5))
        if folds < 2 or train[group].nunique() < folds:
            raise ValueError(f"Group cross-validation requires at least {folds} labeled participants and cv_folds >= 2.")
        best_config, best_rmse, best_std = config, float("inf"), 0.0
        for params in ParameterGrid(self._plain_mapping(grid)):
            candidate = {**config, **params}
            total_error, total_rows, scores = 0.0, 0, []
            for train_ids, val_ids in GroupKFold(n_splits=folds).split(train, groups=train[group]):
                fold_train, fold_val = train.iloc[train_ids], train.iloc[val_ids]
                fitted = self._fit_predictor(fold_train, None, features, target, candidate)
                error = (fitted.predict(fold_val) - fold_val[target].to_numpy()) ** 2
                total_error += float(error.sum())
                total_rows += len(error)
                scores.append(float(np.sqrt(error.mean())))
            rmse = float(np.sqrt(total_error / total_rows))
            if rmse < best_rmse:
                best_config, best_rmse, best_std = candidate, rmse, float(np.std(scores))
        return best_config, {"cv_RMSE": best_rmse, "cv_fold_RMSE_std": best_std,
                             "cv_folds": folds, "selected_params": {k: best_config[k] for k in grid}}

    def _fit_predictor(self, train, val, features, target, config):
        threads = int(get_config_value(self.config, ["cpu_threads"], 4))
        if threads < 1:
            raise ValueError("cpu_threads must be positive.")
        torch.set_num_threads(threads)
        random.seed(self.seed)
        np.random.seed(self.seed)
        torch.manual_seed(self.seed)
        preprocessor = build_preprocessor(
            train[list(features)], drop_features=get_config_value(self.config, ["dataset", "drop_features"], [])
        )
        x = preprocessor.fit_transform(train[list(features)])
        if not np.isfinite(x).all():
            raise ValueError("Feature preprocessing produced nonfinite values.")
        target_scaler = StandardScaler().fit(train[[target]])
        y = target_scaler.transform(train[[target]])
        model_name = self._get_model_name()
        model_config = {**config, "input_dim": x.shape[1], "output_dim": 1,
                         "random_state": config.get("random_state", self.seed)}
        if model_name in NEURAL_MODELS:
            selected_precision = get_config_value(self.config, ["precision"], config.get("precision", "32-true"))
            selected_precision = {32: "32-true", 64: "64-true", 16: "16-mixed",
                                  "32": "32-true", "64": "64-true", "16": "16-mixed",
                                  "bf16": "bf16-mixed"}.get(selected_precision, selected_precision)
            if selected_precision not in {"32-true", "64-true", "16-mixed", "bf16-mixed"}:
                raise ValueError("Supported neural precisions are 32-true, 64-true, 16-mixed and bf16-mixed; true-half training is unsupported.")
            model_config["precision"] = selected_precision
        if model_name == "gnn":
            model_config["feature_names"] = preprocessor.get_feature_names_out().tolist()
        model = self._build_model(model_name, model_config, x.shape[1])
        loader = _loader(x, y, batch_size=int(get_config_value(self.config, ["batch_size"], 256)),
                         shuffle=model_name in NEURAL_MODELS, seed=self.seed,
                         workers=int(get_config_value(self.config, ["num_workers"], 0)),
                         pin_memory=model_name in NEURAL_MODELS and torch.cuda.is_available()
                         and get_config_value(self.config, ["accelerator"], "auto") != "cpu")
        if model_name in NEURAL_MODELS:
            from lightning.pytorch import Trainer as LightningTrainer
            from lightning.pytorch.callbacks import EarlyStopping, ModelCheckpoint

            val_loader = _loader(preprocessor.transform(val[list(features)]),
                                 target_scaler.transform(val[[target]]),
                                 batch_size=int(get_config_value(self.config, ["batch_size"], 256)),
                                 pin_memory=loader.pin_memory)
            with tempfile.TemporaryDirectory(prefix="music-checkpoint-") as checkpoint_dir:
                checkpoint = ModelCheckpoint(dirpath=checkpoint_dir, monitor="val_loss", mode="min", save_top_k=1)
                early_stop = EarlyStopping(monitor="val_loss", mode="min", patience=int(config.get("early_stopping_patience", config.get("patience", 12))),
                                           min_delta=float(config.get("min_delta", 1e-4)), check_finite=True)
                trainer = LightningTrainer(
                    max_epochs=int(config.get("max_epochs", 100)),
                    accelerator=get_config_value(self.config, ["accelerator"], "auto"),
                    devices=get_config_value(self.config, ["devices"], 1),
                    precision=model_config["precision"],
                    logger=False, callbacks=[checkpoint, early_stop],
                    deterministic=True, enable_progress_bar=bool(get_config_value(self.config, ["progress_bar"], False)),
                    enable_model_summary=False, num_sanity_val_steps=0,
                )
                trainer.fit(model, train_dataloaders=loader, val_dataloaders=val_loader)
                if not checkpoint.best_model_path:
                    raise RuntimeError("No valid neural validation checkpoint was produced.")
                state = torch.load(checkpoint.best_model_path, map_location="cpu", weights_only=True)
                model.load_state_dict(state["state_dict"])
            model.to(trainer.strategy.root_device).eval()
            model._trainer = None
        else:
            if model_name == "lightgbm" and val is not None and not val.empty:
                val_loader = _loader(preprocessor.transform(val[list(features)]),
                                     target_scaler.transform(val[[target]]))
                model.fit(loader, val_dataloader=val_loader)
            else:
                model.fit(loader)
        return TrainedPredictor(model, model_name, preprocessor, target_scaler, list(features),
                                target, model_config, dict(getattr(self, "data_quality", {})),
                                dict(getattr(self, "run_metadata", {})))

    def _build_model(self, model_name, model_config, input_dim):
        registry = {
            "baseline": ("models.baseline", "BaselineModel"),
            "elastic_net": ("models.elastic_net", "ElasticNetModel"),
            "random_forest": ("models.random_forest", "RandomForestModel"),
            "lightgbm": ("models.lightgbm_model", "LightGBMModel"),
            "tabpfn": ("models.tabpfn", "TabPFNModel"),
            "neural_net": ("models.neural_net", "NeuralNet"),
            "transformer": ("models.transformer", "TransformerModel"),
            "gnn": ("models.gnn", "GNNModel"),
        }
        if model_name not in registry:
            raise ValueError(f"Unknown model {model_name!r}; available: {', '.join(registry)}")
        module, name = registry[model_name]
        return getattr(importlib.import_module(module), name)({**model_config, "input_dim": input_dim, "output_dim": 1})

    def _get_model_name(self):
        return str(self._get_model_config().get("name", "elastic_net"))

    def _get_model_config(self):
        config = self._plain_mapping(get_config_value(self.config, ["model"], {}))
        checkpoint = get_config_value(self.config, ["tabpfn_checkpoint"])
        if config.get("name") == "tabpfn" and checkpoint is not None:
            config["model_path"] = str(resolve_path(checkpoint))
            config["model_kwargs"] = dict(config.get("model_kwargs") or {})
            config["model_kwargs"].pop("model_path", None)
        return config

    @staticmethod
    def _plain_mapping(value):
        from omegaconf import OmegaConf

        return OmegaConf.to_container(value, resolve=True) if OmegaConf.is_config(value) else dict(value)

    def _plain_config(self):
        return self._plain_mapping(self.config)

    @staticmethod
    def _metrics(predictions, observed):
        return {key: float(value) if torch.isfinite(value) else None
                for key, value in calculate_metrics(predictions, observed).items()}

    @staticmethod
    def _write_json(path, value):
        with path.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, allow_nan=False)
