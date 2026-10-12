from numbers import Integral
from typing import Any

import torch

from .base_model import BaseModel


class LightGBMModel(BaseModel):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        device_type = self.config.get("device_type", "cpu")
        if not isinstance(device_type, str) or device_type not in ("cpu", "gpu", "cuda"):
            raise ValueError("LightGBM device_type must be 'cpu', 'gpu' (OpenCL), or 'cuda'.")
        seed = self.config.get("random_state", 42)
        self.params = {
            "objective": "regression",
            "metric": "l2",
            "device_type": device_type,
            "boosting_type": self.config.get("boosting_type", "gbdt"),
            "num_leaves": self.config.get("num_leaves", 31),
            "max_depth": self.config.get("max_depth", 8),
            "min_data_in_leaf": self.config.get("min_data_in_leaf", 50),
            "learning_rate": self.config.get("learning_rate", 0.05),
            "feature_fraction": self.config.get("feature_fraction", 0.8),
            "bagging_fraction": self.config.get("bagging_fraction", 0.8),
            "bagging_freq": self.config.get("bagging_freq", 1),
            "lambda_l1": self.config.get("lambda_l1", 0.0),
            "lambda_l2": self.config.get("lambda_l2", 1.0),
            "verbosity": self.config.get("verbose", -1),
            "seed": seed,
            "bagging_seed": seed,
            "feature_fraction_seed": seed,
            "data_random_seed": seed,
            "num_threads": self.config.get("n_jobs", 4),
        }
        # These histogram/reproducibility settings are supported only on CPU.
        if device_type == "cpu":
            self.params.update(deterministic=True, force_col_wise=True)
        for option in ("gpu_device_id", "gpu_platform_id"):
            value = self.config.get(option)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, Integral) or value < -1:
                raise ValueError(f"LightGBM {option} must be an integer >= -1 (-1 selects the default).")
            if device_type == "cpu" or (option == "gpu_platform_id" and device_type != "gpu"):
                supported = "device_type='gpu' (OpenCL)" if option == "gpu_platform_id" else "device_type='gpu' or 'cuda'"
                raise ValueError(f"LightGBM {option} requires {supported}; leave it null otherwise.")
            self.params[option] = int(value)
        self.num_rounds = self.config.get("num_rounds", 300)
        self.model = None

    def fit(self, dataloader, val_dataloader=None, **kwargs):
        """Optionally early-stop on a caller-supplied participant-disjoint fold."""
        try:
            import lightgbm as lgb
        except ImportError as exc:
            raise ImportError("LightGBM requires the optional 'lightgbm' extra: pip install -e '.[lightgbm]'.") from exc
        X, y = self._loader_arrays(dataloader)
        train_data = lgb.Dataset(X, label=y)
        val_dataloader = val_dataloader if val_dataloader is not None else kwargs.get("val_loader")
        valid_sets = None
        callbacks = [lgb.log_evaluation(period=0)]
        if val_dataloader is not None:
            X_val, y_val = self._loader_arrays(val_dataloader)
            valid_sets = [lgb.Dataset(X_val, label=y_val, reference=train_data)]
            patience = self.config.get("early_stopping_rounds", 30)
            if patience > 0:
                callbacks.append(lgb.early_stopping(patience, verbose=False))
        try:
            self.model = lgb.train(
                self.params, train_data, num_boost_round=self.num_rounds,
                valid_sets=valid_sets, callbacks=callbacks,
            )
        except lgb.basic.LightGBMError as exc:
            device_type = self.params["device_type"]
            if device_type == "cpu":
                raise
            if device_type == "cuda":
                guidance = (
                    "Use a CUDA-enabled LightGBM build (cmake.define.USE_CUDA=ON), a compatible "
                    "NVIDIA GPU/driver and CUDA runtime on Linux; an ordinary pip wheel or "
                    "CUDA-enabled PyTorch does not provide LightGBM CUDA support."
                )
            else:
                guidance = (
                    "Use an OpenCL-enabled LightGBM build (cmake.define.USE_GPU=ON) and a working "
                    "vendor OpenCL runtime; check gpu_platform_id and gpu_device_id."
                )
            raise RuntimeError(
                f"LightGBM device_type='{device_type}' training failed. {guidance} "
                f"No CPU fallback was attempted. Backend error: {exc}"
            ) from exc
        return self

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs) -> torch.Tensor:
        """Predict using LightGBM model."""
        if self.model is None:
            raise RuntimeError("Fit LightGBM before predicting.")
        X, _ = self._loader_arrays(dataloader, require_targets=False)
        predictions = self.model.predict(X, num_iteration=self.model.best_iteration or None)
        return torch.as_tensor(predictions, dtype=torch.float32).reshape(-1, 1)
