from abc import ABC, abstractmethod
from typing import Any

import joblib
import numpy as np
import torch


class BaseModel(ABC):
    """Single-target regression interface for tabular window loaders."""

    def __init__(self, config: dict[str, Any], wandb_logger=None):
        self.config = dict(config)
        self.wandb_logger = wandb_logger

    @abstractmethod
    def fit(self, dataloader, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def predict(self, dataloader, **kwargs) -> torch.Tensor:
        """Return one prediction per yielded row, shaped [N, 1]."""
        raise NotImplementedError

    @staticmethod
    def _loader_arrays(dataloader, require_targets=True):
        features, targets = [], []
        for x, y in dataloader:
            x = torch.as_tensor(x).detach().cpu()
            if x.ndim != 2:
                raise ValueError("Tabular models require features shaped [rows, features].")
            features.append(x.numpy())
            if require_targets:
                y = torch.as_tensor(y).detach().cpu()
                if y.ndim not in (1, 2) or (y.ndim == 2 and y.shape[1] != 1):
                    raise ValueError("Models learn one target at a time: use y shaped [N] or [N, 1].")
                if len(y) != len(x):
                    raise ValueError("Each feature row must have exactly one target.")
                targets.append(y.reshape(-1).numpy())
        if not features or not sum(len(x) for x in features):
            raise ValueError("The data loader must yield at least one row.")
        X = np.concatenate(features, axis=0)
        y = np.concatenate(targets) if require_targets else None
        if not np.isfinite(X).all() or (y is not None and not np.isfinite(y).all()):
            raise ValueError("Models require finite preprocessed features and observed targets.")
        return X, y

    def evaluate(self, dataloader) -> dict[str, float]:
        """Freeze one loader traversal so shuffling cannot misalign targets."""
        from utils.metrics import calculate_metrics

        batches = list(dataloader)
        if not batches:
            raise ValueError("Cannot evaluate an empty data loader.")
        _, targets = self._loader_arrays(batches)
        predictions = self.predict(batches).detach().cpu().reshape(-1)
        if predictions.numel() != len(targets):
            raise ValueError("Predictions and targets must have equal lengths.")
        return {
            name: float(value)
            for name, value in calculate_metrics(predictions, torch.as_tensor(targets)).items()
        }

    def save(self, path):
        """Persist classical wrappers or neural weights (load with architecture config)."""
        if isinstance(self, torch.nn.Module):
            torch.save(self.state_dict(), path)
        else:
            joblib.dump(self, path)

    @classmethod
    def load(cls, path, config: dict[str, Any] | None = None):
        """Load trusted local artifacts; neural weights are restored on CPU."""
        if issubclass(cls, torch.nn.Module):
            model = cls(config or {})
            state = torch.load(path, map_location="cpu", weights_only=True)
            model.load_state_dict(state.get("state_dict", state))
            return model.eval()
        loaded = joblib.load(path)
        if isinstance(loaded, cls):
            return loaded
        # Existing trainer artifacts contain the estimator rather than the wrapper.
        model = cls(config or {})
        if hasattr(model, "model") and hasattr(loaded, "predict"):
            model.model = loaded
            return model
        raise ValueError(f"Artifact does not contain a fitted {cls.__name__}.")


def __getattr__(name):
    # Preserve the existing neural base import without requiring Lightning for baselines.
    if name == "LightningBaseModel":
        from .lightning_base import LightningBaseModel

        return LightningBaseModel
    raise AttributeError(name)
