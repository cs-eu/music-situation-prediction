from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import torch

from .base_model import BaseModel


class TabPFNModel(BaseModel):
    """Opt-in backend with conservative, configurable local resource limits."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.model = None
        self._fitted_payload = None

    @staticmethod
    def _native_helpers():
        try:
            from tabpfn.model_loading import load_fitted_tabpfn_model, save_fitted_tabpfn_model
        except ImportError as exc:
            raise ImportError(
                "TabPFN artifacts require native fitted persistence helpers in tabpfn.model_loading. "
                "Install/upgrade the optional TabPFN package to a version providing "
                "save_fitted_tabpfn_model and load_fitted_tabpfn_model. Generic estimator pickling "
                "is not supported because it duplicates foundation weights and is not CUDA-portable."
            ) from exc
        return save_fitted_tabpfn_model, load_fitted_tabpfn_model

    def _validate_checkpoint(self):
        model_path = self.config.get("model_kwargs", {}).get("model_path", self.config.get("model_path"))
        if model_path is not None:
            if not Path(model_path).is_file():
                raise ValueError(f"TabPFN model_path must be an existing local checkpoint: {model_path}")
            return str(model_path)
        if not self.config.get("allow_model_download", False):
            raise ValueError(
                "TabPFN requires a local model_path checkpoint by default. Automatic model download is disabled. "
                "Supply licensed local weights or explicitly set allow_model_download=True and configure "
                "the authentication/license required by your installed TabPFN version."
            )
        return None

    def __getstate__(self):
        """Serialize native fitted context, never the live foundation-weight estimator."""
        state = self.__dict__.copy()
        if self.model is not None:
            save_fitted, _ = self._native_helpers()
            with TemporaryDirectory(prefix="tabpfn-context-") as directory:
                path = Path(directory) / "fitted.tabpfn_fit"
                save_fitted(self.model, path)
                state["_fitted_payload"] = path.read_bytes()
        state["model"] = None
        return state

    def __setstate__(self, state):
        """Keep loading offline; external weights are resolved only on prediction."""
        self.__dict__.update(state)
        self.model = None

    def _ensure_model(self, inference_device=None):
        """Rehydrate on CPU by default, independently of the original fitting device."""
        if self.model is not None:
            return self.model
        if self._fitted_payload is None:
            raise RuntimeError("Fit TabPFN successfully before predicting.")
        self._validate_checkpoint()
        device = str(inference_device if inference_device is not None else self.config.get("inference_device", "cpu"))
        if device.startswith("cuda") and not torch.cuda.is_available():
            raise ValueError("TabPFN inference requested CUDA, but no GPU is available. Use inference_device='cpu'.")
        _, load_fitted = self._native_helpers()
        try:
            with TemporaryDirectory(prefix="tabpfn-context-") as directory:
                path = Path(directory) / "fitted.tabpfn_fit"
                path.write_bytes(self._fitted_payload)
                model = load_fitted(path, device=device)
        except (OSError, RuntimeError, ValueError, TypeError) as exc:
            raise RuntimeError(
                "TabPFN fitted-context reload failed. Check the compatible TabPFN version, external checkpoint, "
                f"weight license/authentication, download policy and {device} memory. Original error: {exc}"
            ) from exc
        self.model = model
        return model

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Never silently subsample participants or download pretrained weights."""
        X, y = self._loader_arrays(dataloader)
        self.model = None
        self._fitted_payload = None
        model_kwargs = dict(self.config.get("model_kwargs", {}))
        device = str(model_kwargs.get("device", self.config.get("device", "cpu")))
        if device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        if device.startswith("cuda") and not torch.cuda.is_available():
            raise ValueError("TabPFN was configured for CUDA, but no GPU is available. Use a classical model on CPU.")
        row_limit = self.config.get("max_train_rows", 10000)
        if device.startswith("cpu"):
            row_limit = min(row_limit, self.config.get("max_cpu_rows", 1000))
        feature_limit = self.config.get("max_features", 500)
        if len(X) > row_limit or X.shape[1] > feature_limit:
            raise ValueError(
                f"TabPFN input ({len(X)} rows, {X.shape[1]} features) exceeds configured {device} "
                f"resource limits ({row_limit} rows, {feature_limit} features). These are wrapper safeguards, "
                "not universal TabPFN architecture limits. The ~58k-window/117-target task is not a "
                "practical default CPU TabPFN workload; use elastic_net, random_forest or lightgbm, "
                "or explicitly configure a supported checkpoint and adequate resources."
            )
        model_path = self._validate_checkpoint()
        if model_path is not None:
            model_kwargs["model_path"] = model_path
        try:
            from tabpfn import TabPFNRegressor
        except ImportError as exc:
            raise ImportError("TabPFN requires the optional 'tabpfn' extra: pip install -e '.[tabpfn]'.") from exc
        model_kwargs["device"] = device
        model_kwargs.setdefault("random_state", self.config.get("random_state", 42))
        model_kwargs.setdefault("n_estimators", 4)
        try:
            self.model = TabPFNRegressor(**model_kwargs)
            self.model.fit(X, y)
        except (OSError, RuntimeError, ValueError) as exc:
            self.model = None
            raise RuntimeError(
                "TabPFN initialization/fit failed. Check checkpoint compatibility, weight license/authentication, "
                f"model download access and {device} memory; a classical regressor does not require weights. "
                f"Original error: {exc}"
            ) from exc
        return self

    def predict(
        self, dataloader: torch.utils.data.DataLoader, inference_device=None, **kwargs
    ) -> torch.Tensor:
        """Predict with TabPFN regressor."""
        model = self._ensure_model(inference_device)
        X, _ = self._loader_arrays(dataloader, require_targets=False)
        predictions = model.predict(X)
        return torch.as_tensor(predictions, dtype=torch.float32).reshape(-1, 1)
