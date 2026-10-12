from __future__ import annotations

import json
import os
from importlib.metadata import PackageNotFoundError, version
from io import BytesIO
from numbers import Integral
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from zipfile import BadZipFile, ZipFile

import torch

from .base_model import BaseModel


class TabPFNModel(BaseModel):
    """Opt-in backend with conservative, configurable local resource limits."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.model = None
        self._fitted_payload = None
        self._fitted_versions = None
        self._model_device = None

    @staticmethod
    def _offline_defaults():
        # Documented opt-out, set before importing versions that initialize telemetry.
        os.environ["TABPFN_DISABLE_TELEMETRY"] = "1"
        os.environ["TABPFN_NO_BROWSER"] = "1"

    @staticmethod
    def _native_helpers():
        TabPFNModel._offline_defaults()
        try:
            from tabpfn.model_loading import load_fitted_tabpfn_model, save_fitted_tabpfn_model
            if not callable(save_fitted_tabpfn_model) or not callable(load_fitted_tabpfn_model):
                raise ImportError("Native fitted persistence helpers must be callable.")
        except ImportError as exc:
            raise ImportError(
                "TabPFN artifacts require native fitted persistence helpers in tabpfn.model_loading. "
                "Install/upgrade the optional TabPFN package to a version providing "
                "save_fitted_tabpfn_model and load_fitted_tabpfn_model. Generic estimator pickling "
                "is not supported because it duplicates foundation weights and is not CUDA-portable."
            ) from exc
        return save_fitted_tabpfn_model, load_fitted_tabpfn_model

    @staticmethod
    def _backend_versions():
        # Native archives contain sklearn preprocessors and Torch executor state.
        try:
            return {name: version(name) for name in ("tabpfn", "scikit-learn", "torch", "numpy")}
        except PackageNotFoundError as exc:
            raise ImportError("TabPFN fitted artifacts require the recorded optional backend environment; install its dependencies before predicting.") from exc

    @staticmethod
    def _device(value):
        if value == "auto":
            excluded = os.environ.get("TABPFN_EXCLUDE_DEVICES", "").split(",")
            value = (
                "cuda" if torch.cuda.is_available() and "cuda" not in excluded
                else "mps" if torch.backends.mps.is_available() and "mps" not in excluded
                else "cpu"
            )
        if not isinstance(value, (str, torch.device)):
            raise ValueError("TabPFN wrapper device must be a single cpu, cuda[:index], mps or auto device.")
        try:
            device = torch.device(value)
        except (RuntimeError, ValueError) as exc:
            raise ValueError(f"Unsupported TabPFN device: {value}") from exc
        if device.type not in {"cpu", "cuda", "mps"}:
            raise ValueError(f"Unsupported TabPFN device: {value}; use cpu, cuda[:index], mps or auto.")
        if device.type == "cuda":
            if not torch.cuda.is_available():
                raise ValueError("TabPFN requested CUDA, but no GPU is available. Use device='cpu'.")
            if device.index is not None and device.index >= torch.cuda.device_count():
                raise ValueError(f"TabPFN CUDA device index is unavailable: {value}")
        if device.type == "mps" and not torch.backends.mps.is_available():
            raise ValueError("TabPFN requested MPS, but MPS is unavailable. Use device='cpu'.")
        return str(device)

    @staticmethod
    def _positive_int(name, value):
        if isinstance(value, bool) or not isinstance(value, Integral) or value < 1:
            raise ValueError(f"TabPFN {name} must be a positive integer.")
        return int(value)

    def _validate_checkpoint(self):
        model_path = self.config.get("model_kwargs", {}).get("model_path", self.config.get("model_path"))
        if model_path is not None:
            if not isinstance(model_path, (str, Path)) or not Path(model_path).is_file():
                raise ValueError(f"TabPFN model_path must be an existing local checkpoint: {model_path}")
            if Path(model_path).suffix not in {".ckpt", ".safetensors"}:
                raise ValueError("TabPFN model_path must be a local .ckpt or .safetensors file.")
            return str(Path(model_path).resolve())
        if not self.config.get("allow_model_download", False):
            raise ValueError(
                "TabPFN requires a local model_path checkpoint by default. Automatic model download is disabled. "
                "Supply a compatible local .ckpt/.safetensors file, or explicitly enable download of public v2 weights. "
                "No license acceptance or authentication is performed by this wrapper."
            )
        if self.config.get("model_version", "v2") != "v2":
            raise ValueError("Automatic TabPFN downloads are restricted to ungated public v2 weights; use local weights for other versions.")
        return None

    def _reload_payload(self, model_path):
        # The native loader has no model_path override: never let a stale archived
        # path bypass the local-file guard or trigger an unintended download.
        output = BytesIO()
        try:
            with ZipFile(BytesIO(self._fitted_payload)) as source, ZipFile(output, "w") as destination:
                params = json.loads(source.read("init_params.json"))
                if model_path is not None:
                    params["model_path"] = model_path
                elif not isinstance(params.get("model_path"), str) or Path(params["model_path"]).name != "tabpfn-v2-regressor.ckpt":
                    raise ValueError("Download-enabled fitted archives must explicitly reference public v2 regression weights.")
                for member in source.infolist():
                    data = json.dumps(params).encode("utf-8") if member.filename == "init_params.json" else source.read(member)
                    destination.writestr(member, data)
        except (BadZipFile, KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("Invalid TabPFN native fitted archive; refit with a compatible package version.") from exc
        return output.getvalue()

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
        state["_model_device"] = None
        return state

    def __setstate__(self, state):
        """Keep loading offline; external weights are resolved only on prediction."""
        self.__dict__.update(state)
        self.model = None
        self._model_device = None

    def _ensure_model(self, inference_device=None):
        """Rehydrate on CPU by default, independently of the original fitting device."""
        if self.model is None and self._fitted_payload is None:
            raise RuntimeError("Fit TabPFN successfully before predicting.")
        model_path = self._validate_checkpoint()
        device = self._device(inference_device if inference_device is not None else self.config.get("inference_device", "cpu"))
        if self.model is not None:
            if device != self._model_device:
                self.model.to(device)
                self._model_device = device
            return self.model
        saved_versions = getattr(self, "_fitted_versions", None)
        installed_versions = self._backend_versions()
        # CPU/CUDA wheel build tags do not change the Torch serialization format.
        incompatible = not saved_versions or set(saved_versions) != set(installed_versions) or any(
            (str(saved_versions[name]).split("+")[0] if name == "torch" else saved_versions[name])
            != (installed.split("+")[0] if name == "torch" else installed)
            for name, installed in installed_versions.items()
        )
        if incompatible:
            raise RuntimeError(
                f"TabPFN fitted archive backend versions differ or are missing: saved={saved_versions}, "
                f"installed={installed_versions}. Restore the recorded environment or refit."
            )
        _, load_fitted = self._native_helpers()
        try:
            with TemporaryDirectory(prefix="tabpfn-context-") as directory:
                path = Path(directory) / "fitted.tabpfn_fit"
                path.write_bytes(self._reload_payload(model_path))
                model = load_fitted(path, device=device)
        except (OSError, RuntimeError, ValueError, TypeError) as exc:
            raise RuntimeError(
                "TabPFN fitted-context reload failed. Check the compatible TabPFN version, external checkpoint, "
                f"weight license/authentication, download policy and {device} memory. Original error: {exc}"
            ) from exc
        self.model = model
        self._model_device = device
        return model

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Never silently subsample participants or download pretrained weights."""
        X, y = self._loader_arrays(dataloader)
        self.model = None
        self._fitted_payload = None
        self._fitted_versions = None
        self._model_device = None
        model_kwargs = dict(self.config.get("model_kwargs", {}))
        device = self._device(model_kwargs.get("device", self.config.get("device", "cpu")))
        row_limit = self._positive_int("max_train_rows", self.config.get("max_train_rows", 10000))
        cpu_limit = self._positive_int("max_cpu_rows", self.config.get("max_cpu_rows", 1000))
        if device.startswith("cpu"):
            row_limit = min(row_limit, cpu_limit)
        feature_limit = self._positive_int("max_features", self.config.get("max_features", 500))
        if len(X) > row_limit or X.shape[1] > feature_limit:
            raise ValueError(
                f"TabPFN input ({len(X)} rows, {X.shape[1]} features) exceeds configured {device} "
                f"resource limits ({row_limit} rows, {feature_limit} features). These are wrapper safeguards, "
                "not universal TabPFN architecture limits. Set an explicit dataset.max_rows cap BEFORE "
                "participant splitting for the same cohort across all models, or configure adequate resources. "
                "The wrapper never samples rows implicitly."
            )
        model_path = self._validate_checkpoint()
        if model_path is not None:
            model_kwargs["model_path"] = model_path
        model_kwargs["device"] = device
        model_kwargs.setdefault("random_state", self.config.get("random_state", 42))
        seed = model_kwargs["random_state"]
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, Integral) or not 0 <= seed < 2**32):
            raise ValueError("TabPFN random_state must be an integer in [0, 2**32) or None.")
        if seed is not None:
            model_kwargs["random_state"] = int(seed)
        model_kwargs.setdefault("n_estimators", 4)
        if model_kwargs["n_estimators"] != "auto":
            model_kwargs["n_estimators"] = self._positive_int("n_estimators", model_kwargs["n_estimators"])
        inference_config = model_kwargs.get("inference_config") or {}
        if not isinstance(inference_config, dict):
            raise ValueError("TabPFN inference_config must be a JSON-compatible dictionary.")
        if inference_config.get("SUBSAMPLE_SAMPLES") is not None:
            raise ValueError("TabPFN row subsampling is disabled; use the common explicit dataset.max_rows cap instead.")
        model_kwargs["inference_config"] = dict(inference_config, SUBSAMPLE_SAMPLES=None)
        self._offline_defaults()
        try:
            from tabpfn import TabPFNRegressor
        except ImportError as exc:
            raise ImportError("TabPFN requires the optional 'tabpfn' extra: pip install -e '.[tabpfn]'.") from exc
        # Trainer always persists fitted artifacts, so fail before loading any weights.
        self._native_helpers()
        backend_versions = self._backend_versions()
        try:
            self.model = (
                TabPFNRegressor(**model_kwargs) if model_path is not None
                else TabPFNRegressor.create_default_for_version("v2", **model_kwargs)
            )
            self.model.fit(X, y)
        except (OSError, RuntimeError, ValueError, TypeError) as exc:
            self.model = None
            raise RuntimeError(
                "TabPFN initialization/fit failed. Check checkpoint compatibility, weight license/authentication, "
                f"model download access and {device} memory; a classical regressor does not require weights. "
                f"Original error: {exc}"
            ) from exc
        self._fitted_versions = backend_versions
        self._model_device = device
        return self

    def predict(
        self, dataloader: torch.utils.data.DataLoader, inference_device=None, **kwargs
    ) -> torch.Tensor:
        """Predict with TabPFN regressor."""
        model = self._ensure_model(inference_device)
        X, _ = self._loader_arrays(dataloader, require_targets=False)
        predictions = model.predict(X)
        predictions = torch.as_tensor(predictions, dtype=torch.float32)
        if predictions.numel() != len(X) or not torch.isfinite(predictions).all():
            raise RuntimeError("TabPFN must return one finite regression prediction per input row.")
        return predictions.reshape(-1, 1)
