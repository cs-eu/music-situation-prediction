"""Offline adapter tests and real-package checks; tests never download weights."""

import json
import socket
import sys
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import joblib
import numpy as np
import pytest
import torch
from torch.utils.data import DataLoader, TensorDataset

from models.tabpfn import TabPFNModel


def loader(rows=24, features=3, *, targets=True):
    x = torch.randn(rows, features, generator=torch.Generator().manual_seed(42))
    y = x[:, :1] * 0.1 - x[:, 1:2] * 0.05 if features > 1 else x * 0.1
    if not targets:
        y = torch.full_like(y, float("nan"))
    return DataLoader(TensorDataset(x, y), batch_size=7)


@pytest.fixture(autouse=True)
def offline(monkeypatch, tmp_path):
    # Upstream 9.1 settings read cwd/.env with no public opt-out. Use an empty
    # test working directory, never the repository's credentials or HF storage.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TABPFN_DISABLE_TELEMETRY", "1")
    monkeypatch.setenv("TABPFN_NO_BROWSER", "1")
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")

    def no_network(*args, **kwargs):
        raise AssertionError("Offline TabPFN tests must not contact download/auth/upload services.")

    monkeypatch.setattr(socket.socket, "connect", no_network)


@pytest.fixture
def fake_backend(monkeypatch):
    """Protocol fake for guard coverage, not proof of foundation-model inference."""
    calls = {"init": [], "fit": [], "save": [], "load": [], "to": [], "version": []}

    class Regressor:
        def __init__(self, **kwargs):
            calls["init"].append(kwargs)
            self.params = kwargs
            self.device = kwargs["device"]

        @classmethod
        def create_default_for_version(cls, version, **kwargs):
            calls["version"].append(version)
            return cls(model_path="/tmp/kilo/tabpfn-v2-regressor.ckpt", **kwargs)

        def fit(self, x, y):
            assert isinstance(x, np.ndarray) and isinstance(y, np.ndarray)
            assert y.ndim == 1
            calls["fit"].append((x.copy(), y.copy()))
            self.mean = float(y.mean())

        def predict(self, x):
            return np.full(len(x), self.mean)

        def to(self, device):
            calls["to"].append(device)
            self.device = device

        def __reduce_ex__(self, protocol):
            raise AssertionError("Live foundation estimators must not be pickled.")

    def save(estimator, path):
        calls["save"].append(path)
        assert Path(path).suffix == ".tabpfn_fit"
        with ZipFile(path, "w") as archive:
            archive.writestr("init_params.json", json.dumps(estimator.params))
            archive.writestr("context.json", json.dumps({"mean": estimator.mean}))

    def load(path, *, device):
        with ZipFile(path) as archive:
            params = json.loads(archive.read("init_params.json"))
            context = json.loads(archive.read("context.json"))
        calls["load"].append({"params": params, "device": device})
        params["device"] = device
        estimator = Regressor(**params)
        estimator.mean = context["mean"]
        return estimator

    monkeypatch.setitem(sys.modules, "tabpfn", SimpleNamespace(TabPFNRegressor=Regressor))
    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", SimpleNamespace(
        save_fitted_tabpfn_model=save, load_fitted_tabpfn_model=load,
    ))
    monkeypatch.setattr(TabPFNModel, "_backend_versions", staticmethod(lambda: {
        "tabpfn": "9.1.0", "scikit-learn": "1.8.0", "torch": "2.6.0", "numpy": "1.26.3",
    }))
    return calls


@pytest.fixture
def checkpoint(tmp_path):
    path = tmp_path / "external.ckpt"
    path.write_bytes(b"fake external weights, not a real checkpoint")
    return path


@pytest.mark.parametrize("key", ["max_cpu_rows", "max_train_rows", "max_features"])
@pytest.mark.parametrize("value", [None, 0, -1, True, 3.5, "1000"])
def test_limits_are_positive_integers_before_backend_construction(fake_backend, checkpoint, key, value):
    with pytest.raises(ValueError, match=f"{key} must be a positive integer"):
        TabPFNModel({"model_path": checkpoint, key: value}).fit(loader())
    assert not fake_backend["init"]


@pytest.mark.parametrize("config,rows,features", [
    ({"max_cpu_rows": 23}, 24, 3),
    ({"max_train_rows": 23}, 24, 3),
    ({"max_features": 2}, 24, 3),
    ({}, 1001, 3),
])
def test_limits_reject_instead_of_silently_sampling(fake_backend, checkpoint, config, rows, features):
    with pytest.raises(ValueError, match="never samples rows implicitly"):
        TabPFNModel({"model_path": checkpoint, **config}).fit(loader(rows, features))
    assert not fake_backend["init"]
    assert not fake_backend["fit"]


@pytest.mark.parametrize("device", ["meta", "xpu", "tpu", "nonsense", "cuda:9", ["cpu"], None])
def test_unsupported_or_unavailable_devices_fail_early(fake_backend, checkpoint, device, monkeypatch):
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    with pytest.raises(ValueError):
        TabPFNModel({"model_path": checkpoint, "device": device}).fit(loader())
    assert not fake_backend["init"]


def test_auto_uses_available_mps_and_rejects_unavailable_mps(fake_backend, checkpoint, monkeypatch):
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: True)
    model = TabPFNModel({"model_path": checkpoint, "device": "auto"}).fit(loader())
    assert model._model_device == "mps"
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: False)
    with pytest.raises(ValueError, match="MPS is unavailable"):
        model.predict(loader(), inference_device="mps")


@pytest.mark.parametrize("seed", [-1, 2**32, True, 1.2, "42"])
def test_seed_validation_is_explicit(fake_backend, checkpoint, seed):
    with pytest.raises(ValueError, match="random_state must be an integer"):
        TabPFNModel({"model_path": checkpoint, "random_state": seed}).fit(loader())
    assert not fake_backend["init"]


def test_numpy_integer_seed_remains_native_json_serializable(fake_backend, checkpoint, tmp_path):
    model = TabPFNModel({"model_path": checkpoint, "random_state": np.int64(17)}).fit(loader())
    assert type(model.model.params["random_state"]) is int
    model.save(tmp_path / "seed.joblib")
    restored = TabPFNModel.load(tmp_path / "seed.joblib")
    torch.testing.assert_close(restored.predict(loader()), model.predict(loader()))


@pytest.mark.parametrize("estimators", [0, -1, True, 1.5])
def test_ensemble_size_validation(fake_backend, checkpoint, estimators):
    with pytest.raises(ValueError, match="n_estimators must be a positive integer"):
        TabPFNModel({"model_path": checkpoint, "model_kwargs": {"n_estimators": estimators}}).fit(loader())
    assert not fake_backend["init"]


@pytest.mark.parametrize("helpers", [SimpleNamespace(), SimpleNamespace(
    save_fitted_tabpfn_model=None, load_fitted_tabpfn_model=None,
)])
def test_helper_capability_is_checked_before_loading_weights(fake_backend, checkpoint, monkeypatch, helpers):
    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", helpers)
    with pytest.raises(ImportError, match="native fitted persistence helpers"):
        TabPFNModel({"model_path": checkpoint}).fit(loader())
    assert not fake_backend["init"]
    assert not fake_backend["fit"]


def test_no_checkpoint_or_gated_auto_download_is_rejected(fake_backend):
    with pytest.raises(ValueError, match="download is disabled"):
        TabPFNModel({}).fit(loader())
    with pytest.raises(ValueError, match="restricted to ungated public v2"):
        TabPFNModel({"allow_model_download": True, "model_version": "v3.5"}).fit(loader())
    assert not fake_backend["init"]


def test_opt_in_download_selects_v2_not_upstream_gated_default(fake_backend, tmp_path):
    model = TabPFNModel({"allow_model_download": True}).fit(loader())
    assert fake_backend["version"] == ["v2"]
    path = tmp_path / "fitted.joblib"
    model.save(path)
    restored = TabPFNModel.load(path)
    restored.predict(loader())
    assert fake_backend["load"][0]["params"]["model_path"].endswith("tabpfn-v2-regressor.ckpt")


def test_subsampling_override_is_rejected_and_default_is_explicit(fake_backend, checkpoint):
    with pytest.raises(ValueError, match="row subsampling is disabled"):
        TabPFNModel({"model_path": checkpoint, "model_kwargs": {
            "inference_config": {"SUBSAMPLE_SAMPLES": 5},
        }}).fit(loader())
    model = TabPFNModel({"model_path": checkpoint}).fit(loader())
    assert model.model.params["inference_config"]["SUBSAMPLE_SAMPLES"] is None
    assert fake_backend["fit"][0][0].shape == (24, 3)


@pytest.mark.parametrize("suffix", [".ckpt", ".safetensors"])
def test_local_checkpoint_formats_and_model_kwargs_precedence(fake_backend, tmp_path, suffix):
    path = tmp_path / f"local{suffix}"
    path.write_bytes(b"fake weights")
    model = TabPFNModel({"model_path": "ignored-missing.ckpt", "model_kwargs": {
        "model_path": path, "device": "cpu", "random_state": 17, "n_estimators": 1,
    }}).fit(loader())
    assert model.model.params["model_path"] == str(path.resolve())
    assert model.model.params["random_state"] == 17
    assert model.predict(loader(targets=False)).shape == (24, 1)


def test_live_model_checks_checkpoint_and_applies_inference_device(fake_backend, checkpoint, monkeypatch):
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 1)
    model = TabPFNModel({"model_path": checkpoint, "device": "cuda:0"}).fit(loader())
    model.predict(loader())
    assert fake_backend["to"] == ["cpu"]
    model.predict(loader(), inference_device="cuda:0")
    assert fake_backend["to"] == ["cpu", "cuda:0"]
    checkpoint.unlink()
    with pytest.raises(ValueError, match="existing local checkpoint"):
        model.predict(loader())


def test_native_archive_relocation_lazy_load_and_resave(fake_backend, checkpoint, tmp_path, monkeypatch):
    model = TabPFNModel({"model_path": str(checkpoint)}).fit(loader())
    expected = model.predict(loader())
    artifact = tmp_path / "bundle.joblib"
    joblib.dump({"model": model, "parent_preprocessor": "untouched"}, artifact)
    checkpoint.rename(tmp_path / "relocated.ckpt")
    restored = joblib.load(artifact)["model"]
    assert restored.model is None
    assert not fake_backend["load"]
    restored.config["model_path"] = str(tmp_path / "relocated.ckpt")
    with monkeypatch.context() as unavailable:
        unavailable.setitem(sys.modules, "tabpfn", None)
        unavailable.setitem(sys.modules, "tabpfn.model_loading", None)
        restored.save(tmp_path / "resaved.joblib")
    assert len(fake_backend["save"]) == 1
    torch.testing.assert_close(restored.predict(loader()), expected)
    assert fake_backend["load"][0]["params"]["model_path"] == restored.config["model_path"]


@pytest.mark.parametrize("package", ["tabpfn", "scikit-learn", "torch", "numpy"])
def test_native_archive_version_guard_precedes_backend_load(fake_backend, checkpoint, tmp_path, package):
    model = TabPFNModel({"model_path": checkpoint}).fit(loader())
    model.save(tmp_path / "model.joblib")
    restored = TabPFNModel.load(tmp_path / "model.joblib")
    restored._fitted_versions[package] = "incompatible"
    with pytest.raises(RuntimeError, match="backend versions differ"):
        restored.predict(loader())
    assert not fake_backend["load"]
    assert restored._fitted_payload is not None


def test_torch_wheel_build_tag_does_not_prevent_cpu_portability(fake_backend, checkpoint, tmp_path):
    model = TabPFNModel({"model_path": checkpoint}).fit(loader())
    model.save(tmp_path / "model.joblib")
    restored = TabPFNModel.load(tmp_path / "model.joblib")
    restored._fitted_versions["torch"] = "2.6.0+cu124"
    torch.testing.assert_close(restored.predict(loader()), model.predict(loader()))
    assert fake_backend["load"][0]["device"] == "cpu"


def test_native_archive_missing_metadata_or_bad_archive_is_not_guessed(fake_backend, checkpoint, tmp_path):
    model = TabPFNModel({"model_path": checkpoint}).fit(loader())
    model.save(tmp_path / "model.joblib")
    restored = TabPFNModel.load(tmp_path / "model.joblib")
    versions = restored._fitted_versions
    restored._fitted_versions = None
    with pytest.raises(RuntimeError, match="versions differ or are missing"):
        restored.predict(loader())
    restored._fitted_versions = versions
    restored._fitted_payload = b"not a native ZIP archive"
    with pytest.raises(RuntimeError, match="Invalid TabPFN native fitted archive"):
        restored.predict(loader())
    assert not fake_backend["load"]


@pytest.mark.parametrize("prediction", [np.array([np.nan] * 24), np.zeros((24, 2)), np.zeros(23)])
def test_bad_backend_predictions_are_rejected(fake_backend, checkpoint, prediction):
    model = TabPFNModel({"model_path": checkpoint}).fit(loader())
    model.model.predict = lambda x: prediction
    with pytest.raises(RuntimeError, match="one finite regression prediction"):
        model.predict(loader())


def test_real_package_api_and_native_helper_signatures():
    pytest.importorskip("tabpfn", reason="Optional TabPFN package not installed; no package or weight download attempted.")
    from inspect import signature
    from tabpfn import TabPFNRegressor

    save, load = TabPFNModel._native_helpers()
    parameters = signature(TabPFNRegressor).parameters
    assert {"model_path", "device", "random_state", "n_estimators", "inference_config"} <= parameters.keys()
    assert {"estimator", "path"} <= signature(save).parameters.keys()
    assert {"path", "device"} <= signature(load).parameters.keys()
    public_v2 = TabPFNRegressor.create_default_for_version("v2", device="cpu", n_estimators=1)
    assert Path(public_v2.model_path).name == "tabpfn-v2-regressor.ckpt"


@pytest.fixture
def real_checkpoint():
    # Search approved locations only, not ~/.cache/huggingface or auth storage.
    workspace = Path(__file__).resolve().parents[2]
    roots = [workspace, Path("/tmp/kilo")]
    paths = sorted({path for root in roots for path in root.rglob("tabpfn*v2*regressor*.ckpt")
                    if "v2.5" not in path.name and "v2.6" not in path.name})
    if not paths:
        pytest.skip("No existing public TabPFN-v2 regression checkpoint in workspace or /tmp/kilo; "
                    "real CPU roundtrip requires local weights, and tests never download/accept gated licenses.")
    return paths[0]


def test_real_cpu_fit_predict_and_native_helpers_roundtrip(real_checkpoint, tmp_path):
    pytest.importorskip("tabpfn", reason="Optional TabPFN package not installed; real CPU inference unavailable.")
    previous_threads = torch.get_num_threads()
    torch.set_num_threads(2)
    try:
        config = {"model_path": str(real_checkpoint), "device": "cpu", "random_state": 42,
                  "allow_model_download": False, "model_kwargs": {
                      "n_estimators": 1, "n_preprocessing_jobs": 1,
                  }}
        model = TabPFNModel(config).fit(loader())
        expected = model.predict(loader(targets=False))
        assert expected.shape == (24, 1)
        assert torch.isfinite(expected).all()
        assert model.model.inference_config_.SUBSAMPLE_SAMPLES is None
        # Synthetic labels are deliberately small, as scaled regression targets are valid.
        assert float(expected.abs().max()) < 1
        save, load = TabPFNModel._native_helpers()
        native_path = tmp_path / "real.tabpfn_fit"
        save(model.model, native_path)
        native = load(native_path, device="cpu")
        x, _ = model._loader_arrays(loader())
        torch.testing.assert_close(torch.as_tensor(native.predict(x), dtype=torch.float32).reshape(-1, 1), expected)

        artifact = tmp_path / "real.joblib"
        model.save(artifact)
        restored = TabPFNModel.load(artifact)
        assert restored.model is None
        assert restored._fitted_versions["tabpfn"] == model._fitted_versions["tabpfn"]
        with ZipFile(BytesIO(restored._fitted_payload)) as archive:
            assert "init_params.json" in archive.namelist()
        assert len(restored._fitted_payload) < real_checkpoint.stat().st_size / 10
        torch.testing.assert_close(restored.predict(loader(targets=False)), expected)
        assert str(restored.model.devices_[0]) == "cpu"
    finally:
        torch.set_num_threads(previous_threads)


def test_real_local_safetensors_fit_and_native_roundtrip(real_checkpoint, tmp_path):
    pytest.importorskip("tabpfn", reason="Optional TabPFN package not installed; real safetensors test unavailable.")
    from tabpfn.checkpoint import Checkpoint, save_as_safetensors

    # Convert existing public v2 weights locally, without downloading gated
    # safetensors models. The upstream format helper preserves checkpoint metadata.
    converted = tmp_path / "tabpfn-v2-regressor.safetensors"
    save_as_safetensors(Checkpoint(real_checkpoint).load(), converted)
    previous_threads = torch.get_num_threads()
    torch.set_num_threads(2)
    try:
        model = TabPFNModel({"model_path": str(converted), "model_kwargs": {"n_estimators": 1}}).fit(loader())
        expected = model.predict(loader())
        assert torch.isfinite(expected).all()
        artifact = tmp_path / "safetensors.joblib"
        model.save(artifact)
        restored = TabPFNModel.load(artifact)
        torch.testing.assert_close(restored.predict(loader()), expected)
        assert restored.model.model_path == str(converted)
    finally:
        torch.set_num_threads(previous_threads)


def test_real_checkpoint_relocation_overrides_archived_model_path(real_checkpoint, tmp_path, monkeypatch):
    pytest.importorskip("tabpfn", reason="Optional TabPFN package not installed; real checkpoint relocation unavailable.")
    import shutil

    original = tmp_path / "original" / real_checkpoint.name
    relocated = tmp_path / "relocated" / real_checkpoint.name
    original.parent.mkdir()
    relocated.parent.mkdir()
    shutil.copyfile(real_checkpoint, original)
    previous_threads = torch.get_num_threads()
    torch.set_num_threads(2)
    try:
        model = TabPFNModel({"model_path": str(original), "model_kwargs": {"n_estimators": 1}}).fit(loader())
        expected = model.predict(loader())
        artifact = tmp_path / "relocation.joblib"
        model.save(artifact)
        original.rename(relocated)
        restored = TabPFNModel.load(artifact)
        with pytest.raises(ValueError, match="existing local checkpoint"):
            restored.predict(loader())
        restored.config["model_path"] = str(relocated)
        from tabpfn import model_loading

        def forbidden_download(*args, **kwargs):
            raise AssertionError("Relocated native archive must use local current path, not download old path.")

        monkeypatch.setattr(model_loading, "download_model", forbidden_download)
        torch.testing.assert_close(restored.predict(loader()), expected)
        assert restored.model.model_path == str(relocated)
    finally:
        torch.set_num_threads(previous_threads)
