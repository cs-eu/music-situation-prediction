import os
import sys
from pathlib import Path
from types import SimpleNamespace

import joblib
import numpy as np
import pytest
import torch
from torch.utils.data import DataLoader, TensorDataset

from models.lightgbm_model import LightGBMModel


def loader(seed=42, rows=96):
    generator = torch.Generator().manual_seed(seed)
    x = torch.randn(rows, 4, generator=generator)
    y = 2 * x[:, :1] - x[:, 1:2] + 0.25
    return DataLoader(TensorDataset(x, y), batch_size=17)


@pytest.fixture
def fake_lightgbm(monkeypatch):
    calls = []

    class LightGBMError(Exception):
        pass

    class Dataset:
        def __init__(self, X, label, reference=None):
            self.X, self.label, self.reference = X, label, reference

    class Booster:
        best_iteration = 2

        def predict(self, X, num_iteration=None):
            assert num_iteration == self.best_iteration
            return np.zeros(len(X))

    def train(params, train_data, **kwargs):
        calls.append({"params": dict(params), "train_data": train_data, **kwargs})
        if backend.failure is not None:
            raise backend.failure
        return Booster()

    backend = SimpleNamespace(
        Dataset=Dataset, train=train, basic=SimpleNamespace(LightGBMError=LightGBMError),
        log_evaluation=lambda period: ("log", period),
        early_stopping=lambda patience, verbose: ("early_stop", patience),
        calls=calls, failure=None,
    )
    monkeypatch.setitem(sys.modules, "lightgbm", backend)
    return backend


@pytest.mark.parametrize("config,expected", [
    ({}, {"device_type": "cpu"}),
    ({"device_type": "cpu", "gpu_device_id": None, "gpu_platform_id": None}, {"device_type": "cpu"}),
    ({"device_type": "gpu"}, {"device_type": "gpu"}),
    ({"device_type": "gpu", "gpu_device_id": 1, "gpu_platform_id": 2},
     {"device_type": "gpu", "gpu_device_id": 1, "gpu_platform_id": 2}),
    ({"device_type": "gpu", "gpu_device_id": -1, "gpu_platform_id": -1},
     {"device_type": "gpu", "gpu_device_id": -1, "gpu_platform_id": -1}),
    ({"device_type": "cuda", "gpu_device_id": 0}, {"device_type": "cuda", "gpu_device_id": 0}),
    ({"device_type": "cuda", "gpu_device_id": -1, "gpu_platform_id": None},
     {"device_type": "cuda", "gpu_device_id": -1}),
])
def test_backend_options_reach_train(config, expected, fake_lightgbm, monkeypatch):
    # LightGBM backend availability is independent of PyTorch's CUDA build.
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    model = LightGBMModel(config).fit(loader(), val_dataloader=loader(seed=43, rows=32))
    assert len(fake_lightgbm.calls) == 1
    call = fake_lightgbm.calls[0]
    params = call["params"]
    assert {key: params[key] for key in expected} == expected
    assert {key for key in params if key.startswith("gpu_")} == {key for key in expected if key.startswith("gpu_")}
    if expected["device_type"] == "cpu":
        assert params["deterministic"] is True
        assert params["force_col_wise"] is True
    else:
        assert "deterministic" not in params
        assert "force_col_wise" not in params
    assert call["valid_sets"][0].reference is call["train_data"]
    assert ("early_stop", 30) in call["callbacks"]
    assert model.predict(loader()).shape == (96, 1)


@pytest.mark.parametrize("device_type", ["auto", "CUDA", "cuda:0", "mps", "", None, 0, [], {}])
def test_invalid_device_type_is_rejected(device_type):
    with pytest.raises(ValueError, match="device_type must be"):
        LightGBMModel({"device_type": device_type})


@pytest.mark.parametrize("option", ["gpu_device_id", "gpu_platform_id"])
@pytest.mark.parametrize("value", [-2, 0.5, "0", True, False, [], {}])
def test_gpu_ids_require_integer_or_default(option, value):
    with pytest.raises(ValueError, match=f"{option} must be an integer"):
        LightGBMModel({"device_type": "gpu", option: value})


@pytest.mark.parametrize("config,option", [
    ({"gpu_device_id": 0}, "gpu_device_id"),
    ({"gpu_platform_id": -1}, "gpu_platform_id"),
    ({"device_type": "cuda", "gpu_platform_id": 0}, "gpu_platform_id"),
])
def test_inapplicable_gpu_ids_are_rejected(config, option):
    with pytest.raises(ValueError, match=f"{option} requires"):
        LightGBMModel(config)


@pytest.mark.parametrize("device_type,message,build_option", [
    ("gpu", "GPU Tree Learner was not enabled in this build", "USE_GPU=ON"),
    ("gpu", "No OpenCL device found", "USE_GPU=ON"),
    ("gpu", "clGetPlatformIDs failed: PLATFORM_NOT_FOUND_KHR", "USE_GPU=ON"),
    ("cuda", "CUDA Tree Learner was not enabled in this build", "USE_CUDA=ON"),
    ("cuda", "CUDA error: no CUDA-capable device is detected", "USE_CUDA=ON"),
])
def test_gpu_backend_failure_is_descriptive_without_cpu_fallback(device_type, message, build_option, fake_lightgbm):
    backend_error = fake_lightgbm.basic.LightGBMError(message)
    fake_lightgbm.failure = backend_error
    model = LightGBMModel({"device_type": device_type, "gpu_device_id": 0})
    with pytest.raises(RuntimeError, match="No CPU fallback was attempted") as failure:
        model.fit(loader())
    assert failure.value.__cause__ is backend_error
    assert message in str(failure.value)
    assert build_option in str(failure.value)
    assert len(fake_lightgbm.calls) == 1
    assert fake_lightgbm.calls[0]["params"]["device_type"] == device_type
    assert model.model is None


def test_cpu_backend_errors_are_preserved(fake_lightgbm):
    backend_error = fake_lightgbm.basic.LightGBMError("Invalid training parameter")
    fake_lightgbm.failure = backend_error
    with pytest.raises(fake_lightgbm.basic.LightGBMError) as failure:
        LightGBMModel({}).fit(loader())
    assert failure.value is backend_error
    assert len(fake_lightgbm.calls) == 1


@pytest.mark.parametrize("device_type", ["cpu", "gpu", "cuda"])
def test_lightgbm_dependency_remains_optional(device_type, monkeypatch):
    monkeypatch.setitem(sys.modules, "lightgbm", None)
    with pytest.raises(ImportError, match="optional 'lightgbm'"):
        LightGBMModel({"device_type": device_type}).fit(loader())


@pytest.mark.parametrize("overrides,expected", [
    ([], {"device_type": "cpu"}),
    (["model.device_type=cuda", "model.gpu_device_id=0"], {"device_type": "cuda", "gpu_device_id": 0}),
    (["model.device_type=gpu", "model.gpu_device_id=0", "model.gpu_platform_id=1"],
     {"device_type": "gpu", "gpu_device_id": 0, "gpu_platform_id": 1}),
])
def test_hydra_backend_overrides_do_not_require_plus_prefix(overrides, expected):
    from hydra import compose, initialize_config_dir
    from omegaconf import OmegaConf

    directory = Path(__file__).resolve().parents[1] / "conf"
    with initialize_config_dir(config_dir=str(directory), version_base=None):
        config = compose(config_name="config", overrides=["model=lightgbm", *overrides])
    model = LightGBMModel(OmegaConf.to_container(config.model, resolve=True))
    assert {key: model.params[key] for key in expected} == expected


def fit_config(**overrides):
    return {"num_rounds": 8, "num_leaves": 7, "min_data_in_leaf": 2,
            "feature_fraction": 1.0, "bagging_fraction": 1.0, "bagging_freq": 0,
            "early_stopping_rounds": 3, "n_jobs": 1, **overrides}


def test_real_lightgbm_cpu_fit_predict_and_load(tmp_path):
    lgb = pytest.importorskip("lightgbm", reason="Real CPU smoke test needs the optional LightGBM package.")
    model = LightGBMModel(fit_config()).fit(loader(), val_dataloader=loader(seed=43, rows=32))
    assert isinstance(model.model, lgb.Booster)
    assert model.model.params["device_type"] == "cpu"
    expected = model.predict(loader(seed=44, rows=16))
    assert expected.shape == (16, 1)
    assert torch.isfinite(expected).all()
    assert expected.std() > 0
    assert 1 <= model.model.best_iteration <= 8

    path = tmp_path / "lightgbm.joblib"
    model.save(path)
    restored = LightGBMModel.load(path)
    assert restored.params["device_type"] == "cpu"
    torch.testing.assert_close(restored.predict(loader(seed=44, rows=16)), expected)

    # BaseModel also supports an existing estimator-only LightGBM artifact.
    estimator_path = tmp_path / "booster.joblib"
    joblib.dump(model.model, estimator_path)
    restored_estimator = LightGBMModel.load(estimator_path, fit_config())
    torch.testing.assert_close(restored_estimator.predict(loader(seed=44, rows=16)), expected)


def test_real_gpu_fit_opt_in():
    device_type = os.environ.get("LIGHTGBM_TEST_DEVICE")
    if device_type is None:
        pytest.skip("No GPU execution requested on this host; set LIGHTGBM_TEST_DEVICE=gpu or cuda on a GPU-enabled machine with the matching LightGBM build.")
    if device_type not in ("gpu", "cuda"):
        pytest.fail("LIGHTGBM_TEST_DEVICE must be gpu (OpenCL) or cuda.")
    pytest.importorskip("lightgbm", reason="GPU smoke test needs a matching GPU-enabled LightGBM build.")
    model = LightGBMModel(fit_config(device_type=device_type, gpu_device_id=0)).fit(loader())
    assert model.model.params["device_type"] == device_type
    predictions = model.predict(loader(seed=44, rows=16))
    assert predictions.shape == (16, 1)
    assert torch.isfinite(predictions).all()
