import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import joblib
import numpy as np
import pytest
import torch
from torch.utils.data import DataLoader, SubsetRandomSampler, TensorDataset

from models import BaselineModel, ElasticNetModel, RandomForestModel
from models.base_model import BaseModel


def loader(x=None, y=None, batch_size=7, shuffle=False):
    if x is None:
        x = torch.arange(60, dtype=torch.float32).reshape(20, 3) / 10
    if y is None:
        y = 2 * x[:, :1] - x[:, 1:2] + 0.25
    return DataLoader(TensorDataset(x, y), batch_size=batch_size, shuffle=shuffle,
                      generator=torch.Generator().manual_seed(42))


class EchoModel(BaseModel):
    def fit(self, dataloader, **kwargs):
        return self

    def predict(self, dataloader, **kwargs):
        return torch.cat([x[:, 0] for x, _ in dataloader])


def test_evaluate_uses_one_pass_and_normalizes_scalar_shapes():
    class CountingLoader:
        passes = 0

        def __iter__(self):
            self.passes += 1
            x = torch.arange(13, dtype=torch.float32).reshape(-1, 1)
            return iter(loader(x, x, shuffle=True))

    batches = CountingLoader()
    metrics = EchoModel({}).evaluate(batches)
    assert batches.passes == 1
    assert metrics["RMSE"] == 0
    assert metrics["R2"] == 1


@pytest.mark.parametrize("kind,config", [
    (BaselineModel, {}),
    (ElasticNetModel, {"alpha": 0.001}),
    (ElasticNetModel, {"alpha": 0.001, "standardize": False}),
    (RandomForestModel, {"n_estimators": 5, "min_samples_leaf": 1, "n_jobs": 1}),
])
def test_classical_wrapper_roundtrip_and_shape(kind, config, tmp_path):
    model = kind(config).fit(loader())
    expected = model.predict(loader())
    assert expected.shape == (20, 1)
    path = tmp_path / "model.joblib"
    model.save(path)
    restored = kind.load(path, config)
    torch.testing.assert_close(restored.predict(loader()), expected)
    assert np.isfinite(list(restored.evaluate(loader()).values())).all()


def test_elastic_net_pipeline_preserves_scaler_in_estimator_artifact(tmp_path):
    x = torch.arange(60, dtype=torch.float32).reshape(20, 3)
    x[:, 0] *= 1000
    model = ElasticNetModel({"alpha": 0.001}).fit(loader(x))
    path = tmp_path / "estimator.joblib"
    joblib.dump(model.model, path)
    restored = ElasticNetModel.load(path, {"alpha": 0.001})
    assert "scaler" in restored.model.named_steps
    torch.testing.assert_close(restored.predict(loader(x)), model.predict(loader(x)))


def test_baseline_counts_actual_sampler_rows_and_checks_fitted_state():
    model = BaselineModel()
    with pytest.raises(RuntimeError, match="Fit the baseline"):
        model.predict(loader())
    model.fit(loader())
    full = loader()
    subset = DataLoader(full.dataset, batch_size=2, sampler=SubsetRandomSampler([0, 2, 4, 6, 8]),
                        drop_last=True)
    assert model.predict(subset).shape == (4, 1)


def test_tabular_models_reject_multioutput_nonfinite_and_empty_data():
    model = ElasticNetModel({})
    with pytest.raises(ValueError, match="one target"):
        model.fit(loader(torch.ones(3, 2), torch.ones(3, 2)))
    with pytest.raises(ValueError, match="finite"):
        model.fit(loader(torch.ones(3, 2), torch.tensor([1.0, float("nan"), 2.0])))
    with pytest.raises(ValueError, match="at least one row"):
        model.fit(loader(torch.empty(0, 2), torch.empty(0, 1)))
    with pytest.raises(ValueError, match="shaped"):
        model.fit(loader(torch.ones(3, 2, 2), torch.ones(3, 1)))


def test_classical_imports_without_optional_backends():
    script = """
import importlib.abc
import sys
class BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'lightning', 'pytorch_lightning', 'torch_geometric', 'tabpfn', 'lightgbm', 'wandb', 'shap'}:
            raise ImportError('optional dependency deliberately unavailable: ' + fullname)
sys.meta_path.insert(0, BlockOptional())
from models import BaselineModel, ElasticNetModel, RandomForestModel
from models.base_model import BaseModel
BaselineModel({})
ElasticNetModel({})
RandomForestModel({})
assert not any(name in sys.modules for name in ['lightning', 'torch_geometric', 'tabpfn', 'lightgbm'])
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                            cwd=Path(__file__).resolve().parents[1] / "src")
    assert result.returncode == 0, result.stderr


def neural_class(name):
    pytest.importorskip("lightning")
    import models

    return getattr(models, name)


@pytest.mark.parametrize("name", ["NeuralNet", "TransformerModel"])
def test_neural_predictions_are_independent_of_batch_and_checkpoint_roundtrip(name, tmp_path):
    kind = neural_class(name)
    torch.manual_seed(42)
    config = {"input_dim": 3, "output_dim": 1, "hidden_dims": [8, 4],
              "d_model": 8, "nhead": 2, "num_layers": 1, "dim_feedforward": 16}
    model = kind(config).eval()
    x = torch.randn(9, 3)
    with torch.inference_mode():
        full = model(x)
        singles = torch.cat([model(row.unsqueeze(0)) for row in x])
        permuted = model(x.flip(0)).flip(0)
    torch.testing.assert_close(full, singles, atol=1e-6, rtol=1e-5)
    torch.testing.assert_close(full, permuted, atol=1e-6, rtol=1e-5)
    assert full.shape == (9, 1)
    path = tmp_path / "model.pt"
    model.save(path)
    restored = kind.load(path, config)
    assert not restored.training
    torch.testing.assert_close(restored(x), full)
    with pytest.raises(ValueError, match="tabular"):
        model(x.unsqueeze(1))
    if name == "TransformerModel":
        assert model.transformer_encoder.layers[0].self_attn.batch_first
        assert model.feature_embedding.shape == (3, 8)
        assert model.value_embedding.in_features == 1


def test_neural_mse_rmse_weighting_and_val_loss_scheduler():
    kind = neural_class("NeuralNet")
    import lightning as L

    model = kind({"input_dim": 2, "output_dim": 1, "hidden_dims": [4]})
    with torch.no_grad():
        for parameter in model.parameters():
            parameter.zero_()
    trainer = L.Trainer(logger=False, enable_checkpointing=False, enable_progress_bar=False,
                        accelerator="cpu", devices=1, enable_model_summary=False)
    # Unequal batches distinguish global RMSE from averaging per-batch RMSEs.
    results = trainer.validate(model, loader(torch.zeros(3, 2), torch.tensor([1.0, 1.0, 3.0]),
                                            batch_size=2), verbose=False)[0]
    assert results["val_loss"] == pytest.approx(11 / 3)
    assert results["val_mse"] == pytest.approx(11 / 3)
    assert results["val_rmse"] == pytest.approx(np.sqrt(11 / 3))
    optimizer = model.configure_optimizers()
    assert isinstance(optimizer["optimizer"], torch.optim.AdamW)
    assert optimizer["lr_scheduler"]["monitor"] == "val_loss"


@pytest.mark.parametrize("name", ["NeuralNet", "TransformerModel"])
def test_neural_fit_runs_with_validation_scheduler(name):
    kind = neural_class(name)
    import lightning as L

    model = kind({"input_dim": 3, "output_dim": 1, "hidden_dims": [8, 4], "dropout": 0.1,
                  "d_model": 8, "nhead": 2, "num_layers": 1, "dim_feedforward": 16})
    trainer = L.Trainer(max_epochs=2, logger=False, enable_checkpointing=False,
                        enable_progress_bar=False, enable_model_summary=False, num_sanity_val_steps=0,
                        accelerator="cpu", devices=1)
    trainer.fit(model, train_dataloaders=loader(shuffle=True), val_dataloaders=loader())
    assert torch.isfinite(trainer.callback_metrics["train_loss"])
    assert torch.isfinite(trainer.callback_metrics["val_loss"])
    assert trainer.callback_metrics["train_rmse"].square() == pytest.approx(
        float(trainer.callback_metrics["train_mse"]), rel=1e-6)
    assert model.predict(loader()).shape == (20, 1)


def test_gnn_fails_instead_of_inventing_tabular_graphs():
    kind = neural_class("GNNModel")
    with pytest.raises(ValueError, match="does not construct graphs"):
        kind({"input_dim": 3, "output_dim": 1})


def test_genuine_gnn_graph_pooling():
    pytest.importorskip("torch_geometric")
    kind = neural_class("GNNModel")
    from torch_geometric.data import Batch, Data

    model = kind({"graph_input": True, "input_dim": 3, "output_dim": 1, "dropout": 0}).eval()
    graphs = [Data(x=torch.randn(n, 3), edge_index=torch.empty((2, 0), dtype=torch.long)) for n in (3, 2)]
    batched = model(Batch.from_data_list(graphs))
    individual = torch.cat([model(graph) for graph in graphs])
    assert batched.shape == (2, 1)
    torch.testing.assert_close(batched, individual)
    for graph, prediction in zip(graphs, individual):
        graph.y = prediction.detach().reshape(1, 1)
    assert model.evaluate(graphs)["RMSE"] == pytest.approx(0, abs=1e-7)
    with pytest.raises(ValueError, match="explicit PyG"):
        model(torch.randn(2, 3))
    with pytest.raises(ValueError, match="not tabular"):
        model.predict(loader())


def test_tabpfn_resource_download_and_dependency_guards(monkeypatch):
    from models import TabPFNModel

    with pytest.raises(ValueError, match="resource limits"):
        TabPFNModel({"max_cpu_rows": 3}).fit(loader())
    with pytest.raises(ValueError, match="download is disabled"):
        TabPFNModel({}).fit(loader())
    with pytest.raises(ValueError, match="existing local checkpoint"):
        TabPFNModel({"model_path": "/definitely/missing/tabpfn.ckpt"}).fit(loader())
    monkeypatch.setitem(sys.modules, "tabpfn", None)
    with pytest.raises(ImportError, match="optional 'tabpfn'"):
        TabPFNModel({"allow_model_download": True}).fit(loader())
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    with pytest.raises(ValueError, match="no GPU"):
        TabPFNModel({"device": "cuda"}).fit(loader())


def test_tabpfn_opt_in_small_fit_and_descriptive_failure(monkeypatch):
    from models import TabPFNModel

    calls = {}

    class FakeRegressor:
        def __init__(self, **kwargs):
            calls.update(kwargs)

        def fit(self, X, y):
            self.mean = y.mean()

        def predict(self, X):
            return np.full(len(X), self.mean)

    monkeypatch.setitem(sys.modules, "tabpfn", SimpleNamespace(TabPFNRegressor=FakeRegressor))
    model = TabPFNModel({"allow_model_download": True}).fit(loader())
    assert model.predict(loader()).shape == (20, 1)
    assert calls == {"device": "cpu", "random_state": 42, "n_estimators": 4}

    def failed_download(**kwargs):
        raise OSError("weight authentication failed")

    monkeypatch.setitem(sys.modules, "tabpfn", SimpleNamespace(TabPFNRegressor=failed_download))
    with pytest.raises(RuntimeError, match="license/authentication"):
        TabPFNModel({"allow_model_download": True}).fit(loader())


@pytest.fixture
def native_tabpfn(monkeypatch):
    calls = {"save": [], "load": [], "init": []}

    class FakeRegressor:
        def __init__(self, **kwargs):
            calls["init"].append(kwargs)
            self.params = kwargs
            self.foundation_weights = b"FOUNDATION_WEIGHTS_MUST_STAY_EXTERNAL" * 1000

        def fit(self, X, y):
            self.mean = float(y.mean())
            self.rows = len(X)

        def predict(self, X):
            return np.full(len(X), self.mean)

        def __reduce_ex__(self, protocol):
            raise AssertionError("The fitted estimator and its foundation weights must not be pickled.")

    def save_fitted(estimator, path):
        assert Path(path).suffix == ".tabpfn_fit"
        calls["save"].append(str(path))
        Path(path).write_bytes(json.dumps({
            "fitted_context": {"mean": estimator.mean, "rows": estimator.rows},
            "model_path": estimator.params.get("model_path"),
        }).encode("ascii"))

    def load_fitted(path, *, device="auto"):
        assert Path(path).suffix == ".tabpfn_fit"
        payload = json.loads(Path(path).read_bytes())
        calls["load"].append({"device": device, "payload": payload})
        estimator = FakeRegressor(device=device, model_path=payload["model_path"])
        estimator.mean = payload["fitted_context"]["mean"]
        estimator.rows = payload["fitted_context"]["rows"]
        return estimator

    monkeypatch.setitem(sys.modules, "tabpfn", SimpleNamespace(TabPFNRegressor=FakeRegressor))
    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", SimpleNamespace(
        save_fitted_tabpfn_model=save_fitted, load_fitted_tabpfn_model=load_fitted,
    ))
    return calls


def test_tabpfn_native_joblib_roundtrip_is_lazy_and_cpu_portable(native_tabpfn, monkeypatch, tmp_path):
    from models import TabPFNModel

    checkpoint = tmp_path / "weights.ckpt"
    checkpoint.write_bytes(b"external foundation weights")
    config = {"model_path": str(checkpoint), "device": "cuda", "allow_model_download": False}
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    model = TabPFNModel(config).fit(loader())
    expected = model.predict(loader())
    path = tmp_path / "predictor.joblib"
    joblib.dump({"model": model, "preprocessing": "parent-owned"}, path)
    assert len(native_tabpfn["save"]) == 1
    assert not native_tabpfn["load"]
    assert model.model is not None
    assert b"FOUNDATION_WEIGHTS_MUST_STAY_EXTERNAL" not in path.read_bytes()

    # Loading and re-saving the full parent bundle need neither TabPFN nor weights.
    with monkeypatch.context() as unavailable:
        unavailable.setitem(sys.modules, "tabpfn", None)
        unavailable.setitem(sys.modules, "tabpfn.model_loading", None)
        unavailable.setattr(torch.cuda, "is_available", lambda: False)
        restored_bundle = joblib.load(path)
        restored = restored_bundle["model"]
        assert restored.config == config
        assert restored.model is None
        assert json.loads(restored._fitted_payload)["fitted_context"]["rows"] == 20
        assert not native_tabpfn["load"]
        resaved_path = tmp_path / "resaved.joblib"
        joblib.dump(restored_bundle, resaved_path)
        resaved = joblib.load(resaved_path)["model"]
        assert resaved.model is None
        assert resaved._fitted_payload == restored._fitted_payload
        assert len(native_tabpfn["save"]) == 1

    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    torch.testing.assert_close(restored.predict(loader()), expected)
    assert native_tabpfn["load"][0]["device"] == "cpu"
    assert restored.model.rows == 20
    restored.predict(loader())
    assert len(native_tabpfn["load"]) == 1


@pytest.mark.parametrize("override", ["config", "predict"])
def test_tabpfn_native_reload_respects_explicit_inference_device(native_tabpfn, monkeypatch, tmp_path, override):
    from models import TabPFNModel

    config = {"allow_model_download": True}
    if override == "config":
        config["inference_device"] = "cuda:0"
    model = TabPFNModel(config).fit(loader())
    path = tmp_path / "predictor.joblib"
    model.save(path)
    restored = TabPFNModel.load(path)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    restored.predict(loader(), **({"inference_device": "cuda:0"} if override == "predict" else {}))
    assert native_tabpfn["load"][0]["device"] == "cuda:0"


def test_tabpfn_native_reload_checks_external_checkpoint_before_backend_import(native_tabpfn, monkeypatch, tmp_path):
    from models import TabPFNModel

    checkpoint = tmp_path / "weights.ckpt"
    checkpoint.write_bytes(b"external foundation weights")
    model = TabPFNModel({"model_path": str(checkpoint)}).fit(loader())
    path = tmp_path / "predictor.joblib"
    model.save(path)
    checkpoint.unlink()
    monkeypatch.setitem(sys.modules, "tabpfn", None)
    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", None)
    restored = TabPFNModel.load(path)
    assert restored.model is None
    with pytest.raises(ValueError, match="existing local checkpoint"):
        restored.predict(loader())
    assert not native_tabpfn["load"]


def test_tabpfn_native_persistence_requires_helpers_without_pickle_fallback(native_tabpfn, monkeypatch, tmp_path):
    from models import TabPFNModel

    model = TabPFNModel({"allow_model_download": True}).fit(loader())
    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", SimpleNamespace())
    with pytest.raises(ImportError, match="native fitted persistence helpers"):
        model.save(tmp_path / "missing-helpers.joblib")
    assert model.model is not None
    assert not native_tabpfn["save"]


def test_tabpfn_native_reload_errors_are_actionable_and_keep_payload(native_tabpfn, monkeypatch, tmp_path):
    from models import TabPFNModel

    model = TabPFNModel({"allow_model_download": True}).fit(loader())
    path = tmp_path / "predictor.joblib"
    model.save(path)
    restored = TabPFNModel.load(path)
    payload = restored._fitted_payload
    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", SimpleNamespace())
    with pytest.raises(ImportError, match="native fitted persistence helpers"):
        restored.predict(loader())
    assert restored.model is None
    assert restored._fitted_payload == payload

    def failed_reload(path, *, device):
        raise OSError("license/authentication failed")

    monkeypatch.setitem(sys.modules, "tabpfn.model_loading", SimpleNamespace(
        save_fitted_tabpfn_model=lambda estimator, path: None, load_fitted_tabpfn_model=failed_reload,
    ))
    with pytest.raises(RuntimeError, match="license/authentication"):
        restored.predict(loader())
    assert restored.model is None
    assert restored._fitted_payload == payload


def test_lightgbm_validation_early_stopping_api(monkeypatch):
    from models import LightGBMModel

    calls = {}

    class FakeDataset:
        def __init__(self, X, label, reference=None):
            self.X, self.label, self.reference = X, label, reference

    class FakeBooster:
        best_iteration = 2

        def predict(self, X, num_iteration=None):
            assert num_iteration == 2
            return np.zeros(len(X))

    def train(params, train_data, **kwargs):
        calls.update(kwargs)
        calls["params"] = params
        calls["train_data"] = train_data
        return FakeBooster()

    monkeypatch.setitem(sys.modules, "lightgbm", SimpleNamespace(
        Dataset=FakeDataset, train=train,
        log_evaluation=lambda period: ("log", period),
        early_stopping=lambda patience, verbose: ("early_stop", patience),
    ))
    model = LightGBMModel({"early_stopping_rounds": 3}).fit(loader(), val_dataloader=loader())
    assert ("early_stop", 3) in calls["callbacks"]
    assert calls["valid_sets"][0].reference is calls["train_data"]
    assert calls["params"]["metric"] == "l2"
    assert calls["params"]["seed"] == 42
    assert model.predict(loader()).shape == (20, 1)


def test_lightgbm_dependency_is_optional(monkeypatch):
    from models import LightGBMModel

    monkeypatch.setitem(sys.modules, "lightgbm", None)
    with pytest.raises(ImportError, match="optional 'lightgbm'"):
        LightGBMModel({}).fit(loader())


def test_model_configs_have_cpu_safe_neural_defaults_and_optional_backends():
    from omegaconf import OmegaConf

    directory = Path(__file__).resolve().parents[1] / "conf" / "model"
    assert OmegaConf.load(directory.parent / "config.yaml").precision == "32-true"
    for name in ("neural_net", "transformer", "gnn"):
        config = OmegaConf.load(directory / f"{name}.yaml")
        assert "precision" not in config
        assert config.max_epochs == 100
        assert config.early_stopping_patience == 12
        assert config.random_state == 42
    assert OmegaConf.load(directory / "lightgbm.yaml").name == "lightgbm"
    assert not OmegaConf.load(directory / "tabpfn.yaml").allow_model_download
    assert not OmegaConf.load(directory / "gnn.yaml").graph_input
