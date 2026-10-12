import subprocess
import sys
from pathlib import Path

import joblib
import pytest
import torch
from torch.utils.data import DataLoader, TensorDataset

pytest.importorskip("lightning")
pytest.importorskip("torch_geometric")
from torch_geometric.data import Batch, Data
from torch_geometric.loader import DataLoader as GraphLoader
from torch_geometric.nn import GCNConv

from models import GNNModel


FEATURE_NAMES = [
    "num__gps_count", "num__gps_first_latitude", "cat__gps_landuse_type_residential",
    "gps_missing__missingindicator_gps_first_latitude", "num__hour_sin", "cat__time_bin_morning",
    "num__session_duration_sec", "num__screen_ON_UNLOCKED", "num__headset_PLUGGED",
    "num__detected_WALKING", "num__phone_GPS", "num__app_Orientation", "num__other_context",
]


@pytest.fixture(scope="module", autouse=True)
def small_graph_cpu_threads():
    previous = torch.get_num_threads()
    torch.set_num_threads(1)
    yield
    torch.set_num_threads(previous)


def config(**overrides):
    return {"input_dim": len(FEATURE_NAMES), "feature_names": FEATURE_NAMES,
            "hidden_dim": 8, "num_layers": 2, "dropout": 0.1, **overrides}


def loader(x, y=None, batch_size=3, shuffle=False):
    if y is None:
        y = x[:, :1] + 0.25 * x[:, 1:2]
    return DataLoader(TensorDataset(x, y), batch_size=batch_size, shuffle=shuffle,
                      generator=torch.Generator().manual_seed(42))


def test_feature_graph_uses_real_gcn_and_domain_hubs_with_sparse_undirected_edges():
    model = GNNModel(config())
    assert all(isinstance(conv, GCNConv) and not conv.cached for conv in model.convs)
    assert model.modality_names == ("location", "time", "screen", "headset", "activity", "phone", "app", "other")
    assert model.feature_hub_index.tolist() == [0, 0, 0, 0, 1, 1, 1, 2, 3, 4, 5, 6, 7]
    edges = set(map(tuple, model.edge_index.t().tolist()))
    assert len(edges) == 2 * (model.input_dim + len(model.modality_names))
    assert all((target, source) in edges for source, target in edges)
    context = model.num_nodes_per_window - 1
    for index, modality in enumerate(model.feature_hub_index):
        assert (index, model.input_dim + int(modality)) in edges
    for hub in range(model.input_dim, context):
        assert (hub, context) in edges


def test_vectorized_feature_graph_has_no_cross_window_edges_and_is_label_independent():
    model = GNNModel(config()).eval()
    x = torch.randn(5, len(FEATURE_NAMES))
    nodes, edges, batch = model._feature_graph(x)
    assert nodes.shape == (len(x) * model.num_nodes_per_window, 8)
    assert batch.tolist() == [i for i in range(len(x)) for _ in range(model.num_nodes_per_window)]
    assert torch.equal(batch[edges[0]], batch[edges[1]])
    assert edges.shape[1] == len(x) * model.edge_index.shape[1]
    assert torch.equal(edges, model._feature_graph(x + 100)[1])
    assert torch.equal(edges, GNNModel(config())._feature_graph(x)[1])
    first = model.predict(loader(x, torch.randn(len(x), 1)))
    second = model.predict(loader(x, torch.randn(len(x), 1) * 100))
    torch.testing.assert_close(first, second, rtol=0, atol=0)


def test_eval_predictions_are_batch_size_and_member_invariant():
    torch.manual_seed(42)
    model = GNNModel(config()).eval()
    x = torch.randn(9, len(FEATURE_NAMES))
    with torch.inference_mode():
        expected = model(x)
        singles = torch.cat([model(row.unsqueeze(0)) for row in x])
        permutation = torch.tensor([4, 1, 8, 0, 5, 3, 2, 7, 6])
        permuted = model(x[permutation])[torch.argsort(permutation)]
        co_members = torch.cat((x[3:4], torch.randn(7, x.shape[1]) * 100))
        changed_members = model(co_members)[0]
    assert expected.shape == (9, 1)
    torch.testing.assert_close(expected, singles, atol=1e-6, rtol=1e-5)
    torch.testing.assert_close(expected, permuted, atol=1e-6, rtol=1e-5)
    torch.testing.assert_close(expected[3], changed_members, atol=1e-6, rtol=1e-5)
    for batch_size in (1, 2, 4, 9):
        torch.testing.assert_close(model.predict(loader(x, batch_size=batch_size)), expected,
                                   atol=1e-6, rtol=1e-5)


def test_learned_identity_distinguishes_equal_values_and_swapping_features():
    model = GNNModel(config(input_dim=2, feature_names=["num__gps_count", "num__gps_home"], dropout=0)).eval()
    with torch.no_grad():
        model.feature_embedding.zero_()
        model.feature_embedding[1, 0] = 3
        model.value_embedding.weight.fill_(1)
        model.value_embedding.bias.zero_()
    nodes, _, _ = model._feature_graph(torch.zeros(1, 2))
    assert not torch.equal(nodes[0], nodes[1])
    # A simple deterministic parameter setting proves feature swapping is not a symmetry.
    with torch.no_grad():
        for conv in model.convs:
            conv.lin.weight.copy_(torch.eye(8))
            conv.bias.zero_()
        model.output_layer.weight.fill_(1)
        model.output_layer.bias.zero_()
        model.virtual_embedding.zero_()
    first = model(torch.tensor([[-2.0, 1.0]]))
    second = model(torch.tensor([[1.0, -2.0]]))
    assert not torch.allclose(first, second)
    model(torch.randn(4, 2)).sum().backward()
    assert model.feature_embedding.grad is not None
    assert model.value_embedding.weight.grad is not None


@pytest.mark.parametrize("overrides,match", [
    ({"feature_names": None}, "train-fitted"),
    ({"feature_names": ["gps_count"]}, "matching input_dim"),
    ({"feature_names": ["gps_count"] * len(FEATURE_NAMES)}, "unique"),
    ({"input_dim": 0}, "input_dim"),
    ({"input_dim": 1.5}, "input_dim"),
    ({"hidden_dim": 0}, "hidden_dim"),
    ({"num_layers": 0}, "num_layers"),
    ({"dropout": float("nan")}, "dropout"),
    ({"dropout": 1}, "dropout"),
    ({"output_dim": 2}, "one regression target"),
    ({"graph_level": False}, "one prediction per window"),
])
def test_configuration_validation(overrides, match):
    with pytest.raises(ValueError, match=match):
        GNNModel(config(**overrides))


@pytest.mark.parametrize("x,match", [
    (torch.ones(3, 2), "features"),
    (torch.ones(3, 1, len(FEATURE_NAMES)), "features"),
    (torch.empty(0, len(FEATURE_NAMES)), "at least one window"),
    (torch.ones(3, len(FEATURE_NAMES), dtype=torch.long), "floating-point"),
    (torch.full((3, len(FEATURE_NAMES)), float("inf")), "finite"),
    (torch.full((3, len(FEATURE_NAMES)), float("nan")), "finite"),
    (torch.full((3, len(FEATURE_NAMES)), 1e300, dtype=torch.float64), "finite"),
])
def test_tabular_input_validation(x, match):
    with pytest.raises(ValueError, match=match):
        GNNModel(config())(x)


def test_training_rejects_nonfinite_labels():
    model = GNNModel(config())
    with pytest.raises(ValueError, match="finite observed targets"):
        model._regression_step((torch.zeros(2, len(FEATURE_NAMES)), torch.tensor([float("inf"), 1.])), "train")


def test_train_fitted_schema_not_unseen_test_categories_defines_topology():
    import pandas as pd
    from data.preprocess import build_preprocessor

    train = pd.DataFrame({"gps_landuse_type": ["home", "work"], "gps_count": [1., 2.],
                          "phone_GPS": [0., 1.], "app_Orientation": [1., 0.]})
    heldout = train.copy()
    heldout["gps_landuse_type"] = "unseen_test_category"
    preprocessor = build_preprocessor(train).fit(train)
    names = preprocessor.get_feature_names_out().tolist()
    assert all("unseen_test_category" not in name for name in names)
    model = GNNModel(config(input_dim=len(names), feature_names=names)).eval()
    train_x = torch.as_tensor(preprocessor.transform(train), dtype=torch.float32)
    test_x = torch.as_tensor(preprocessor.transform(heldout), dtype=torch.float32)
    assert torch.equal(model._feature_graph(train_x)[1], model._feature_graph(test_x)[1])
    assert model.modality_names == ("location", "phone", "app")


def test_topology_buffers_follow_model_dtype_and_device():
    model = GNNModel(config()).to(device="cpu", dtype=torch.float64).eval()
    assert model.edge_index.dtype == torch.long
    assert model.feature_hub_index.dtype == torch.long
    assert model.hub_counts.dtype == torch.float64
    nodes, edges, batch = model._feature_graph(torch.randn(2, len(FEATURE_NAMES)))
    assert nodes.dtype == torch.float64
    assert nodes.device == edges.device == batch.device == model.hub_counts.device
    assert model(torch.randn(2, len(FEATURE_NAMES))).dtype == torch.float64


def test_optional_dependency_error_and_classical_import_isolation():
    script = """
import importlib.abc
import sys
class BlockGraph(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'torch_geometric':
            raise ImportError('graph unavailable')
sys.meta_path.insert(0, BlockGraph())
from models import BaselineModel, ElasticNetModel, GNNModel
BaselineModel({})
ElasticNetModel({})
assert 'torch_geometric' not in sys.modules
try:
    GNNModel({'input_dim': 1, 'feature_names': ['num__gps_count']})
except ImportError as exc:
    assert 'pip install torch-geometric' in str(exc)
else:
    raise AssertionError('missing PyG should be actionable')
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                            cwd=Path(__file__).resolve().parents[1] / "src")
    assert result.returncode == 0, result.stderr


def explicit_graphs():
    return [Data(x=torch.randn(n, 3),
                 edge_index=torch.stack((torch.arange(n - 1), torch.arange(1, n))),
                 y=torch.tensor([[float(n)]])) for n in (4, 2, 3)]


def test_explicit_graph_path_pools_one_output_per_graph_and_preserves_node_mode():
    graphs = explicit_graphs()
    model = GNNModel(config(graph_input=True, input_dim=3, dropout=0)).eval()
    batch = Batch.from_data_list(graphs)
    expected = torch.cat([model(graph) for graph in graphs])
    torch.testing.assert_close(model(batch), expected)
    assert model(batch).shape == (3, 1)
    torch.testing.assert_close(model.predict(GraphLoader(graphs, batch_size=2)), expected)
    for graph, prediction in zip(graphs, expected):
        graph.y = prediction.detach().reshape(1, 1)
    assert model.evaluate(GraphLoader(graphs, batch_size=2))["RMSE"] == pytest.approx(0, abs=1e-7)
    node_model = GNNModel(config(graph_input=True, graph_level=False, input_dim=3)).eval()
    assert node_model(batch).shape == (9, 1)
    with pytest.raises(ValueError, match="explicit PyG"):
        model(torch.randn(2, 3))


@pytest.mark.parametrize("problem,match", [
    ("nonfinite", "finite"), ("width", "node features"), ("edge_dtype", "long tensor"),
    ("edge_range", "valid nodes"), ("cross_graph", "cross-graph"),
    ("batch_gap", "contiguous"), ("batch_shape", "assign each node"), ("empty_graph", "empty graphs"),
])
def test_explicit_graph_validation(problem, match):
    model = GNNModel(config(graph_input=True, input_dim=3))
    batch = Batch.from_data_list(explicit_graphs())
    if problem == "nonfinite":
        batch.x[0, 0] = float("nan")
    elif problem == "width":
        batch.x = batch.x[:, :2]
    elif problem == "edge_dtype":
        batch.edge_index = batch.edge_index.float()
    elif problem == "edge_range":
        batch.edge_index[0, 0] = len(batch.x)
    elif problem == "cross_graph":
        batch.edge_index[:, 0] = torch.tensor([0, 4])
    elif problem == "batch_gap":
        batch.batch[batch.batch == 1] = 3
    elif problem == "batch_shape":
        batch.batch = batch.batch[:-1]
    elif problem == "empty_graph":
        batch = Batch.from_data_list([explicit_graphs()[0], Data(x=torch.empty(0, 3),
                                           edge_index=torch.empty(2, 0, dtype=torch.long), y=torch.zeros(1, 1))])
    with pytest.raises(ValueError, match=match):
        model(batch)


@pytest.mark.parametrize("explicit", [False, True])
def test_lightning_two_epoch_fit_and_cpu_roundtrips(explicit, tmp_path):
    import lightning as L

    torch.manual_seed(42)
    model_config = config(graph_input=explicit, **({"input_dim": 3} if explicit else {}))
    model = GNNModel(model_config)
    data = GraphLoader(explicit_graphs(), batch_size=2) if explicit else loader(torch.randn(11, len(FEATURE_NAMES)))
    trainer = L.Trainer(max_epochs=2, logger=False, enable_checkpointing=False,
                        enable_progress_bar=False, enable_model_summary=False, num_sanity_val_steps=0,
                        accelerator="cpu", devices=1)
    initial = model.value_embedding.weight.detach().clone() if not explicit else model.convs[0].lin.weight.detach().clone()
    trainer.fit(model, train_dataloaders=data, val_dataloaders=data)
    assert trainer.current_epoch == 2
    assert torch.isfinite(trainer.callback_metrics["train_loss"])
    assert torch.isfinite(trainer.callback_metrics["val_loss"])
    assert trainer.callback_metrics["train_rmse"].square() == pytest.approx(
        float(trainer.callback_metrics["train_mse"]), rel=1e-6)
    trained = model.value_embedding.weight if not explicit else model.convs[0].lin.weight
    assert not torch.equal(initial, trained)
    expected = model.predict(data)
    torch.testing.assert_close(model.predict(data, trainer=trainer), expected)
    assert expected.shape == (3 if explicit else 11, 1)
    assert all(torch.isfinite(torch.tensor(value)) for value in model.evaluate(data).values())
    path = tmp_path / "gnn.pt"
    model.save(path)
    restored = GNNModel.load(path, model_config)
    assert not restored.training
    assert all(buffer.device.type == "cpu" for buffer in restored.buffers())
    torch.testing.assert_close(restored.predict(data), expected)
    if not explicit:
        assert {"edge_index", "feature_hub_index", "hub_counts"} <= set(restored.state_dict())
        torch.testing.assert_close(restored.edge_index, model.edge_index)
    # Parent stores the full model wrapper with preprocessing in a joblib bundle.
    model._trainer = None
    bundle = tmp_path / "bundle.joblib"
    joblib.dump({"model": model.cpu()}, bundle)
    roundtrip = joblib.load(bundle)["model"]
    torch.testing.assert_close(roundtrip.predict(data), expected)
    checkpoint = tmp_path / "gnn.ckpt"
    trainer.save_checkpoint(checkpoint)
    checkpoint_model = GNNModel.load(checkpoint, model_config)
    torch.testing.assert_close(checkpoint_model.predict(data), expected)


def test_hydra_gnn_config_supports_individual_and_multimodel_composition():
    from hydra import compose, initialize_config_dir

    conf = Path(__file__).resolve().parents[1] / "conf"
    with initialize_config_dir(config_dir=str(conf), version_base=None):
        for name in ("neural_net", "gnn", "transformer"):
            cfg = compose(config_name="config", overrides=[f"model={name}"])
            assert cfg.model.name == name
            if name == "gnn":
                assert not cfg.model.graph_input
                assert cfg.model.hidden_dim == 64
                assert cfg.model.num_layers == 2
                assert cfg.model.max_epochs == 100
                assert cfg.model.early_stopping_patience == 12
                assert cfg.model.learning_rate == 1e-3
                configured = dict(cfg.model)
                configured.update(input_dim=len(FEATURE_NAMES), feature_names=FEATURE_NAMES)
                assert GNNModel(configured)(torch.randn(2, len(FEATURE_NAMES))).shape == (2, 1)


def test_hydra_actual_multirun_includes_tabular_gnn(tmp_path):
    conf = Path(__file__).resolve().parents[1] / "conf"
    script = f"""
import hydra
import torch
from models import GNNModel
torch.set_num_threads(1)
@hydra.main(version_base=None, config_path={str(conf)!r}, config_name='config')
def run(cfg):
    if cfg.model.name == 'gnn':
        config = dict(cfg.model)
        config.update(input_dim=2, feature_names=['num__gps_count', 'num__phone_GPS'])
        model = GNNModel(config).eval()
        assert model(torch.ones(3, 2)).shape == (3, 1)
    print('COMPOSED=' + cfg.model.name)
run()
"""
    result = subprocess.run([sys.executable, "-c", script, "-m", "model=neural_net,gnn,transformer",
                             f"hydra.sweep.dir={tmp_path / 'sweep'}", "hydra.job.chdir=false"],
                            capture_output=True, text=True,
                            cwd=Path(__file__).resolve().parents[1] / "src", timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert all(f"COMPOSED={name}" in result.stdout for name in ("neural_net", "gnn", "transformer"))
