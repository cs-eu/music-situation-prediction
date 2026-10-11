"""Lazy exports keep optional neural/graph dependencies out of classical runs."""

from importlib import import_module

_MODULES = {
    "BaselineModel": "baseline",
    "ElasticNetModel": "elastic_net",
    "RandomForestModel": "random_forest",
    "LightGBMModel": "lightgbm_model",
    "TabPFNModel": "tabpfn",
    "NeuralNet": "neural_net",
    "TransformerModel": "transformer",
    "GNNModel": "gnn",
}
__all__ = list(_MODULES)


def __getattr__(name):
    if name not in _MODULES:
        raise AttributeError(name)
    value = getattr(import_module(f".{_MODULES[name]}", __name__), name)
    globals()[name] = value
    return value
