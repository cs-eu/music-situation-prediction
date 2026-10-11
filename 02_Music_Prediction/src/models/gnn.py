from typing import Any

import torch
import torch.nn as nn
import torch.nn.functional as F

from .lightning_base import LightningBaseModel


class GNNModel(LightningBaseModel):
    """Optional graph regressor; tabular situational windows are not graphs."""

    def __init__(self, config: dict[str, Any]):
        if not config.get("graph_input", False):
            raise ValueError(
                "GNN is unsupported by the tabular music-prediction pipeline: it does not construct graphs. "
                "Use neural_net, transformer, elastic_net, random_forest or lightgbm. "
                "Graph-specific consumers may set graph_input=True and provide PyG Data/Batch objects "
                "with explicit x, edge_index, batch (for graph-level regression), and y."
            )
        super().__init__(config)
        try:
            from torch_geometric.data import Data
            from torch_geometric.nn import GCNConv, global_mean_pool
        except ImportError as exc:
            raise ImportError("GNN requires the optional 'graph' extra: pip install -e '.[graph]'.") from exc
        self._data_type = Data
        self._pool = global_mean_pool
        hidden_dim = self.config.get("hidden_dim", 64)
        num_layers = self.config.get("num_layers", 2)
        if num_layers < 1:
            raise ValueError("GNN num_layers must be at least one.")
        if self.config.get("output_dim", 1) != 1:
            raise ValueError("GNN learns one regression target at a time (output_dim=1).")
        self.convs = nn.ModuleList([
            GCNConv(self.config["input_dim"] if i == 0 else hidden_dim, hidden_dim)
            for i in range(num_layers)
        ])
        self.output_layer = nn.Linear(hidden_dim, 1)

    def forward(self, data) -> torch.Tensor:
        if not isinstance(data, self._data_type) or data.x is None or data.edge_index is None:
            raise ValueError("GNN requires explicit PyG Data/Batch node features and edges, not tabular tensors.")
        x = data.x
        for conv in self.convs:
            x = F.dropout(F.relu(conv(x, data.edge_index)),
                          p=self.config.get("dropout", 0.1), training=self.training)
        if self.config.get("graph_level", True):
            batch = getattr(data, "batch", None)
            if batch is None:
                batch = torch.zeros(len(x), dtype=torch.long, device=x.device)
            x = self._pool(x, batch)
        return self.output_layer(x)

    def _regression_step(self, batch, stage):
        if not isinstance(batch, self._data_type) or batch.y is None:
            raise ValueError("GNN training requires explicit PyG Data/Batch objects with observed y.")
        return super()._regression_step((batch, batch.y), stage)

    def predict_step(self, batch, batch_idx):
        return self(batch)

    def predict(self, dataloader, **kwargs):
        was_training = self.training
        self.eval()
        try:
            with torch.inference_mode():
                predictions = []
                for data in dataloader:
                    if not isinstance(data, self._data_type):
                        raise ValueError("GNN prediction requires explicit PyG Data/Batch, not tabular batches.")
                    predictions.append(self(data.to(self.device)).cpu())
        finally:
            self.train(was_training)
        return torch.cat(predictions) if predictions else torch.empty((0, 1))

    def evaluate(self, dataloader):
        from utils.metrics import calculate_metrics

        batches = list(dataloader)
        if not batches or any(not isinstance(data, self._data_type) or data.y is None for data in batches):
            raise ValueError("GNN evaluation requires explicit PyG Data/Batch with observed targets.")
        predictions = self.predict(batches).reshape(-1)
        targets = torch.cat([data.y.detach().cpu().reshape(-1) for data in batches])
        return {name: float(value) for name, value in calculate_metrics(predictions, targets).items()}
