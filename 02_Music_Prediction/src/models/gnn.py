from numbers import Integral
from typing import Any

import torch
import torch.nn as nn
import torch.nn.functional as F

from .lightning_base import LightningBaseModel


class GNNModel(LightningBaseModel):
    """Per-window feature GCN with an experimental, non-causal modality prior.

    Train-fitted transformed feature names define sparse feature-to-modality and
    modality-to-context edges, never relationships between windows/participants.
    ``graph_input=True`` instead accepts explicit PyG Data/Batch node features.
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.graph_input = self.config.get("graph_input", False)
        self.graph_level = self.config.get("graph_level", True)
        for name, default in (("input_dim", None), ("hidden_dim", 64), ("num_layers", 2)):
            value = self.config.get(name, default)
            if isinstance(value, bool) or not isinstance(value, Integral) or value < 1:
                raise ValueError(f"GNN {name} must be a positive integer.")
        if self.config.get("output_dim", 1) != 1:
            raise ValueError("GNN learns one regression target at a time (output_dim=1).")
        if not self.graph_input and not self.graph_level:
            raise ValueError("Tabular GNN requires graph_level=True: one prediction per window.")
        self.input_dim = self.config["input_dim"]
        hidden_dim = self.config.get("hidden_dim", 64)
        self.dropout = self.config.get("dropout", 0.1)
        if not isinstance(self.dropout, (int, float)) or not 0 <= self.dropout < 1:
            raise ValueError("GNN dropout must be finite and in [0, 1).")
        if not self.graph_input:
            feature_names = self.config.get("feature_names")
            if (not isinstance(feature_names, (list, tuple))
                    or len(feature_names) != self.input_dim
                    or any(not isinstance(name, str) or not name for name in feature_names)
                    or len(set(feature_names)) != len(feature_names)):
                raise ValueError(
                    "Tabular GNN requires unique train-fitted transformed feature_names "
                    "matching input_dim (preprocessor.get_feature_names_out().tolist())."
                )
            self.feature_names = tuple(feature_names)
            self._init_feature_graph(hidden_dim)
        try:
            from torch_geometric.data import Data
            from torch_geometric.nn import GCNConv, global_mean_pool
        except ImportError as exc:
            raise ImportError(
                "GNN requires the optional 'graph' extra: pip install -e '.[graph]' "
                "or pip install torch-geometric."
            ) from exc
        self._data_type = Data
        self._pool = global_mean_pool
        self.convs = nn.ModuleList([
            GCNConv(self.input_dim if self.graph_input and i == 0 else hidden_dim,
                    hidden_dim, cached=False)
            for i in range(self.config.get("num_layers", 2))
        ])
        self.output_layer = nn.Linear(hidden_dim, 1)

    @staticmethod
    def _modality(feature_name: str) -> str:
        name = feature_name.lower()
        while "__" in name and name.split("__", 1)[0] in {"num", "bin", "cat", "pass"}:
            name = name.split("__", 1)[1]
        if name.startswith(("gps_", "location_", "latitude", "longitude")):
            return "location"
        if name.startswith(("time_", "hour_", "dow_", "day_", "session_duration", "duration_sec")):
            return "time"
        for prefix, modality in (("screen_", "screen"), ("headset_", "headset"),
                                 ("detected_", "activity"), ("phone_", "phone"), ("app_", "app")):
            if name.startswith(prefix):
                return modality
        return "other"

    def _init_feature_graph(self, hidden_dim: int):
        modalities = [self._modality(name) for name in self.feature_names]
        self.modality_names = tuple(name for name in (
            "location", "time", "screen", "headset", "activity", "phone", "app", "other"
        ) if name in modalities)
        hub_index = torch.tensor([self.modality_names.index(name) for name in modalities], dtype=torch.long)
        self.register_buffer("feature_hub_index", hub_index)
        self.register_buffer("hub_counts", torch.bincount(hub_index).float())
        self.num_nodes_per_window = self.input_dim + len(self.modality_names) + 1
        features = torch.arange(self.input_dim)
        hubs = torch.arange(len(self.modality_names)) + self.input_dim
        context = torch.full_like(hubs, self.num_nodes_per_window - 1)
        edges = torch.stack((torch.cat((features, hubs)),
                             torch.cat((hub_index + self.input_dim, context))))
        self.register_buffer("edge_index", torch.cat((edges, edges.flip(0)), dim=1))
        self.value_embedding = nn.Linear(1, hidden_dim)
        self.feature_embedding = nn.Parameter(torch.empty(self.input_dim, hidden_dim))
        self.virtual_embedding = nn.Parameter(torch.empty(len(self.modality_names) + 1, hidden_dim))
        nn.init.normal_(self.feature_embedding, std=0.02)
        nn.init.normal_(self.virtual_embedding, std=0.02)

    def _feature_graph(self, x: torch.Tensor):
        if (not isinstance(x, torch.Tensor) or x.ndim != 2
                or x.shape[1] != self.input_dim or not x.shape[0]):
            raise ValueError(f"Tabular GNN requires [windows, {self.input_dim}] features and at least one window.")
        if not x.is_floating_point():
            raise ValueError("Tabular GNN requires finite floating-point preprocessed features.")
        x = x.to(dtype=self.value_embedding.weight.dtype)
        if not torch.isfinite(x).all():
            raise ValueError("Tabular GNN requires finite floating-point preprocessed features.")
        tokens = self.value_embedding(x.unsqueeze(-1)) + self.feature_embedding
        # Initialize modality hubs from this window only. Two GCN layers can then
        # exchange observed modality summaries through the shared context hub.
        hubs = tokens.new_zeros((len(x), len(self.modality_names), tokens.shape[-1]))
        indices = self.feature_hub_index.view(1, -1, 1).expand_as(tokens)
        hubs.scatter_add_(1, indices, tokens)
        hubs = hubs / self.hub_counts.view(1, -1, 1) + self.virtual_embedding[:-1]
        context = self.virtual_embedding[-1:].unsqueeze(0).expand(len(x), -1, -1)
        nodes = torch.cat((tokens, hubs, context), dim=1).reshape(-1, tokens.shape[-1])
        # Broadcasting offsets builds disjoint batched edges without per-row Data objects.
        windows = torch.arange(len(x), device=x.device)
        edges = (self.edge_index.unsqueeze(1)
                 + (windows * self.num_nodes_per_window).view(1, -1, 1)).reshape(2, -1)
        batch = windows.repeat_interleave(self.num_nodes_per_window)
        return nodes, edges, batch

    def _explicit_graph(self, data):
        if not isinstance(data, self._data_type) or data.x is None or data.edge_index is None:
            raise ValueError("GNN requires explicit PyG Data/Batch node features and edges, not tabular tensors.")
        x = data.x
        edges = data.edge_index
        if x.ndim != 2 or x.shape[1] != self.input_dim or not len(x):
            raise ValueError(f"Explicit GNN node features must have nonempty shape [nodes, {self.input_dim}].")
        if not x.is_floating_point():
            raise ValueError("Explicit GNN node features must be finite floating-point values.")
        x = x.to(dtype=self.output_layer.weight.dtype)
        if not torch.isfinite(x).all():
            raise ValueError("Explicit GNN node features must be finite floating-point values.")
        if edges.ndim != 2 or edges.shape[0] != 2 or edges.dtype != torch.long:
            raise ValueError("Explicit GNN edge_index must be a long tensor shaped [2, edges].")
        if edges.device != x.device or (edges.numel() and (edges.min() < 0 or edges.max() >= len(x))):
            raise ValueError("Explicit GNN edges must reference valid nodes on the same device.")
        batch = getattr(data, "batch", None)
        if batch is None:
            batch = torch.zeros(len(x), dtype=torch.long, device=x.device)
        if (batch.ndim != 1 or batch.shape[0] != len(x) or batch.dtype != torch.long
                or batch.device != x.device or batch.min() < 0):
            raise ValueError("Explicit GNN batch must assign each node a nonnegative long graph ID.")
        ids = torch.unique(batch)
        if not torch.equal(ids, torch.arange(len(ids), device=x.device)):
            raise ValueError("Explicit GNN graph IDs must be contiguous with no empty graphs.")
        ptr = getattr(data, "ptr", None)
        if ptr is not None and (len(ptr) - 1 != len(ids) or (ptr.diff() <= 0).any()):
            raise ValueError("Explicit GNN batches cannot contain empty graphs.")
        if edges.numel() and (batch[edges[0]] != batch[edges[1]]).any():
            raise ValueError("Explicit GNN batches must have no cross-graph edges.")
        return x, edges, batch

    def forward(self, data) -> torch.Tensor:
        x, edges, batch = self._explicit_graph(data) if self.graph_input else self._feature_graph(data)
        for conv in self.convs:
            x = F.dropout(F.relu(conv(x, edges)), p=self.dropout, training=self.training)
        if self.graph_level:
            x = self._pool(x, batch)
        return self.output_layer(x)

    def _regression_step(self, batch, stage):
        if self.graph_input:
            if not isinstance(batch, self._data_type) or batch.y is None:
                raise ValueError("GNN training requires explicit PyG Data/Batch objects with observed y.")
            batch = (batch, batch.y)
        if not torch.isfinite(batch[1]).all():
            raise ValueError("GNN training requires finite observed targets.")
        return super()._regression_step(batch, stage)

    def predict_step(self, batch, batch_idx):
        return self(batch) if self.graph_input else super().predict_step(batch, batch_idx)

    def predict(self, dataloader, **kwargs):
        if not self.graph_input or kwargs.get("trainer") is not None:
            return super().predict(dataloader, **kwargs)
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
        if not self.graph_input:
            return super().evaluate(dataloader)
        from utils.metrics import calculate_metrics

        batches = list(dataloader)
        if not batches or any(not isinstance(data, self._data_type) or data.y is None for data in batches):
            raise ValueError("GNN evaluation requires explicit PyG Data/Batch with observed targets.")
        predictions = self.predict(batches).reshape(-1)
        targets = torch.cat([data.y.detach().cpu().reshape(-1) for data in batches])
        if predictions.shape != targets.shape or not torch.isfinite(targets).all():
            raise ValueError("Explicit GNN requires one finite observed target per prediction.")
        return {name: float(value) for name, value in calculate_metrics(predictions, targets).items()}
