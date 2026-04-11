import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv
from typing import Dict, Any, Tuple
from .base_model import LightningBaseModel

class GNNModel(LightningBaseModel):

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        # Define GNN layers
        self.convs = nn.ModuleList()
        self.convs.append(GCNConv(self.config["input_dim"], self.config["hidden_dim"]))

        for _ in range(self.config["num_layers"] - 2):
            self.convs.append(GCNConv(self.config["hidden_dim"], self.config["hidden_dim"]))

        if self.config["num_layers"] > 1:
            self.convs.append(GCNConv(self.config["hidden_dim"], self.config["hidden_dim"]))
            self.output_layer = nn.Linear(self.config["hidden_dim"], self.config["output_dim"])
        else:
            self.output_layer = nn.Linear(self.config["hidden_dim"], self.config["output_dim"])

    def _create_graph_data(self, x: torch.Tensor) -> Data:
        """Create a graph data object from input features."""
        # Create a fully connected graph for this example
        # In a real scenario, you would define edges based on your data relationships
        batch_size, num_nodes, feature_dim = x.shape
        x = x.view(-1, feature_dim)  # Flatten nodes

        # Create fully connected graph within each sample
        edge_indices = []
        for i in range(batch_size):
            # Create edges for a fully connected graph
            nodes = torch.arange(i * num_nodes, (i + 1) * num_nodes)
            edges = torch.combinations(nodes, 2).t()
            edge_indices.append(edges)

        edge_index = torch.cat(edge_indices, dim=1)

        return Data(x=x, edge_index=edge_index)

    def forward(self, data: Data) -> torch.Tensor:
        """Forward pass of the GNN model."""
        x, edge_index = data.x, data.edge_index

        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i < len(self.convs) - 1:
                x = F.relu(x)
                x = F.dropout(x, p=self.config.get("dropout", 0.1), training=self.training)

        return self.output_layer(x)

    def training_step(self, batch: Tuple[torch.Tensor, torch.Tensor], batch_idx: int) -> torch.Tensor:
        """Training step for PyTorch Lightning."""
        x, y = batch
        data = self._create_graph_data(x.unsqueeze(1))  # Add dummy node dimension
        y_hat = self(data)
        loss = F.mse_loss(y_hat, y)
        self.log("train_loss", loss, on_step=True, on_epoch=True, prog_bar=True)
        return loss

    def predict_step(self, batch: Tuple[torch.Tensor, torch.Tensor], batch_idx: int):
        """Predict step for PyTorch Lightning."""
        x, _ = batch
        data = self._create_graph_data(x.unsqueeze(1))  # Add dummy node dimension
        return self(data)
