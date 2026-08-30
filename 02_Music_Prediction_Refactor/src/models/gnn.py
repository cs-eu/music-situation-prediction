import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv

from .base_model import LightningBaseModel


class GNNModel(LightningBaseModel):
    def __init__(self, config: dict[str, any]):
        super().__init__(config)

        # Define GNN layers
        self.convs = nn.ModuleList()
        self.convs.append(GCNConv(self.config["input_dim"], self.config["hidden_dim"]))

        for _ in range(self.config["num_layers"] - 2):
            self.convs.append(
                GCNConv(self.config["hidden_dim"], self.config["hidden_dim"])
            )

        if self.config["num_layers"] > 1:
            self.convs.append(
                GCNConv(self.config["hidden_dim"], self.config["hidden_dim"])
            )
            self.output_layer = nn.Linear(
                self.config["hidden_dim"], self.config["output_dim"]
            )
        else:
            self.output_layer = nn.Linear(
                self.config["hidden_dim"], self.config["output_dim"]
            )

    def _create_graph_data(self, x: torch.Tensor) -> Data:
        """Create a graph data object from input features.
        
        Args:
            x: Input tensor of shape (batch_size, feature_dim) or (batch_size, num_nodes, feature_dim)
            
        Returns:
            Data object with node features and edges
        """
        # Handle 3D input (batch_size, num_nodes, feature_dim) -> flatten to (total_nodes, feature_dim)
        if x.dim() == 3:
            batch_size, num_nodes, feature_dim = x.shape
            x_flat = x.view(-1, feature_dim)  # (batch_size * num_nodes, feature_dim)
        else:
            # Treat as single node per sample
            batch_size = x.shape[0]
            feature_dim = x.shape[1]
            num_nodes = 1
            x_flat = x

        # Create a simple graph: connect each node to a few neighbors
        # For single-node-per-sample case, create a self-loop
        if num_nodes == 1:
            # Self-loop for each sample
            edge_index = torch.arange(batch_size, dtype=torch.long, device=x.device).repeat(2, 1)
        else:
            # Create fully connected graph within each sample
            edge_indices = []
            for i in range(batch_size):
                start_idx = i * num_nodes
                end_idx = (i + 1) * num_nodes
                # Get node indices for this sample
                nodes_in_sample = torch.arange(start_idx, end_idx, dtype=torch.long, device=x.device)
                # Create all edges within the sample (fully connected)
                if num_nodes > 1:
                    edges = torch.combinations(nodes_in_sample, 2).t()
                    # Add reverse edges for undirected graph
                    edges = torch.cat([edges, edges.flip(0)], dim=1)
                else:
                    # Self-loop
                    edges = torch.stack([nodes_in_sample, nodes_in_sample])
                edge_indices.append(edges)
            
            edge_index = torch.cat(edge_indices, dim=1)
        
        # Validate edge_index
        assert edge_index.shape[0] == 2, f"Edge index must have shape [2, num_edges], got {edge_index.shape}"
        assert edge_index.dtype == torch.long, f"Edge index must be torch.long, got {edge_index.dtype}"
        assert edge_index.min() >= 0, "Edge index has negative indices"
        assert edge_index.max() < x_flat.shape[0], f"Edge index out of bounds: max={edge_index.max()}, num_nodes={x_flat.shape[0]}"
        assert edge_index.shape[1] > 0, "Edge index is empty"
        
        return Data(x=x_flat, edge_index=edge_index)

    def forward(self, data: Data) -> torch.Tensor:
        """Forward pass of the GNN model."""
        x, edge_index = data.x, data.edge_index

        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i < len(self.convs) - 1:
                x = F.relu(x)
                x = F.dropout(
                    x, p=self.config.get("dropout", 0.1), training=self.training
                )

        return self.output_layer(x)

    def training_step(
        self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int
    ) -> torch.Tensor:
        """Training step for PyTorch Lightning."""
        x, y = batch
        data = self._create_graph_data(x)
        y_hat = self(data)
        # Match output shape to target shape
        y_hat = y_hat[:y.shape[0]]
        loss = F.mse_loss(y_hat.view_as(y), y)
        self.log("train_loss", loss, on_step=True, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(
        self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int
    ) -> torch.Tensor:
        """Validation step for PyTorch Lightning."""
        x, y = batch
        data = self._create_graph_data(x)
        y_hat = self(data)
        # Match output shape to target shape
        y_hat = y_hat[:y.shape[0]]
        loss = F.mse_loss(y_hat.view_as(y), y)
        self.log("val_loss", loss, on_step=False, on_epoch=True, prog_bar=True)
        return loss

    def test_step(
        self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int
    ) -> torch.Tensor:
        """Test step for PyTorch Lightning."""
        x, y = batch
        data = self._create_graph_data(x)
        y_hat = self(data)
        # Match output shape to target shape
        y_hat = y_hat[:y.shape[0]]
        loss = F.mse_loss(y_hat.view_as(y), y)
        self.log("test_loss", loss, on_step=False, on_epoch=True, prog_bar=True)
        return loss

    def predict_step(self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int):
        """Predict step for PyTorch Lightning."""
        x, _ = batch
        data = self._create_graph_data(x)
        y_hat = self(data)
        # Return only predictions for actual samples
        return y_hat[:x.shape[0]]
