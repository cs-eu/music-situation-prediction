from typing import Any

import torch
import torch.nn as nn

from .lightning_base import LightningBaseModel


class NeuralNet(LightningBaseModel):
    def __init__(self, config: dict[str, Any], wandb_logger=None):
        super().__init__(config, wandb_logger)

        # Validate required config parameters
        if "input_dim" not in self.config:
            raise ValueError("Missing required config parameter: 'input_dim'")
        if "output_dim" not in self.config:
            raise ValueError("Missing required config parameter: 'output_dim'")
        hidden_dims = self.config.get("hidden_dims", [128, 64])
        if not hidden_dims or any(not isinstance(dim, int) or dim <= 0 for dim in hidden_dims):
            raise ValueError("'hidden_dims' must be a non-empty sequence of positive integers")
        if self.config["output_dim"] != 1:
            raise ValueError("NeuralNet learns one regression target at a time (output_dim=1).")

        self.blocks = nn.ModuleList()
        self.shortcuts = nn.ModuleList()
        prev_dim = self.config["input_dim"]
        for dim in hidden_dims:
            self.blocks.append(nn.Sequential(
                nn.Linear(prev_dim, dim), nn.ReLU(),
                nn.Dropout(self.config.get("dropout", 0.1)), nn.Linear(dim, dim),
            ))
            self.shortcuts.append(nn.Identity() if prev_dim == dim else nn.Linear(prev_dim, dim, bias=False))
            prev_dim = dim
        self.output_layer = nn.Linear(prev_dim, 1)

    def forward(self, X: torch.Tensor) -> torch.Tensor:
        """Performs one forward pass of the Neural Network."""
        if X.ndim != 2 or X.shape[1] != self.config["input_dim"]:
            raise ValueError("NeuralNet requires tabular features shaped [rows, input_dim].")
        for block, shortcut in zip(self.blocks, self.shortcuts):
            X = torch.relu(block(X) + shortcut(X))
        return self.output_layer(X)
