from .base_model import LightningBaseModel
from typing import Dict, Any
import torch
import torch.nn as nn

class NeuralNet(LightningBaseModel):

    def __init__(self, config: Dict[str, Any], wandb_logger=None):
        super().__init__(config, wandb_logger)

        layers = []
        prev_dim = self.config.get("input_dim")
        for dim in self.config.get("hidden_dims"):
            layers.append(nn.Linear(prev_dim, dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(self.config.get("dropout", 0.1)))
            prev_dim = dim
        layers.append(nn.Linear(prev_dim, self.config.get("output_dim")))

        self.model = nn.Sequential(*layers)

    def forward(self, X: torch.Tensor) -> torch.Tensor:
        """Performs one forward pass of the Neural Network."""
        return self.model(X)