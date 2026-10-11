import torch
import torch.nn as nn

from .base_model import LightningBaseModel


class NeuralNet(LightningBaseModel):
    def __init__(self, config: dict[str, any], wandb_logger=None):
        super().__init__(config, wandb_logger)

        # Validate required config parameters
        if "input_dim" not in self.config:
            raise ValueError("Missing required config parameter: 'input_dim'")
        if "hidden_dims" not in self.config:
            raise ValueError("Missing required config parameter: 'hidden_dims'")
        if "output_dim" not in self.config:
            raise ValueError("Missing required config parameter: 'output_dim'")
        
        hidden_dims = self.config["hidden_dims"]
        if not isinstance(hidden_dims, list) or len(hidden_dims) == 0:
            raise ValueError("'hidden_dims' must be a non-empty list")

        layers = []
        prev_dim = self.config["input_dim"]
        for dim in hidden_dims:
            layers.append(nn.Linear(prev_dim, dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(self.config.get("dropout", 0.1)))
            prev_dim = dim
        layers.append(nn.Linear(prev_dim, self.config["output_dim"]))

        self.model = nn.Sequential(*layers)

    def forward(self, X: torch.Tensor) -> torch.Tensor:
        """Performs one forward pass of the Neural Network."""
        return self.model(X)
