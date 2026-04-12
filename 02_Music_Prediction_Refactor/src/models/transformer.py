from .base_model import LightningBaseModel
import torch.nn as nn
import torch
from typing import Dict, Any

class PositionalEncoding(nn.Module):
    """Positional Encoding for Transformer Models."""

    def __init__(self, d_model: int, max_len: int = 5000):
        super().__init__()
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-torch.log(torch.tensor(10000.0)) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)
        self.register_buffer('pe', pe)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + self.pe[:, :x.size(1)]

class TransformerModel(LightningBaseModel):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        # Input embedding layer
        self.embedding = nn.Linear(self.config["input_dim"], self.config["d_model"])
        self.pos_encoder = PositionalEncoding(self.config["d_model"], self.config["max_seq_len"])

        # Transformer encoder
        encoder_layers = nn.TransformerEncoderLayer(
            d_model=self.config["d_model"],
            nhead=self.config["nhead"],
            dropout=self.config.get("dropout", 0.1)
        )
        self.transformer_encoder = nn.TransformerEncoder(
            encoder_layers,
            num_layers=self.config["num_layers"]
        )

        # Output layer
        self.output_layer = nn.Linear(self.config["d_model"], self.config["output_dim"])

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass of the transformer model."""
        # x shape: (batch_size, seq_len, input_dim) or (batch_size, input_dim)
        if x.dim() == 2:
            x = x.unsqueeze(1)  # Add sequence dimension if not present

        # Embed and add positional encoding
        x = self.embedding(x)
        x = self.pos_encoder(x)

        # Transformer
        x = self.transformer_encoder(x)

        # Average over sequence dimension and project to output
        x = x.mean(dim=1)
        return self.output_layer(x)