from typing import Any

import torch
import torch.nn as nn

from .lightning_base import LightningBaseModel


class TransformerModel(LightningBaseModel):
    """Attention across feature tokens within each independent tabular window."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.input_dim = self.config["input_dim"]
        d_model = self.config.get("d_model", 32)
        nhead = self.config.get("nhead", 4)
        if self.input_dim <= 0 or d_model <= 0 or nhead <= 0 or d_model % nhead:
            raise ValueError("input_dim must be positive and d_model must be divisible by nhead.")
        if self.config.get("output_dim", 1) != 1:
            raise ValueError("TransformerModel learns one regression target at a time (output_dim=1).")
        self.value_embedding = nn.Linear(1, d_model)
        self.feature_embedding = nn.Parameter(torch.empty(self.input_dim, d_model))
        nn.init.normal_(self.feature_embedding, std=0.02)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=nhead,
            dim_feedforward=self.config.get("dim_feedforward", 128),
            dropout=self.config.get("dropout", 0.1), batch_first=True,
        )
        self.transformer_encoder = nn.TransformerEncoder(
            encoder_layer, num_layers=self.config.get("num_layers", 2), enable_nested_tensor=False,
        )
        self.output_layer = nn.Linear(d_model, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 2 or x.shape[1] != self.input_dim:
            raise ValueError("TransformerModel expects tabular [rows, input_dim], not a fabricated sequence.")
        tokens = self.value_embedding(x.unsqueeze(-1)) + self.feature_embedding.unsqueeze(0)
        tokens = self.transformer_encoder(tokens)
        return self.output_layer(tokens.mean(dim=1))
