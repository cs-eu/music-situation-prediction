from .base_model import LightningBaseModel
import torch.nn as nn

class PositionalEmbedding(nn.Module):
    """Positional Encoding for Transformer Models."""

    def __init__(self, d_model: int, max_len: int = 5000):
        pass


class Transformer(LightningBaseModel):
    pass