import torch
import numpy as np
from typing import Dict, Any
from .base_model import BaseModel

class BaselineModel(BaseModel):

    def __init__(self):
        self.mean = None

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Baseline is mean of training set."""
        targets = []
        for _, y in dataloader:
            targets.append(y)
        self.mean = torch.cat(targets, dim=0).mean(dim=0)

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Predict mean for all samples."""
        length = len(dataloader)
        return torch.stack([self.mean] * length)