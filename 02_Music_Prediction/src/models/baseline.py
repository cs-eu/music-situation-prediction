from typing import Any

import torch

from .base_model import BaseModel


class BaselineModel(BaseModel):
    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config or {})
        self.mean = None

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Baseline is mean of training set."""
        _, targets = self._loader_arrays(dataloader)
        self.mean = float(targets.mean(dtype="float64"))
        return self

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Predict mean for all samples."""
        if self.mean is None:
            raise RuntimeError("Fit the baseline on training targets before predicting.")
        length = sum(len(x) for x, _ in dataloader)
        return torch.full((length, 1), self.mean, dtype=torch.float32)
