from __future__ import annotations

from typing import Any, Dict

import numpy as np
import torch

from .base_model import BaseModel


class TabPFNModel(BaseModel):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        try:
            from tabpfn import TabPFNRegressor
        except ImportError as exc:
            raise ImportError(
                "TabPFNModel requires the 'tabpfn' package. Install it to use this model."
            ) from exc

        model_kwargs = dict(self.config.get("model_kwargs", {}))
        self.model = TabPFNRegressor(**model_kwargs)

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit TabPFN regressor on the full training set."""
        features = []
        targets = []

        for x_batch, y_batch in dataloader:
            features.append(x_batch.detach().cpu().numpy())
            targets.append(y_batch.detach().cpu().numpy())

        X = np.concatenate(features, axis=0)
        y = np.concatenate(targets, axis=0).ravel()
        self.model.fit(X, y)

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs) -> torch.Tensor:
        """Predict with TabPFN regressor."""
        features = []

        for x_batch, _ in dataloader:
            features.append(x_batch.detach().cpu().numpy())

        X = np.concatenate(features, axis=0)
        predictions = self.model.predict(X)
        return torch.as_tensor(predictions, dtype=torch.float32).view(-1)