from typing import Any

import torch
from sklearn.ensemble import RandomForestRegressor

from .base_model import BaseModel


class RandomForestModel(BaseModel):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.model = RandomForestRegressor(
            n_estimators=self.config.get("n_estimators", 300),
            max_depth=self.config.get("max_depth", 16),
            max_features=self.config.get("max_features", 0.7),
            min_samples_leaf=self.config.get("min_samples_leaf", 20),
            min_samples_split=self.config.get("min_samples_split", 2),
            random_state=self.config.get("random_state", 42),
            n_jobs=self.config.get("n_jobs", 4),
            verbose=self.config.get("verbose", 0),
            warm_start=self.config.get("warm_start", False),
        )

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit Random Forest Regressor."""
        X, y = self._loader_arrays(dataloader)
        self.model.fit(X, y)
        return self

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs) -> torch.Tensor:
        """Predict using Random Forest Regressor."""
        X, _ = self._loader_arrays(dataloader, require_targets=False)
        return torch.as_tensor(self.model.predict(X), dtype=torch.float32).reshape(-1, 1)
