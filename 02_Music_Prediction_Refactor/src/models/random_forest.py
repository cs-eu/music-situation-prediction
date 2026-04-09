from .base_model import BaseModel
from typing import Dict, Any
from sklearn.ensemble import RandomForestRegressor
import torch
import numpy as np

class RandomForestModel(BaseModel):

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model = RandomForestRegressor(
            n_estimators=self.config.get("n_estimators", 200),
            max_features=self.config.get("max_features", "sqrt"),
            min_samples_leaf=self.config.get("min_samples_leaf", 20),
            random_state=self.config.get("random_state", 42)
        )

    def fit(self, dataloader: torch.utils.data.DataLoader):
        """Fit Random Forest Regressor."""
        X, y = [], []
        for x_batch, y_batch in dataloader:
            X.append(x_batch)
            y.append(y_batch)
        
        X = np.concatenate(X, axis=0)
        y = np.concatenate(y, axis=0)
        self.model.fit(X, y)

    def predict(self, dataloader: torch.utils.data.DataLoader) -> torch.Tensor:
        """Predict using Random Forest Regressor."""
        X = []
        for x_batch, _ in dataloader:
            X.append(x_batch)

        X = np.concatenate(X, axis=0)
        predictions = self.model.predict(X)
        return torch.tensor(predictions, dtype=torch.float32)