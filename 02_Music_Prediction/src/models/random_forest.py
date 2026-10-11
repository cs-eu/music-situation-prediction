import numpy as np
import torch
from sklearn.ensemble import RandomForestRegressor

from .base_model import BaseModel


class RandomForestModel(BaseModel):
    def __init__(self, config: dict[str, any]):
        super().__init__(config)
        self.model = RandomForestRegressor(
            n_estimators=self.config.get("n_estimators", 200),
            max_depth=self.config.get("max_depth", None),
            max_features=self.config.get("max_features", "sqrt"),
            min_samples_leaf=self.config.get("min_samples_leaf", 20),
            min_samples_split=self.config.get("min_samples_split", 2),
            random_state=self.config.get("random_state", 42),
            n_jobs=self.config.get("n_jobs", -1),
            verbose=self.config.get("verbose", 0),
            warm_start=self.config.get("warm_start", False),
        )

    def fit(self, dataloader: torch.utils.data.DataLoader):
        """Fit Random Forest Regressor."""
        X, y = [], []
        for x_batch, y_batch in dataloader:
            X.append(x_batch.detach().cpu().numpy())
            y.append(y_batch.detach().cpu().numpy())

        X = np.concatenate(X, axis=0)
        y = np.concatenate(y, axis=0)
        self.model.fit(X, y.ravel())

    def predict(self, dataloader: torch.utils.data.DataLoader) -> torch.Tensor:
        """Predict using Random Forest Regressor."""
        X = []
        for x_batch, _ in dataloader:
            X.append(x_batch.detach().cpu().numpy())

        X = np.concatenate(X, axis=0)
        predictions = self.model.predict(X)
        return torch.tensor(predictions, dtype=torch.float32)
