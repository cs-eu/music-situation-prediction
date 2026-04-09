from .base_model import BaseModel
from typing import Dict, Any
from sklearn.linear_model import ElasticNet
import torch
import numpy as np

class ElasticNetModel(BaseModel):

    def __init__(self, config: Dict[str, Any], alpha: int, l1_ratio: float): # TODO: check if I can hand over alpha and l1_ration like this for inherance
        super().__init__(config)
        self.model = ElasticNet(
            alpha=alpha,
            l1_ratio=l1_ratio,
            max_iter=self.config.get("max_iter", 10000),
            random_state=self.config.get("random_state", 42)
        )

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit the model."""
        X, y = [], []
        for x_batch, y_batch in dataloader:
            X.append(x_batch)
            y.append(y_batch)

        X = np.concatenate(X, axis=0)
        y = np.concatenate(y, axis=0)
        self.model.fit(X, y)

    def predict(self, dataloader, **kwargs) -> torch.Tensor:
        """Predict using the elastic net model."""
        X = []
        for x_batch, _ in dataloader:
            X.append(x_batch)
        X = np.concatenate(X, axis=0)
        predictions = self.model.predict(X)
        return torch.tensor(predictions, dtype=torch.float32)
    