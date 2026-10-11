import numpy as np
import torch
from sklearn.linear_model import ElasticNet
from sklearn.preprocessing import StandardScaler

from .base_model import BaseModel


class ElasticNetModel(BaseModel):
    def __init__(
        self,
        config: dict[str, any],
        alpha: float | None = None,
        l1_ratio: float | None = None,
    ):
        super().__init__(config)
        
        self.scaler = StandardScaler()

        resolved_alpha = alpha if alpha is not None else self.config.get("alpha")
        if resolved_alpha is None:
            resolved_alpha = 0.01  # Lowered from 1.0

        resolved_l1_ratio = (
            l1_ratio if l1_ratio is not None else self.config.get("l1_ratio")
        )
        if resolved_l1_ratio is None:
            resolved_l1_ratio = 0.5

        self.model = ElasticNet(
            alpha=resolved_alpha,
            l1_ratio=resolved_l1_ratio,
            max_iter=self.config.get("max_iter", 10000),
            random_state=self.config.get("random_state", 42),
            tol=self.config.get("tol", 1e-4),
            warm_start=self.config.get("warm_start", False),
        )

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit the model with feature scaling."""
        X, y = [], []
        for x_batch, y_batch in dataloader:
            X.append(x_batch.detach().cpu().numpy())
            y.append(y_batch.detach().cpu().numpy())

        X = np.concatenate(X, axis=0)
        y = np.concatenate(y, axis=0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled, y.ravel())

    def predict(self, dataloader, **kwargs) -> torch.Tensor:
        """Predict using the elastic net model with scaled features."""
        X = []
        for x_batch, _ in dataloader:
            X.append(x_batch.detach().cpu().numpy())
        X = np.concatenate(X, axis=0)
        
        # Scale features using the fitted scaler
        X_scaled = self.scaler.transform(X)
        predictions = self.model.predict(X_scaled)
        return torch.tensor(predictions, dtype=torch.float32)
