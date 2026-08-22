import numpy as np
import torch
from sklearn.linear_model import ElasticNet

from .base_model import BaseModel


class ElasticNetModel(BaseModel):
    def __init__(
        self,
        config: dict[str, any],
        alpha: float | None = None,
        l1_ratio: float | None = None,
    ):
        super().__init__(config)

        resolved_alpha = alpha if alpha is not None else self.config.get("alpha")
        if resolved_alpha is None:
            resolved_alpha = 1.0

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
        )

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit the model."""
        X, y = [], []
        for x_batch, y_batch in dataloader:
            X.append(x_batch.detach().cpu().numpy())
            y.append(y_batch.detach().cpu().numpy())

        X = np.concatenate(X, axis=0)
        y = np.concatenate(y, axis=0)
        self.model.fit(X, y.ravel())

    def predict(self, dataloader, **kwargs) -> torch.Tensor:
        """Predict using the elastic net model."""
        X = []
        for x_batch, _ in dataloader:
            X.append(x_batch.detach().cpu().numpy())
        X = np.concatenate(X, axis=0)
        predictions = self.model.predict(X)
        return torch.tensor(predictions, dtype=torch.float32)
