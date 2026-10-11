from typing import Any

import torch
from sklearn.linear_model import ElasticNet
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .base_model import BaseModel


class ElasticNetModel(BaseModel):
    def __init__(
        self,
        config: dict[str, Any],
        alpha: float | None = None,
        l1_ratio: float | None = None,
    ):
        super().__init__(config)
        resolved_alpha = alpha if alpha is not None else self.config.get("alpha")
        if resolved_alpha is None:
            resolved_alpha = 0.01

        resolved_l1_ratio = (
            l1_ratio if l1_ratio is not None else self.config.get("l1_ratio")
        )
        if resolved_l1_ratio is None:
            resolved_l1_ratio = 0.5

        estimator = ElasticNet(
            alpha=resolved_alpha,
            l1_ratio=resolved_l1_ratio,
            max_iter=self.config.get("max_iter", 10000),
            random_state=self.config.get("random_state", 42),
            tol=self.config.get("tol", 1e-4),
            warm_start=self.config.get("warm_start", False),
        )
        self.model = Pipeline([
            ("scaler", StandardScaler() if self.config.get("standardize", True) else "passthrough"),
            ("regressor", estimator),
        ])

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Keep fitted scaling and coefficients in a single saved estimator."""
        X, y = self._loader_arrays(dataloader)
        self.model.fit(X, y)
        return self

    def predict(self, dataloader, **kwargs) -> torch.Tensor:
        """Predict using the elastic net model with scaled features."""
        X, _ = self._loader_arrays(dataloader, require_targets=False)
        return torch.as_tensor(self.model.predict(X), dtype=torch.float32).reshape(-1, 1)
