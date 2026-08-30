import numpy as np
import torch
import lightgbm as lgb

from .base_model import BaseModel


class LightGBMModel(BaseModel):
    def __init__(self, config: dict[str, any]):
        super().__init__(config)
        
        # LightGBM parameters
        self.params = {
            'objective': 'regression',
            'metric': 'mse',
            'boosting_type': self.config.get("boosting_type", "gbdt"),
            'num_leaves': self.config.get("num_leaves", 31),
            'learning_rate': self.config.get("learning_rate", 0.05),
            'feature_fraction': self.config.get("feature_fraction", 0.8),
            'bagging_fraction': self.config.get("bagging_fraction", 0.8),
            'bagging_freq': self.config.get("bagging_freq", 5),
            'lambda_l1': self.config.get("lambda_l1", 0.0),
            'lambda_l2': self.config.get("lambda_l2", 0.0),
            'verbose': self.config.get("verbose", -1),
            'random_state': self.config.get("random_state", 42),
        }
        
        self.num_rounds = self.config.get("num_rounds", 100)
        self.model = None

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit LightGBM model."""
        X, y = [], []
        for x_batch, y_batch in dataloader:
            X.append(x_batch.detach().cpu().numpy())
            y.append(y_batch.detach().cpu().numpy())

        X = np.concatenate(X, axis=0)
        y = np.concatenate(y, axis=0).ravel()
        
        # Create LightGBM dataset
        train_data = lgb.Dataset(X, label=y)
        
        # Train model
        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=self.num_rounds,
            callbacks=[lgb.log_evaluation(period=0)],
        )

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs) -> torch.Tensor:
        """Predict using LightGBM model."""
        X = []
        for x_batch, _ in dataloader:
            X.append(x_batch.detach().cpu().numpy())

        X = np.concatenate(X, axis=0)
        predictions = self.model.predict(X)
        return torch.tensor(predictions, dtype=torch.float32)
