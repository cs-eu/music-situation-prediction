import torch
import torch.nn as nn
import lightning as L
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple
from metrics import calculate_metrics

class BaseModel(ABC):
    """Abstract base class for all models."""

    def __init__(self, config: Dict[str, Any], wandb_logger=None):
        self.config = config
        self.wandb_logger = wandb_logger

    @abstractmethod
    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit model to training data."""
        pass

    @abstractmethod
    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs) -> torch.Tensor:
        """Do predictions."""
        pass

    def evaluate(self, dataloader: torch.utils.data.DataLoader) -> Dict[str, float]:
        """Evaluate model with RMSE and R2 scores."""
        predictions = self.predict(dataloader)
        targets = torch.cat([y for _, y in dataloader], dim=0) # TODO: check if this is correct and what it does
        return calculate_metrics(predictions, targets)
    
    @classmethod
    def load(cls, path: str, config: Dict[str, Any]):
        """Load model from a file."""
        model = cls(config)
        if hasattr(model, 'load_state_dict'): # TODO: implement this
            model.load_state_dict(torch.load(path))
        else:
            torch.load(path)
        return model
    

class LightningBaseModel(L.LightningModule, BaseModel):

    def __init__(self, config: Dict[str, Any], wandb_logger=None):
        L.LightningModule.__init__(self, wandb_logger)
        BaseModel.__init__(self, config)

    def training_step(self, batch: Tuple[torch.Tensor, torch.Tensor], batch_idx: int) -> torch.Tensor:
        """Training Step for PyTorch Lightning."""
        x, y = batch
        y_hat = self(x) # TODO: check if this is correct and what does it actually do
        loss = nn.functional.mse_loss(y_hat, y)
        self.wandb_logger.log_metrics({"train_loss": loss}, step=self.global_step)
        return loss
    
    def validation_step(self, batch: Tuple[torch.Tensor, torch.Tensor], batch_idx: int) -> torch.Tensor:
        """Validation Step for PyTorchLightning."""
        x, y = batch
        y_hat = self(x)
        loss = nn.functional.mse_loss(y_hat, y)
        self.wandb_logger.log_metrics({"val_loss": loss}, step=self.global_step)
        return loss
    
    def test_step(self, batch: Tuple[torch.Tensor, torch.Tensor], batch_idx: int) -> torch.Tensor:
        """Test Step for PyTorch Lightning."""
        x, y = batch
        y_hat = self(x)
        loss = nn.functional.mse_loss(y_hat, y)
        self.wandb_logger.log_metrics({"test_loss": loss}, step=self.global_step)
        return loss
    
    def predict_step(self, batch: Tuple[torch.Tensor, torch.Tensor], batch_idx: int):
        """Predict Step for PyTorch Lightning."""
        x, _ = batch
        return self(x)
    
    def configure_optimizers(self):
        """Configure Optimizers for PyTorch Lightning."""
        return torch.optim.Adam(self.parameters, lr=self.config.get("learning_rate", 1e-3), weight_decay=self.config.get("weight_decay", 1e-5))