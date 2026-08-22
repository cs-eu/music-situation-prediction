from abc import ABC, abstractmethod
from typing import Any

import lightning as L
import torch
import torch.nn as nn
from utils import calculate_metrics


class BaseModel(ABC):
    """Abstract base class for all models."""

    def __init__(self, config: dict[str, Any], wandb_logger=None):
        self.config = config
        self.wandb_logger = wandb_logger

    @abstractmethod
    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Fit model to training data."""
        raise NotImplementedError

    @abstractmethod
    def predict(
        self, dataloader: torch.utils.data.DataLoader, **kwargs
    ) -> torch.Tensor:
        """Do predictions."""
        raise NotImplementedError

    def evaluate(self, dataloader: torch.utils.data.DataLoader) -> dict[str, float]:
        """Evaluate model with RMSE and R2 scores."""
        predictions = self.predict(dataloader)
        targets = torch.cat(
            [y for _, y in dataloader], dim=0
        )  # TODO: check if this is correct and what it does
        return calculate_metrics(predictions, targets)

    @classmethod
    def load(cls, path: str, config: dict[str, Any]):
        """Load model from a file."""
        model = cls(config)
        if hasattr(model, "load_state_dict"):  # TODO: implement this
            model.load_state_dict(torch.load(path))
        else:
            torch.load(path)
        return model


class LightningBaseModel(L.LightningModule, BaseModel):
    def __init__(self, config: dict[str, Any], wandb_logger=None):
        L.LightningModule.__init__(self)
        BaseModel.__init__(self, config, wandb_logger)

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        raise NotImplementedError("Use the trainer to fit Lightning models.")

    def predict(
        self, dataloader: torch.utils.data.DataLoader, **kwargs
    ) -> torch.Tensor:
        trainer = kwargs.get("trainer")
        if trainer is None:
            trainer = L.Trainer(
                logger=False,
                enable_checkpointing=False,
                enable_progress_bar=False,
                accelerator="auto",
                devices="auto",
            )

        predictions = trainer.predict(self, dataloaders=dataloader)
        if not predictions:
            return torch.empty(0)
        return torch.cat(
            [prediction.detach().cpu() for prediction in predictions], dim=0
        )

    def training_step(
        self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int
    ) -> torch.Tensor:
        """Training Step for PyTorch Lightning."""
        x, y = batch
        y_hat = self(x)  # TODO: check if this is correct and what does it actually do
        loss = nn.functional.mse_loss(y_hat, y)
        self.log("train_loss", loss, on_step=True, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(
        self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int
    ) -> torch.Tensor:
        """Validation Step for PyTorchLightning."""
        x, y = batch
        y_hat = self(x)
        loss = nn.functional.mse_loss(y_hat, y)
        self.log("val_loss", loss, on_step=False, on_epoch=True, prog_bar=True)
        return loss

    def test_step(
        self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int
    ) -> torch.Tensor:
        """Test Step for PyTorch Lightning."""
        x, y = batch
        y_hat = self(x)
        loss = nn.functional.mse_loss(y_hat, y)
        self.log("test_loss", loss, on_step=False, on_epoch=True, prog_bar=True)
        return loss

    def predict_step(self, batch: tuple[torch.Tensor, torch.Tensor], batch_idx: int):
        """Predict Step for PyTorch Lightning."""
        x, _ = batch
        return self(x)

    def configure_optimizers(self):
        """Configure Optimizers for PyTorch Lightning."""
        return torch.optim.Adam(
            self.parameters(),
            lr=self.config.get("learning_rate", 1e-3),
            weight_decay=self.config.get("weight_decay", 1e-5),
        )
