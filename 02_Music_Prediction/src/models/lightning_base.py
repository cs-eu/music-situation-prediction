from typing import Any

import torch
import torch.nn.functional as F

try:
    import lightning as L
except ImportError as exc:
    raise ImportError("Neural models require the optional 'neural' extra: pip install -e '.[neural]'.") from exc

from .base_model import BaseModel


class LightningBaseModel(L.LightningModule, BaseModel):
    """MSE training with observation-weighted losses and epoch-level RMSE."""

    def __init__(self, config: dict[str, Any], wandb_logger=None):
        L.LightningModule.__init__(self)
        BaseModel.__init__(self, config, wandb_logger)
        for stage in ("train", "val", "test"):
            self.register_buffer(f"_{stage}_error", torch.zeros(2, dtype=torch.float64), persistent=False)

    def fit(self, dataloader, **kwargs):
        raise NotImplementedError("Use a Lightning trainer with participant-disjoint validation.")

    def predict(self, dataloader, **kwargs):
        trainer = kwargs.get("trainer")
        if trainer is not None:
            predictions = trainer.predict(self, dataloaders=dataloader)
        else:
            was_training = self.training
            self.eval()
            try:
                with torch.inference_mode():
                    predictions = [self(x.to(self.device)).detach().cpu() for x, _ in dataloader]
            finally:
                self.train(was_training)
        if not predictions:
            return torch.empty((0, 1))
        return torch.cat([p.detach().cpu() for p in predictions], dim=0)

    def _regression_step(self, batch, stage):
        x, y = batch
        predictions = self(x)
        if y.ndim == 1:
            y = y.unsqueeze(1)
        if predictions.shape != y.shape:
            raise ValueError("Neural predictions and targets must have matching [N, 1] shapes.")
        loss = F.mse_loss(predictions, y)
        error = getattr(self, f"_{stage}_error")
        error[0] += (predictions.detach().double() - y.detach().double()).square().sum()
        error[1] += y.numel()
        for metric in ("loss", "mse"):
            self.log(f"{stage}_{metric}", loss, on_step=False, on_epoch=True,
                     batch_size=y.numel(), prog_bar=metric == "loss", sync_dist=True)
        return loss

    def training_step(self, batch, batch_idx):
        return self._regression_step(batch, "train")

    def validation_step(self, batch, batch_idx):
        return self._regression_step(batch, "val")

    def test_step(self, batch, batch_idx):
        return self._regression_step(batch, "test")

    def predict_step(self, batch, batch_idx):
        x, _ = batch
        return self(x)

    def _log_epoch_rmse(self, stage):
        error = getattr(self, f"_{stage}_error").clone()
        if torch.distributed.is_available() and torch.distributed.is_initialized():
            torch.distributed.all_reduce(error)
        if error[1] > 0:
            self.log(f"{stage}_rmse", (error[0] / error[1]).sqrt(), prog_bar=True)

    def on_train_epoch_start(self):
        self._train_error.zero_()

    def on_validation_epoch_start(self):
        self._val_error.zero_()

    def on_test_epoch_start(self):
        self._test_error.zero_()

    def on_train_epoch_end(self):
        self._log_epoch_rmse("train")

    def on_validation_epoch_end(self):
        self._log_epoch_rmse("val")

    def on_test_epoch_end(self):
        self._log_epoch_rmse("test")

    def configure_optimizers(self):
        optimizer = torch.optim.AdamW(self.parameters(), lr=self.config.get("learning_rate", 1e-3),
                                      weight_decay=self.config.get("weight_decay", 1e-4))
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="min", factor=0.5, patience=self.config.get("scheduler_patience", 4)
        )
        return {"optimizer": optimizer, "lr_scheduler": {"scheduler": scheduler, "monitor": "val_loss"}}
