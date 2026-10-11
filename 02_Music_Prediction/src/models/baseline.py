import torch

from .base_model import BaseModel


class BaselineModel(BaseModel):
    def __init__(self, config: dict[str, any] | None = None):
        super().__init__(config or {})
        self.mean = None

    def fit(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Baseline is mean of training set."""
        targets = []
        for _, y in dataloader:
            targets.append(y)
        self.mean = torch.cat(targets, dim=0).mean(dim=0)

    def predict(self, dataloader: torch.utils.data.DataLoader, **kwargs):
        """Predict mean for all samples."""
        length = len(dataloader.dataset)
        return self.mean.unsqueeze(0).repeat(length, 1)
