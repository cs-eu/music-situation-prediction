from __future__ import annotations

from typing import Any

import pandas as pd
import torch
from torch.utils.data import DataLoader, Dataset


class SituationDataset(Dataset):
    """Dataset class for music situation prediction."""

    def __init__(self, data: pd.DataFrame, config: dict[str, Any]):
        target_columns = config.get("target_columns")
        if target_columns is None:
            raise ValueError("SituationDataset requires 'target_columns' in config.")

        if isinstance(target_columns, str):
            target_columns = [target_columns]

        feature_columns = config.get("feature_columns")
        if feature_columns is None:
            excluded_columns = set(target_columns)
            group_column = config.get("group_column")
            if group_column:
                excluded_columns.add(group_column)
            feature_columns = [
                column for column in data.columns if column not in excluded_columns
            ]

        self.X = torch.tensor(data[feature_columns].values, dtype=torch.float32)
        self.y = torch.tensor(data[target_columns].values, dtype=torch.float32)

    def __getitem__(self, index: int):
        return self.X[index], self.y[index]

    def __len__(self):
        return len(self.y)


def get_dataloader(
    data: pd.DataFrame,
    config: dict[str, Any],
    batch_size: int = 32,
    shuffle: bool = True,
    num_workers: int = 4,
) -> DataLoader:
    """Get dataloader for the dataset."""
    dataset = SituationDataset(data, config)
    return DataLoader(
        dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers
    )
