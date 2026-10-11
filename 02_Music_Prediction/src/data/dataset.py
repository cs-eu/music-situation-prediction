from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
import torch
from torch.utils.data import DataLoader, Dataset

from .schema import is_context_column, is_outcome_column, is_target_column


class SituationDataset(Dataset):
    """Dataset class for music situation prediction."""

    def __init__(self, data: pd.DataFrame, config: dict[str, Any]):
        target_columns = config.get("target_columns")
        if target_columns is None:
            raise ValueError("SituationDataset requires 'target_columns' in config.")

        if isinstance(target_columns, str):
            target_columns = [target_columns]
        target_columns = list(target_columns)
        if not target_columns or len(set(target_columns)) != len(target_columns):
            raise ValueError("SituationDataset requires unique, nonempty target_columns.")
        if not data.columns.is_unique:
            raise ValueError("Data columns must have unique names.")
        for column in target_columns:
            if column not in data:
                raise ValueError(f"Target column {column!r} is missing.")
            if not is_target_column(column):
                raise ValueError(f"Column {column!r} is not a valid music regression target.")

        feature_columns = config.get("feature_columns")
        group_column = config.get("group_column", "user_id")
        if feature_columns is None:
            feature_columns = [
                column for column in data.columns
                if column != group_column and is_context_column(column)
            ]
        elif isinstance(feature_columns, str):
            feature_columns = [feature_columns]
        feature_columns = list(feature_columns)
        if not feature_columns or len(set(feature_columns)) != len(feature_columns):
            raise ValueError("SituationDataset requires unique, nonempty feature_columns.")
        for column in feature_columns:
            if column not in data:
                raise ValueError(f"Feature column {column!r} is missing.")
            if column == group_column or is_outcome_column(column) or not is_context_column(column):
                raise ValueError(f"Non-context or outcome feature {column!r} is forbidden.")
        for column in feature_columns + target_columns:
            if not is_numeric_dtype(data[column]):
                raise ValueError(f"Dataloader column {column!r} must already be numeric.")

        features = data[feature_columns].to_numpy(dtype=np.float32, na_value=np.nan)
        targets = data[target_columns].to_numpy(dtype=np.float32, na_value=np.nan)
        if not np.isfinite(features).all() or not np.isfinite(targets).all():
            raise ValueError("Dataloader X/y must be finite; preprocess X and filter unobserved y first.")
        self.X = torch.from_numpy(features)
        self.y = torch.from_numpy(targets)

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
