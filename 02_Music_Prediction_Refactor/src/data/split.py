from __future__ import annotations

import pandas as pd
from sklearn.model_selection import GroupShuffleSplit


class DataSplitter:
    def __init__(
        self, test_size: float = 0.2, val_size: float = 0.0, random_state: int = 42
    ):
        self.test_size = test_size
        self.val_size = val_size
        self.random_state = random_state

    def group_split(
        self, df: pd.DataFrame, group_column: str
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Split dataset into train and test set and block by group_column."""
        train_set, test_set = self._split_frame(df, group_column, self.test_size)
        val_set = None

        if self.val_size > 0:
            remaining_fraction = 1.0 - self.test_size
            if remaining_fraction <= 0:
                raise ValueError(
                    "test_size must be smaller than 1.0 when val_size is set."
                )

            validation_fraction = self.val_size / remaining_fraction
            if validation_fraction >= 1.0:
                raise ValueError(
                    "val_size leaves no room for a training split after removing the test set."
                )

            train_set, val_set = self._split_frame(
                train_set, group_column, validation_fraction
            )

        return (
            train_set.reset_index(drop=True),
            None if val_set is None else val_set.reset_index(drop=True),
            test_set.reset_index(drop=True),
        )

    def _split_frame(
        self, df: pd.DataFrame, group_column: str, test_size: float
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        groups = df[group_column].values
        splitter = GroupShuffleSplit(
            n_splits=1, test_size=test_size, random_state=self.random_state
        )

        train_idx, test_idx = next(splitter.split(df, groups=groups))
        return df.iloc[train_idx], df.iloc[test_idx]
