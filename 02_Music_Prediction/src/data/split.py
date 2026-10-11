from __future__ import annotations

from math import ceil
from numbers import Real

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupShuffleSplit


class DataSplitter:
    def __init__(
        self, test_size: float = 0.2, val_size: float = 0.0, random_state: int = 42
    ):
        for name, fraction in (("test_size", test_size), ("val_size", val_size)):
            if isinstance(fraction, bool) or not isinstance(fraction, Real) or not np.isfinite(fraction):
                raise ValueError(f"{name} must be a finite fraction.")
        if not 0 < test_size < 1:
            raise ValueError("test_size must be strictly between 0 and 1.")
        if not 0 <= val_size < 1 or test_size + val_size >= 1:
            raise ValueError("val_size must be nonnegative and leave a training fraction.")
        self.test_size = float(test_size)
        self.val_size = float(val_size)
        self.random_state = random_state

    def group_split(
        self, df: pd.DataFrame, group_column: str = "user_id"
    ) -> tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame]:
        """Hold out participants; both fractions refer to the original population."""
        if not df.columns.is_unique or group_column not in df:
            raise ValueError(f"A unique group column {group_column!r} is required.")
        groups = df[group_column]
        if df.empty or groups.isna().any():
            raise ValueError("Participant IDs must be observed for every row of nonempty data.")
        try:
            unique_ids = groups.unique()
        except TypeError as exc:
            raise ValueError("Participant IDs must be hashable scalar values.") from exc
        for value in unique_ids:
            if isinstance(value, str) and not value.strip():
                raise ValueError("Participant IDs cannot be empty strings.")
            if isinstance(value, Real) and not np.isfinite(value):
                raise ValueError("Participant IDs must be finite.")
        codes, unique_groups = pd.factorize(groups, sort=True)
        count = len(unique_groups)
        n_test = ceil(self.test_size * count)
        n_val = ceil(self.val_size * count) if self.val_size else 0
        if count - n_test - n_val < 1:
            raise ValueError("Too few unique participants for nonempty requested splits.")
        splitter = GroupShuffleSplit(n_splits=1, test_size=n_test, random_state=self.random_state)
        train_ids, test_ids = next(splitter.split(df, groups=codes))
        val_ids = None
        if n_val:
            splitter = GroupShuffleSplit(n_splits=1, test_size=n_val, random_state=self.random_state)
            remaining_ids, relative_val_ids = next(splitter.split(df.iloc[train_ids], groups=codes[train_ids]))
            val_ids, train_ids = train_ids[relative_val_ids], train_ids[remaining_ids]
        partitions = [train_ids, test_ids] + ([] if val_ids is None else [val_ids])
        for i, left in enumerate(partitions):
            assert len(left), "Requested participant splits must be nonempty."
            for right in partitions[i + 1:]:
                assert not set(codes[left]) & set(codes[right]), "Participant leakage between splits."
                assert not set(left) & set(right), "Rows overlap between splits."
        assert sum(map(len, partitions)) == len(df), "Splitting must retain every row."
        return (
            df.iloc[train_ids].reset_index(drop=True).copy(),
            None if val_ids is None else df.iloc[val_ids].reset_index(drop=True).copy(),
            df.iloc[test_ids].reset_index(drop=True).copy(),
        )
