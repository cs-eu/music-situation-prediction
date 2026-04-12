from sklearn.model_selection import GroupShuffleSplit
import pandas as pd

class DataSplitter:
    def __init__(self, test_size:float=0.2, val_size:float=0.0, random_state:int=42):
        self.test_size = test_size
        self.val_size = val_size
        self.random_state = random_state

    def group_split(self, df : pd.DataFrame, group_column : str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split dataset into train and test set and block by group_column."""
        groups = df[group_column].values
        test_splitter = GroupShuffleSplit(
            n_splits=1,
            test_size=self.test_size,
            random_state=self.random_state
        )

        train_idx, test_idx = next(test_splitter.split(df, groups=groups))
        train_set, test_set = df.iloc[train_idx], df.iloc[test_idx]
        val_set = None

        if self.val_size > 0:
            val_splitter = GroupShuffleSplit(
                n_splits=1,
                test_size=(len(df) / len(train_set)) * self.val_size, # weight with original size
                random_state=self.random_state,
            )

            train_idx, val_idx = next(val_splitter.split(df, groups=groups))
            train_set, val_set = df.iloc[train_idx], df.iloc[val_idx]

        return train_set, val_set, test_set
    
