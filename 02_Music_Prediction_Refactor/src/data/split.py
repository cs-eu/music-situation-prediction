from sklearn.model_selection import GroupShuffleSplit
import pandas as pd

class DataSplitter:
    def __init__(self, test_size=0.2, random_state=42):
        self.test_size = test_size
        self.random_state = random_state

    def group_split(self, df : pd.DataFrame, group_column : str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split dataset into train and test set and block by group_column."""
        groups = df[group_column].values
        splitter = GroupShuffleSplit(
            n_splits=1,
            test_size=self.test_size,
            random_state=self.random_state
        )

        train_idx, test_idx = next(splitter.split(df, groups=groups))

        return df.iloc[train_idx], df.iloc[test_idx]
    
