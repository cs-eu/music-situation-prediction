from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import GroupShuffleSplit
import pandas as pd
import numpy as np

class DataPreprocessor:
    def __init__(self, config):
        self.config = config
        self.preprocessor = self._build_preprocessor()

    def _build_preprocessor(self):
        """Build scikit-learn preprocessing pipeline."""
        numeric_features = self.config["dataset"]["numeric_features"]
        categorical_features = self.config["dataset"]["categorical_features"]
        binary_features = self.config["dataset"]["binary_features"]
        pass_through_features = self.config["dataset"]["pass_through_features"]

        numeric_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler())
            ]
        )
        categorical_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
            ]
        )
        binary_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="most_frequent"))
            ]
        )

        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_pipeline, numeric_features),
                ("cat", categorical_pipeline, categorical_features),
                ("bin", binary_pipeline, binary_features),
                ("pass", "passthrough", pass_through_features)
            ]
        )
        return preprocessor
    
    def _remove_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """Remove features specified in the config."""
        drop_features = self.config["dataset"]["drop_features"]
        return X.drop(columns=drop_features)

    def fit_transform(self, X: pd.DataFrame) -> np.ndarray:
        """Fit and transform the data."""
        # print("BEFORE")
        # print(X)
        # print("AFTER")
        # print(X)
        X = self._remove_features(X)
        return self.preprocessor.fit_transform(X)

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        """Transform the data."""
        X = self._remove_features(X)
        return self.preprocessor.transform(X)