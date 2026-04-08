from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import GroupShuffleSplit
import pandas as pd

class DataPreprocessor:
    def __init__(self, config):
        self.config = config
        self.preprocessor = self._build_preprocessor()

    def _build_preprocessor(self):
        """Build scikit-learn preprocessing pipeline."""
        numeric_features = self.config["numeric_features"]
        categorical_features = self.config["categorical_features"]
        binary_features = self.config["binary_features"]
        pass_through_features = self.config["pass_through_features"]

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

    def fit_transform(self, X):
        """Fit and transform the data."""
        return self.preprocessor.fit_transform(X)

    def transform(self, X):
        """Transform the data."""
        return self.preprocessor.transform(X)