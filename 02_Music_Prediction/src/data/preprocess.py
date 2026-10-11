from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.impute import MissingIndicator, SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler
from sklearn.utils.validation import check_is_fitted

from .schema import (
    CATEGORICAL_COLUMNS, COUNT_COLUMNS, CYCLIC_COLUMNS, END_COLUMNS,
    GPS_COORDINATES, START_COLUMNS, is_context_column, normalize_column,
)


def load_data(path: str | Path) -> pd.DataFrame:
    """Load observations without imputing, scaling, engineering, or dropping labels."""
    path = Path(path)
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    elif path.suffix.lower() == ".rds":
        try:
            import pyreadr
        except ImportError as exc:
            raise ImportError("Loading raw RDS data requires pyreadr.") from exc
        objects = pyreadr.read_r(str(path))
        if len(objects) != 1:
            raise ValueError("RDS data must contain exactly one data frame.")
        frame = next(iter(objects.values()))
        if not isinstance(frame, pd.DataFrame) and hasattr(frame, "to_pandas"):
            frame = frame.to_pandas()
    else:
        raise ValueError("Data must be an untouched CSV or RDS file.")
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("Training data must be a pandas DataFrame.")
    if not frame.columns.is_unique:
        raise ValueError("Data columns must have unique names.")
    return frame


class ContextFeatures(TransformerMixin, BaseEstimator):
    """Persist deterministic context engineering without fitting population statistics."""

    def __init__(self, drop_features=None):
        self.drop_features = drop_features

    def fit(self, X: pd.DataFrame, y=None):
        if not isinstance(X, pd.DataFrame) or not X.columns.is_unique:
            raise ValueError("Context features require a DataFrame with unique columns.")
        invalid = [column for column in X if not is_context_column(column)]
        if invalid:
            raise ValueError(f"Non-context or outcome feature columns are forbidden: {invalid}")
        self.feature_names_in_ = np.asarray(X.columns, dtype=object)
        self.n_features_in_ = len(self.feature_names_in_)
        engineered = self._engineer(X)
        if not len(engineered.columns):
            raise ValueError("No usable situational context features were supplied.")
        self.feature_names_out_ = np.asarray(engineered.columns, dtype=object)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        check_is_fitted(self, "feature_names_out_")
        if not isinstance(X, pd.DataFrame) or not X.columns.is_unique:
            raise ValueError("Context features require a DataFrame with unique columns.")
        missing = set(self.feature_names_in_) - set(X.columns)
        if missing:
            raise ValueError(f"Missing context columns: {sorted(missing)}")
        return self._engineer(X.loc[:, self.feature_names_in_])

    def get_feature_names_out(self, input_features=None):
        check_is_fitted(self, "feature_names_out_")
        if input_features is not None and list(input_features) != list(self.feature_names_in_):
            raise ValueError("Input feature names differ from the fitted context schema.")
        return self.feature_names_out_.copy()

    def _engineer(self, X: pd.DataFrame) -> pd.DataFrame:
        frame = X.copy()
        names = {normalize_column(column): column for column in frame}
        start_column = next((names[name] for name in START_COLUMNS if name in names), None)
        end_column = next((names[name] for name in END_COLUMNS if name in names), None)
        if start_column is not None:
            start = pd.to_datetime(frame[start_column], errors="coerce", format="mixed")
            hours = start.dt.hour + start.dt.minute / 60 + start.dt.second / 3600
            for name in CYCLIC_COLUMNS:
                if name in names:
                    frame = frame.drop(columns=names[name])
            frame["hour_sin"] = np.sin(2 * np.pi * hours / 24)
            frame["hour_cos"] = np.cos(2 * np.pi * hours / 24)
            frame["dow_sin"] = np.sin(2 * np.pi * start.dt.dayofweek / 7)
            frame["dow_cos"] = np.cos(2 * np.pi * start.dt.dayofweek / 7)
            if end_column is not None:
                end = pd.to_datetime(frame[end_column], errors="coerce", format="mixed")
                for name in ("session_duration_sec", "duration_sec"):
                    if name in names:
                        frame = frame.drop(columns=names[name])
                # This is simultaneous window context, not a future track duration.
                frame["session_duration_sec"] = (end - start).dt.total_seconds()
        frame = frame.drop(columns=[
            column for column in frame
            if normalize_column(column) in START_COLUMNS + END_COLUMNS
        ])
        excluded = {normalize_column(column) for column in (self.drop_features or [])}
        if excluded & {"session_duration_sec", "duration_sec"}:
            excluded.update({"session_duration_sec", "duration_sec"})
        frame = frame.drop(columns=[column for column in frame if normalize_column(column) in excluded])
        for column in frame:
            name = normalize_column(column)
            if name in CATEGORICAL_COLUMNS:
                frame[column] = frame[column].map(
                    lambda value: np.nan if pd.isna(value) else str(value)
                ).astype(object)
                continue
            try:
                values = pd.to_numeric(frame[column], errors="raise").astype(float)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Context column {column!r} must contain numeric values.") from exc
            values = values.replace([np.inf, -np.inf], np.nan)
            # R's NA can be a signaling NaN; replace it with an ordinary numpy NaN.
            values = values.mask(values.isna(), np.nan)
            # Legacy num__ inputs are already scaled and must not be logged again.
            if name in COUNT_COLUMNS and column == name:
                if values.lt(0).any():
                    raise ValueError(f"Count/duration context {column!r} cannot be negative.")
                values = np.log1p(values)
            frame[column] = values
        return frame


def _gps_columns(frame):
    return [column for column in frame if normalize_column(column) in GPS_COORDINATES]


def _string_categories(values):
    # An all-missing training category is imputed to 0 by SimpleImputer; normalize
    # that fallback as well so unseen string values remain safe at inference.
    return np.asarray(values).astype(str)


def _finite_array(values):
    values = np.asarray(values, dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("Context preprocessing produced nonfinite numeric values.")
    return values


def build_preprocessor(feature_frame: pd.DataFrame, drop_features=None) -> Pipeline:
    """Build an unfitted, dense transformer. Fit only after participant splitting."""
    if not isinstance(feature_frame, pd.DataFrame) or not feature_frame.columns.is_unique:
        raise ValueError("Context features require a DataFrame with unique columns.")
    invalid = [column for column in feature_frame if not is_context_column(column)]
    if invalid:
        raise ValueError(f"Non-context or outcome feature columns are forbidden: {invalid}")
    numeric = Pipeline([
        ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
        ("scaler", StandardScaler()),
    ])
    categorical = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent", keep_empty_features=True)),
        ("strings", FunctionTransformer(_string_categories, feature_names_out="one-to-one")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    columns = ColumnTransformer([
        ("num", numeric, make_column_selector(dtype_include=np.number)),
        ("cat", categorical, make_column_selector(dtype_exclude=np.number)),
        ("gps_missing", MissingIndicator(features="all"), _gps_columns),
    ], remainder="drop", sparse_threshold=0)
    return Pipeline([
        ("context", ContextFeatures(drop_features=drop_features)),
        ("columns", columns),
        ("finite", FunctionTransformer(_finite_array, feature_names_out="one-to-one")),
    ])


class DataPreprocessor(TransformerMixin, BaseEstimator):
    """Compatibility wrapper that restricts old full-frame callers to safe context."""

    def __init__(self, config):
        self.config = config

    def fit(self, X: pd.DataFrame, y=None):
        drops = self.config.get("dataset", {}).get("drop_features", [])
        excluded = {normalize_column(column) for column in drops}
        self.feature_columns_ = [
            column for column in X
            if is_context_column(column) and normalize_column(column) not in excluded
        ]
        self.preprocessor = build_preprocessor(X[self.feature_columns_], drop_features=drops)
        self.preprocessor.fit(X[self.feature_columns_])
        return self

    def transform(self, X: pd.DataFrame):
        check_is_fitted(self, "feature_columns_")
        return self.preprocessor.transform(X[self.feature_columns_])

    def get_feature_names_out(self, input_features=None):
        check_is_fitted(self, "feature_columns_")
        return self.preprocessor.get_feature_names_out()
