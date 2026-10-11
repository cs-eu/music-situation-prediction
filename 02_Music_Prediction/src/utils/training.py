from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype

from data.schema import (
    TARGET_PREFIXES, is_context_column, is_target_column, normalize_column,
)

DEFAULT_TARGET_PREFIXES = TARGET_PREFIXES


def get_config_value(config: Any, key_path: Sequence[str], default: Any = None) -> Any:
    current = config
    for key in key_path:
        if current is None:
            return default

        if hasattr(current, "get"):
            current = current.get(key)
        else:
            current = getattr(current, key, None)

    if current is None:
        return default
    return current


def resolve_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[2] / path


def to_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def get_target_columns(data_frame: pd.DataFrame, config: Any) -> list[str]:
    if not data_frame.columns.is_unique:
        raise ValueError("Data columns must have unique names.")
    target_columns = get_config_value(config, ["target_columns"], default=None)
    if target_columns is not None:
        columns = to_list(target_columns)
        if len(set(columns)) != len(columns):
            raise ValueError("Requested target columns must be unique.")
        for column in columns:
            if column not in data_frame:
                raise ValueError(f"Requested target column {column!r} is missing.")
            if not is_target_column(column):
                raise ValueError(f"Column {column!r} is not a valid music regression target.")
            if not is_numeric_dtype(data_frame[column]):
                raise ValueError(f"Target column {column!r} must be numeric.")
        return columns

    target_prefixes = get_config_value(config, ["target_prefixes"], default=None)
    if not target_prefixes:
        target_prefixes = DEFAULT_TARGET_PREFIXES
    prefixes = tuple(normalize_column(prefix) for prefix in to_list(target_prefixes))

    return [
        column
        for column in data_frame.columns
        if is_target_column(column)
        and is_numeric_dtype(data_frame[column])
        and normalize_column(column).startswith(prefixes)
    ]


def get_feature_columns(
    data_frame: pd.DataFrame,
    target_columns: Sequence[str],
    group_column: str,
    drop_columns: Sequence[str] | None = None,
) -> list[str]:
    target_columns = to_list(target_columns)
    drop_columns = to_list(drop_columns)
    get_target_columns(data_frame, {"target_columns": target_columns})
    if group_column not in data_frame:
        raise ValueError(f"Group column {group_column!r} is missing.")
    excluded_columns = set(target_columns)
    excluded_columns.add(group_column)
    if drop_columns:
        for column in data_frame.columns:
            normalized_column = normalize_column(column)
            if any(
                column == drop_column or normalized_column == normalize_column(drop_column)
                for drop_column in drop_columns
            ):
                excluded_columns.add(column)

    return [
        column for column in data_frame.columns
        if column not in excluded_columns and is_context_column(column)
    ]


def get_group_column(config: Any) -> str:
    return str(
        get_config_value(
            config,
            ["group_column"],
            default=get_config_value(
                config, ["dataset", "groupsplit_column"], default="user_id"
            ),
        )
    )


def log_model_metrics(model_name: str, metrics: dict[str, Any]) -> None:
    try:
        import wandb
    except ImportError:
        return

    if getattr(wandb, "run", None) is None:
        return

    wandb.log(
        {
            f"{model_name}/{metrics.get('target', 'unknown')}/{key}": value
            for key, value in metrics.items()
            if isinstance(value, (int, float))
        }
    )
