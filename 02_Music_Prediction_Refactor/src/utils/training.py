from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_TARGET_PREFIXES = (
    "num__liwc_",
    "num__topic_",
    "num__genius_",
    "num__music_track_",
)


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
    return Path.cwd() / path


def to_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def get_target_columns(data_frame: pd.DataFrame, config: Any) -> list[str]:
    target_columns = get_config_value(config, ["target_columns"], default=None)
    if target_columns:
        return to_list(target_columns)

    target_prefixes = get_config_value(config, ["target_prefixes"], default=None)
    if not target_prefixes:
        target_prefixes = DEFAULT_TARGET_PREFIXES

    return [
        column
        for column in data_frame.columns
        if column.startswith(tuple(target_prefixes))
    ]


def get_feature_columns(
    data_frame: pd.DataFrame,
    target_columns: Sequence[str],
    group_column: str,
    drop_columns: Sequence[str] | None = None,
) -> list[str]:
    excluded_columns = set(target_columns)
    excluded_columns.add(group_column)
    if drop_columns:
        for column in data_frame.columns:
            normalized_column = column.split("__", 1)[-1]
            if any(
                column == drop_column or normalized_column == drop_column
                for drop_column in drop_columns
            ):
                excluded_columns.add(column)

    return [column for column in data_frame.columns if column not in excluded_columns]


def get_group_column(config: Any) -> str:
    return str(
        get_config_value(
            config,
            ["group_column"],
            default=get_config_value(
                config, ["dataset", "groupsplit_column"], default="pass__user_id"
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
            f"{model_name}/{key}": value
            for key, value in metrics.items()
            if key != "target"
        }
    )