from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd
import torch
from lightning.pytorch import Trainer as LightningTrainer
from sklearn.model_selection import GroupShuffleSplit
from sklearn.preprocessing import StandardScaler

from data.dataset import SituationDataset
from utils.metrics import calculate_metrics


class BaseTrainer:

    def __init__(self, config):
        self.config = config
        self.model = None

    def train(self):
        data_frame = self._load_data()
        target_columns = self._get_target_columns(data_frame)
        if not target_columns:
            raise ValueError("No target columns were found in the training data.")

        feature_columns = self._get_feature_columns(data_frame, target_columns)
        train_frame, val_frame, test_frame = self._split_data(data_frame)

        results = []
        for target_column in target_columns:
            metrics = self._train_single_target(
                train_frame=train_frame,
                val_frame=val_frame,
                test_frame=test_frame,
                feature_columns=feature_columns,
                target_column=target_column,
            )
            results.append(metrics)

        return results

    def _load_data(self) -> pd.DataFrame:
        data_path = self._resolve_path(self._get_config_value(["data_path"], default="data/preprocessed_features.csv"))
        if not data_path.exists():
            raise FileNotFoundError(f"Training data not found at {data_path}")
        return pd.read_csv(data_path)

    def _get_target_columns(self, data_frame: pd.DataFrame) -> List[str]:
        target_columns = self._get_config_value(["target_columns"], default=None)
        if target_columns:
            if isinstance(target_columns, str):
                return [target_columns]
            return list(target_columns)

        target_prefixes = self._get_config_value(["target_prefixes"], default=None)
        if not target_prefixes:
            target_prefixes = ("num__liwc_", "num__topic_", "num__genius_", "num__music_track_")

        return [column for column in data_frame.columns if column.startswith(tuple(target_prefixes))]

    def _get_feature_columns(self, data_frame: pd.DataFrame, target_columns: Sequence[str]) -> List[str]:
        group_column = self._get_group_column()
        excluded_columns = set(target_columns)
        excluded_columns.add(group_column)
        return [column for column in data_frame.columns if column not in excluded_columns]

    def _split_data(self, data_frame: pd.DataFrame):
        group_column = self._get_group_column()
        test_size = float(self._get_config_value(["test_size"], default=0.1))
        val_size = float(self._get_config_value(["val_size"], default=0.0))
        random_state = int(self._get_config_value(["random_state"], default=42))

        groups = data_frame[group_column].values
        splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
        train_indices, test_indices = next(splitter.split(data_frame, groups=groups))

        train_frame = data_frame.iloc[train_indices].reset_index(drop=True)
        test_frame = data_frame.iloc[test_indices].reset_index(drop=True)
        val_frame = None

        if val_size > 0:
            val_splitter = GroupShuffleSplit(n_splits=1, test_size=val_size, random_state=random_state)
            train_indices, val_indices = next(val_splitter.split(train_frame, groups=train_frame[group_column].values))
            val_frame = train_frame.iloc[val_indices].reset_index(drop=True)
            train_frame = train_frame.iloc[train_indices].reset_index(drop=True)

        return train_frame, val_frame, test_frame

    def _train_single_target(
        self,
        train_frame: pd.DataFrame,
        val_frame: pd.DataFrame | None,
        test_frame: pd.DataFrame,
        feature_columns: Sequence[str],
        target_column: str,
    ) -> Dict[str, Any]:
        model_name = self._get_model_name()
        model_config = self._get_model_config()

        train_frame, val_frame, test_frame = self._scale_frames(train_frame, val_frame, test_frame, feature_columns)

        train_loader = self._build_dataloader(train_frame, feature_columns, target_column, shuffle=True)
        val_loader = self._build_dataloader(val_frame, feature_columns, target_column, shuffle=False) if val_frame is not None else None
        test_loader = self._build_dataloader(test_frame, feature_columns, target_column, shuffle=False)

        model = self._build_model(model_name, model_config, input_dim=len(feature_columns))
        self.model = model

        if self._is_lightning_model(model):
            lightning_trainer = self._build_lightning_trainer()
            if val_loader is None:
                lightning_trainer.fit(model, train_dataloaders=train_loader)
            else:
                lightning_trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)
            prediction_batches = lightning_trainer.predict(model, dataloaders=test_loader)
            prediction_tensor = torch.cat([batch.detach().cpu() for batch in prediction_batches], dim=0) if prediction_batches else torch.empty(0)
        else:
            model.fit(train_loader)
            prediction_tensor = model.predict(test_loader)

        prediction_tensor = prediction_tensor.view(-1)
        target_tensor = torch.cat([batch_targets for _, batch_targets in test_loader], dim=0).view(-1)
        metrics = calculate_metrics(prediction_tensor, target_tensor)
        metrics = {
            key: float(value.detach().cpu().item() if torch.is_tensor(value) else value)
            for key, value in metrics.items()
        }
        metrics["target"] = target_column

        self._log_metrics(model_name, metrics)
        self._save_model(model, model_name, target_column)
        return metrics

    def _scale_frames(
        self,
        train_frame: pd.DataFrame,
        val_frame: pd.DataFrame | None,
        test_frame: pd.DataFrame,
        feature_columns: Sequence[str],
    ):
        scaler = StandardScaler()
        train_frame = train_frame.copy()
        test_frame = test_frame.copy()
        train_frame.loc[:, feature_columns] = scaler.fit_transform(train_frame[feature_columns])
        test_frame.loc[:, feature_columns] = scaler.transform(test_frame[feature_columns])

        if val_frame is not None:
            val_frame = val_frame.copy()
            val_frame.loc[:, feature_columns] = scaler.transform(val_frame[feature_columns])

        return train_frame, val_frame, test_frame

    def _build_dataloader(
        self,
        frame: pd.DataFrame,
        feature_columns: Sequence[str],
        target_column: str,
        shuffle: bool,
    ):
        dataset_config = {
            "feature_columns": list(feature_columns),
            "target_columns": [target_column],
            "group_column": self._get_group_column(),
        }
        dataset = SituationDataset(frame, dataset_config)
        batch_size = int(self._get_config_value(["batch_size"], default=32))
        num_workers = int(self._get_config_value(["num_workers"], default=0))
        return torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)

    def _build_model(self, model_name: str, model_config: Dict[str, Any], input_dim: int):
        model_class = self._resolve_model_class(model_name)
        merged_config = dict(model_config)
        merged_config.setdefault("input_dim", input_dim)
        merged_config.setdefault("output_dim", 1)

        if model_name == "baseline":
            return model_class(merged_config)

        return model_class(merged_config)

    def _resolve_model_class(self, model_name: str):
        registry = {
            "baseline": ("models.baseline", "BaselineModel"),
            "elastic_net": ("models.elastic_net", "ElasticNetModel"),
            "random_forest": ("models.random_forest", "RandomForestModel"),
            "tabpfn": ("models.tabpfn", "TabPFNModel"),
            "neural_net": ("models.neural_net", "NeuralNet"),
            "transformer": ("models.transformer", "TransformerModel"),
            "gnn": ("models.gnn", "GNNModel"),
        }

        if model_name not in registry:
            raise ValueError(f"Unknown model '{model_name}'. Available models: {', '.join(sorted(registry))}")

        module_name, class_name = registry[model_name]
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    def _build_lightning_trainer(self):
        max_epochs = int(self._get_model_config_value("max_epochs", default=self._get_config_value(["max_epochs"], default=25)))
        precision = self._get_model_config_value("precision", default=self._get_config_value(["precision"], default="32-true"))
        accelerator = self._get_config_value(["accelerator"], default="auto")
        devices = self._get_config_value(["devices"], default="auto")

        return LightningTrainer(
            max_epochs=max_epochs,
            accelerator=accelerator,
            devices=devices,
            precision=precision,
            logger=False,
            enable_checkpointing=False,
            enable_progress_bar=True,
        )

    def _is_lightning_model(self, model) -> bool:
        from models.base_model import LightningBaseModel

        return isinstance(model, LightningBaseModel)

    def _log_metrics(self, model_name: str, metrics: Dict[str, Any]):
        try:
            import wandb

            wandb.log({f"{model_name}/{key}": value for key, value in metrics.items() if key != "target"})
        except Exception:
            pass

    def _save_model(self, model, model_name: str, target_column: str):
        output_dir = self._resolve_path(self._get_config_value(["output_dir"], default="outputs/models"))
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_target = target_column.replace("/", "_")
        path = output_dir / f"{model_name}_{safe_target}"

        if self._is_lightning_model(model):
            torch.save(model.state_dict(), path.with_suffix(".pt"))
            return

        try:
            import joblib

            joblib.dump(model.model, path.with_suffix(".joblib"))
        except Exception:
            torch.save(model, path.with_suffix(".pt"))

    def _get_model_name(self) -> str:
        model_config = self._get_model_config()
        return str(model_config.get("name", self._get_config_value(["model_name"], default="neural_net")))

    def _get_model_config(self) -> Dict[str, Any]:
        model_config = self._get_config_value(["model"], default={})
        if hasattr(model_config, "items"):
            return dict(model_config)
        return {}

    def _get_model_config_value(self, key: str, default: Any = None) -> Any:
        return self._get_model_config().get(key, default)

    def _get_group_column(self) -> str:
        return str(self._get_config_value(["group_column"], default=self._get_config_value(["dataset", "groupsplit_column"], default="pass__user_id")))

    def _get_config_value(self, key_path: Sequence[str], default: Any = None) -> Any:
        current = self.config
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

    def _resolve_path(self, path: str | Path) -> Path:
        path = Path(path)
        if path.is_absolute():
            return path
        return Path.cwd() / path