from __future__ import annotations

import importlib
import pickle
from collections.abc import Sequence
from typing import Any

import pandas as pd
import torch
from data import DataSplitter, get_dataloader
from lightning.pytorch import Trainer as LightningTrainer
from utils.metrics import calculate_metrics
from utils.training import (
    get_config_value,
    get_feature_columns,
    get_group_column,
    get_target_columns,
    log_model_metrics,
    resolve_path,
)


class Trainer:
    def __init__(self, config):
        self.config = config
        self.model = None

    def train(self):
        data_frame = self._load_data()
        target_columns = get_target_columns(data_frame, self.config)
        if not target_columns:
            raise ValueError("No target columns were found in the training data.")

        group_column = get_group_column(self.config)
        drop_features = get_config_value(
            self.config, ["dataset", "drop_features"], default=[]
        )
        feature_columns = get_feature_columns(
            data_frame,
            target_columns,
            group_column,
            drop_columns=drop_features,
        )
        train_frame, val_frame, test_frame = self._split_data(data_frame, group_column)

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
        data_path = resolve_path(
            get_config_value(
                self.config, ["data_path"], default="data/preprocessed_features.csv"
            )
        )
        if not data_path.exists():
            raise FileNotFoundError(f"Training data not found at {data_path}")
        return pd.read_csv(data_path)

    def _split_data(self, data_frame: pd.DataFrame, group_column: str):
        splitter = DataSplitter(
            test_size=float(get_config_value(self.config, ["test_size"], default=0.1)),
            val_size=float(get_config_value(self.config, ["val_size"], default=0.0)),
            random_state=int(get_config_value(self.config, ["random_state"], default=42)),
        )
        return splitter.group_split(data_frame, group_column)

    def _train_single_target(
        self,
        train_frame: pd.DataFrame,
        val_frame: pd.DataFrame | None,
        test_frame: pd.DataFrame,
        feature_columns: Sequence[str],
        target_column: str,
    ) -> dict[str, Any]:
        model_name = self._get_model_name()
        model_config = self._get_model_config()

        train_loader = self._build_dataloader(
            train_frame, feature_columns, target_column, shuffle=True
        )
        val_loader = (
            self._build_dataloader(
                val_frame, feature_columns, target_column, shuffle=False
            )
            if val_frame is not None
            else None
        )
        test_loader = self._build_dataloader(
            test_frame, feature_columns, target_column, shuffle=False
        )

        model = self._build_model(
            model_name, model_config, input_dim=len(feature_columns)
        )
        self.model = model

        if self._is_lightning_model(model):
            lightning_trainer = self._build_lightning_trainer()
            if val_loader is None:
                lightning_trainer.fit(model, train_dataloaders=train_loader)
            else:
                lightning_trainer.fit(
                    model, train_dataloaders=train_loader, val_dataloaders=val_loader
                )
            prediction_batches = lightning_trainer.predict(
                model, dataloaders=test_loader
            )
            prediction_tensor = (
                torch.cat([batch.detach().cpu() for batch in prediction_batches], dim=0)
                if prediction_batches
                else torch.empty(0)
            )
        else:
            model.fit(train_loader)
            prediction_tensor = model.predict(test_loader)

        prediction_tensor = prediction_tensor.view(-1)
        target_tensor = torch.cat(
            [batch_targets for _, batch_targets in test_loader], dim=0
        ).view(-1)
        metrics = calculate_metrics(prediction_tensor, target_tensor)
        metrics = {
            key: float(value.detach().cpu().item() if torch.is_tensor(value) else value)
            for key, value in metrics.items()
        }
        metrics["target"] = target_column

        log_model_metrics(model_name, metrics)
        self._save_model(model, model_name, target_column)
        return metrics

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
            "group_column": get_group_column(self.config),
        }
        batch_size = int(self._get_config_value(["batch_size"], default=32))
        num_workers = int(self._get_config_value(["num_workers"], default=0))
        return get_dataloader(
            frame,
            dataset_config,
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
        )

    def _build_model(
        self, model_name: str, model_config: dict[str, Any], input_dim: int
    ):
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
            raise ValueError(
                f"Unknown model '{model_name}'. Available models: {', '.join(sorted(registry))}"
            )

        module_name, class_name = registry[model_name]
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    def _build_lightning_trainer(self):
        max_epochs = int(
            self._get_model_config_value(
                "max_epochs",
                default=get_config_value(self.config, ["max_epochs"], default=25),
            )
        )
        precision = self._get_model_config_value(
            "precision",
            default=get_config_value(self.config, ["precision"], default="32-true"),
        )
        accelerator = get_config_value(self.config, ["accelerator"], default="auto")
        devices = get_config_value(self.config, ["devices"], default="auto")

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

    def _save_model(self, model, model_name: str, target_column: str):
        output_dir = resolve_path(
            get_config_value(self.config, ["output_dir"], default="outputs/models")
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_target = target_column.replace("/", "_")
        path = output_dir / f"{model_name}_{safe_target}"

        if self._is_lightning_model(model):
            torch.save(model.state_dict(), path.with_suffix(".pt"))
            return

        try:
            import joblib

        except ImportError:
            torch.save(model, path.with_suffix(".pt"))
            return

        try:
            joblib.dump(model.model, path.with_suffix(".joblib"))
        except (AttributeError, OSError, TypeError, pickle.PicklingError):
            torch.save(model, path.with_suffix(".pt"))

    def _get_model_name(self) -> str:
        model_config = self._get_model_config()
        return str(
            model_config.get(
                "name", self._get_config_value(["model_name"], default="neural_net")
            )
        )

    def _get_model_config(self) -> dict[str, Any]:
        model_config = self._get_config_value(["model"], default={})
        if hasattr(model_config, "items"):
            return dict(model_config)
        return {}

    def _get_model_config_value(self, key: str, default: Any = None) -> Any:
        return self._get_model_config().get(key, default)

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

