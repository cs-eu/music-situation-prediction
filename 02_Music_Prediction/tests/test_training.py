import json

import joblib
import numpy as np
import pandas as pd
import pytest

from training.trainer import Trainer


@pytest.fixture
def training_case(tmp_path):
    rng = np.random.default_rng(8)
    frame = pd.DataFrame({
        "user_id": np.repeat(np.arange(30), 8),
        "gps_count": rng.integers(0, 30, 240).astype(float),
        "gps_landuse_type": np.tile(["home", "work"], 120),
        "music_track_energy": rng.uniform(0.2, 0.8, 240),
        "music_track_valence": rng.uniform(0.1, 0.9, 240),
        "music_track_key": rng.uniform(0, 11, 240),
    })
    path = tmp_path / "raw.csv"
    frame.to_csv(path, index=False)
    config = {
        "data_path": str(path), "group_column": "user_id",
        "target_columns": ["music_track_energy"],
        "model": {"name": "baseline"}, "random_state": 42,
        "test_size": 0.2, "val_size": 0.1, "num_workers": 0,
        "output_dir": str(tmp_path / "models"), "tuning": {"enabled": False},
        "evaluate_test": True,
    }
    return frame, config


def test_complete_artifact_uses_context_only_and_original_target_units(training_case):
    frame, config = training_case
    metrics = Trainer(config).train()[0]
    artifact = joblib.load(f"{config['output_dir']}/baseline_music_track_energy.joblib")
    assert "music_track_valence" not in artifact.feature_columns
    assert "music_track_key" not in artifact.feature_columns
    assert "user_id" not in artifact.feature_columns
    train, val, test = Trainer(config)._split_data(frame, "user_id")
    expected_mean = pd.concat([train, val])["music_track_energy"].mean()
    predictions = artifact.predict(test)
    np.testing.assert_allclose(predictions, expected_mean, rtol=1e-6)
    assert metrics["baseline_RMSE"] == pytest.approx(metrics["RMSE"], abs=1e-7)
    assert metrics["MSE_skill"] == pytest.approx(0, abs=1e-6)
    assert metrics["n_test"] == len(test)
    with open(f"{config['output_dir']}/baseline_manifest.json") as handle:
        manifest = json.load(handle)
    assert manifest["split_counts"]["test"]["participants"] == 6
    assert artifact.run_metadata["run_id"] == manifest["run_id"]
    assert artifact.run_metadata["data_sha256"] == manifest["data_sha256"]
    assert artifact.run_metadata["split_sha256"] == manifest["split_sha256"]
    assert len(manifest["artifacts"]) == 1


def test_missing_targets_are_not_imputed(training_case):
    frame, config = training_case
    train, val, test = Trainer(config)._split_data(frame, "user_id")
    held_out_indices = frame.index[frame["user_id"].isin(test["user_id"].unique())]
    frame.loc[held_out_indices[:5], "music_track_energy"] = np.nan
    frame["liwc_missing"] = np.nan
    frame.to_csv(config["data_path"], index=False)
    config["target_columns"].append("liwc_missing")
    results = Trainer(config).train()
    assert len(results) == 1
    assert results[0]["n_test"] == len(test) - 5
    assert results[0]["n_train"] == len(train) + len(val)


def test_group_cv_records_selected_parameters(training_case):
    _, config = training_case
    config["model"] = {"name": "elastic_net", "alpha": 0.01, "l1_ratio": 0.5, "max_iter": 10000}
    config["tuning"] = {"enabled": True, "cv_folds": 3, "param_grid": {"alpha": [0.001, 0.1]}}
    metrics = Trainer(config).train()[0]
    assert metrics["cv_folds"] == 3
    assert metrics["selected_params"]["alpha"] in [0.001, 0.1]
    assert np.isfinite(metrics["cv_RMSE"])


def test_legacy_globally_transformed_artifact_is_rejected(training_case):
    _, config = training_case
    config["data_path"] = config["data_path"].replace("raw.csv", "preprocessed_features.csv")
    pd.DataFrame({"x": [1]}).to_csv(config["data_path"], index=False)
    with pytest.raises(ValueError, match="before splitting"):
        Trainer(config).train()


def test_renaming_legacy_artifact_does_not_bypass_schema_guard(training_case):
    _, config = training_case
    pd.DataFrame({"pass__user_id": [1], "num__music_track_energy": [0], "num__gps_count": [0]}).to_csv(
        config["data_path"], index=False
    )
    with pytest.raises(ValueError, match="before splitting"):
        Trainer(config).train()


def test_test_metrics_require_explicit_final_evaluation(training_case):
    _, config = training_case
    config["evaluate_test"] = False
    result = Trainer(config).train()[0]
    assert not result["test_evaluated"]
    assert "RMSE" not in result
    assert np.isfinite(result["validation_RMSE"])


def test_gnn_fails_before_loading_data(training_case):
    _, config = training_case
    config["model"] = {"name": "gnn"}
    with pytest.raises(ValueError, match="not a graph"):
        Trainer(config).train()


def test_ambiguous_cross_participant_windows_are_all_quarantined(training_case):
    frame, config = training_case
    duplicate = frame.iloc[[0]].copy()
    duplicate["user_id"] = 1000
    pd.concat([frame, duplicate], ignore_index=True).to_csv(config["data_path"], index=False)
    loaded = Trainer(config)._load_data()
    assert len(loaded) == len(frame) - 1
    assert loaded.attrs["data_quality"]["quarantined_rows"] == 2
    assert loaded.attrs["data_quality"]["provisional_source_data"]


def test_corrupted_liwc_target_is_rejected_if_explicitly_requested(training_case):
    frame, config = training_case
    frame["genius_anger"] = frame["music_track_energy"]
    frame["liwc_anger"] = frame["genius_anger"]
    frame.to_csv(config["data_path"], index=False)
    config["target_columns"] = ["liwc_anger"]
    with pytest.raises(ValueError, match="corrected source data"):
        Trainer(config).train()


@pytest.mark.parametrize("name", ["neural_net", "transformer"])
def test_neural_training_restores_a_portable_complete_predictor(training_case, name, monkeypatch):
    pytest.importorskip("lightning")
    import lightning.pytorch as lightning

    real_trainer = lightning.Trainer
    precision_settings = []

    def capture_trainer(**kwargs):
        precision_settings.append(kwargs["precision"])
        return real_trainer(**kwargs)

    monkeypatch.setattr(lightning, "Trainer", capture_trainer)
    frame, config = training_case
    config["model"] = {
        "name": name, "hidden_dims": [16, 8], "d_model": 8, "nhead": 2,
        "num_layers": 1, "dim_feedforward": 16, "dropout": 0,
        "learning_rate": 0.001, "max_epochs": 2, "early_stopping_patience": 1,
        "precision": "16-mixed",
    }
    config["accelerator"] = "cpu"
    config["devices"] = 1
    config["batch_size"] = 16
    config["precision"] = "32-true"
    metrics = Trainer(config).train()[0]
    assert precision_settings == ["32-true"]
    assert metrics["n_validation"] > 0
    assert np.isfinite(metrics["RMSE"])
    artifact = joblib.load(f"{config['output_dir']}/{name}_music_track_energy.joblib")
    assert not artifact.model.training
    assert artifact.model._trainer is None
    np.testing.assert_allclose(
        artifact.predict(frame.iloc[:9], batch_size=1),
        artifact.predict(frame.iloc[:9], batch_size=9), rtol=1e-5, atol=1e-6,
    )


def test_real_lightgbm_uses_validation_and_roundtrips_predictor(training_case):
    pytest.importorskip("lightgbm")
    frame, config = training_case
    config["model"] = {"name": "lightgbm", "num_rounds": 15, "early_stopping_rounds": 3,
                       "min_data_in_leaf": 5, "num_leaves": 7, "n_jobs": 2}
    result = Trainer(config).train()[0]
    assert result["n_validation"] > 0
    assert 1 <= result["best_iteration"] <= 15
    artifact = joblib.load(f"{config['output_dir']}/lightgbm_music_track_energy.joblib")
    assert np.isfinite(artifact.predict(frame.iloc[:9])).all()
