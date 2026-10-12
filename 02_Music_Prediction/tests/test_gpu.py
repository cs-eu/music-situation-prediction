import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest
import torch

from training.trainer import Trainer


@pytest.mark.skipif(not torch.cuda.is_available(), reason="Requires the CUDA-enabled deployment machine.")
@pytest.mark.parametrize("name", ["neural_net", "transformer", "gnn"])
def test_cuda_training_evaluation_and_portable_inference(tmp_path, name, monkeypatch):
    pytest.importorskip("lightning")
    if name == "gnn":
        pytest.importorskip("torch_geometric")
    monkeypatch.setenv("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
    frame = pd.DataFrame({
        "user_id": np.repeat(np.arange(20), 4),
        "gps_count": np.tile(np.arange(4), 20).astype(float),
        "gps_landuse_type": np.tile(["home", "work"], 40),
        "music_track_energy": np.tile([0.2, 0.3, 0.4, 0.5], 20),
    })
    path = tmp_path / "raw.csv"
    frame.to_csv(path, index=False)
    with torch.cuda.device(0):
        precision = "bf16-mixed" if torch.cuda.is_bf16_supported(including_emulation=False) else "16-mixed"
    config = {
        "data_path": str(path), "group_column": "user_id", "target_columns": ["music_track_energy"],
        "test_size": 0.2, "val_size": 0.1, "random_state": 42,
        "accelerator": "gpu", "devices": 1, "precision": precision, "batch_size": 16,
        "evaluate_test": True, "output_dir": str(tmp_path / "models"), "tuning": {"enabled": False},
        "model": {"name": name, "hidden_dims": [16, 8], "hidden_channels": 8,
                  "d_model": 8, "nhead": 2, "num_layers": 1, "dim_feedforward": 16,
                  "dropout": 0, "max_epochs": 2},
    }
    result = Trainer(config).train()[0]
    assert result["evaluation_device"].startswith("cuda")
    assert np.isfinite(result["RMSE"])
    predictor = joblib.load(tmp_path / "models" / f"{name}_music_track_energy.joblib")
    assert next(predictor.model.parameters()).device.type == "cpu"
    assert predictor.model_config["precision"] == precision
    predicted = predictor.predict(frame.iloc[:8], device="cuda", precision=precision)
    assert next(predictor.model.parameters()).device.type == "cuda"
    assert np.isfinite(predicted).all()
    reloaded = joblib.load(tmp_path / "models" / f"{name}_music_track_energy.joblib")
    np.testing.assert_allclose(reloaded.predict(frame.iloc[:8], device="cuda", precision=precision), predicted,
                               rtol=1e-5, atol=1e-6)
    cpu_predictions = reloaded.predict(frame.iloc[:8], device="cpu")
    assert np.isfinite(cpu_predictions).all()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="Requires the CUDA-enabled deployment machine.")
def test_cuda_tabpfn_pipeline_and_native_artifact(tmp_path, monkeypatch):
    checkpoint = os.environ.get("TABPFN_TEST_CHECKPOINT")
    if not checkpoint or not Path(checkpoint).is_file():
        pytest.skip("Set TABPFN_TEST_CHECKPOINT to trusted compatible regression weights.")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("TABPFN_NO_BROWSER", "1")
    frame = pd.DataFrame({
        "user_id": np.repeat(np.arange(20), 3), "gps_count": np.tile([1.0, 2.0, 3.0], 20),
        "phone_SCREEN": np.tile([3, 1, 2], 20), "music_track_energy": np.tile([0.2, 0.3, 0.4], 20),
    })
    path = tmp_path / "raw.csv"
    frame.to_csv(path, index=False)
    config = {
        "data_path": str(path), "group_column": "user_id", "target_columns": ["music_track_energy"],
        "test_size": 0.2, "val_size": 0.1, "random_state": 42, "evaluate_test": True,
        "tabpfn_checkpoint": str(Path(checkpoint).resolve()), "output_dir": str(tmp_path / "models"),
        "model": {"name": "tabpfn", "device": "cuda", "inference_device": "cuda",
                  "model_kwargs": {"n_estimators": 1, "n_preprocessing_jobs": 1}},
        "tuning": {"enabled": False},
    }
    result = Trainer(config).train()[0]
    assert np.isfinite(result["RMSE"])
    restored = joblib.load(tmp_path / "models" / "tabpfn_music_track_energy.joblib")
    assert restored.model.model is None
    predictions = restored.predict(frame.iloc[:6], device="cuda")
    assert restored.model._model_device.startswith("cuda")
    assert np.isfinite(predictions).all()
