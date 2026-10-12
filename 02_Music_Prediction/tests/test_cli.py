import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import joblib
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def cli_data(tmp_path):
    rng = np.random.default_rng(19)
    count = rng.integers(0, 20, 120).astype(float)
    frame = pd.DataFrame({
        "user_id": np.repeat(np.arange(30), 4),
        "gps_count": count,
        "gps_landuse_type": np.tile(["home", "work"], 60),
        "phone_SCREEN": rng.integers(0, 10, 120),
        "app_Communication": rng.integers(0, 10, 120),
        "music_track_energy": 0.2 + 0.05 * np.log1p(count) + rng.normal(0, 0.01, 120),
    })
    frame["music_track_valence"] = 1 - frame["music_track_energy"]
    path = tmp_path / "raw.csv"
    frame.to_csv(path, index=False)
    return frame, path


def _run_sweep(tmp_path, cli_data, names, checkpoint=None):
    frame, path = cli_data
    entry = Path(__file__).resolve().parents[1] / "main.py"
    output = tmp_path / "sweep"
    command = [
        sys.executable, str(entry), "--multirun", f"model={','.join(names)}",
        f"data_path={json.dumps(str(path))}", "target_columns=[music_track_energy]",
        "dataset.max_rows=100", "tuning.enabled=false", "accelerator=cpu", "cpu_threads=2",
        "++model.device=cpu", "++model.inference_device=cpu",
        "evaluate_test=true", "++model.max_epochs=2",
        f"hydra.sweep.dir={json.dumps(str(output))}",
        "hydra.sweep.subdir=${hydra.job.num}-${model.name}", "output_dir=${hydra:runtime.output_dir}",
    ]
    if checkpoint is not None:
        command.append(f"tabpfn_checkpoint={json.dumps(str(checkpoint))}")
    environment = {**os.environ, "HF_HUB_OFFLINE": "1", "TABPFN_NO_BROWSER": "1",
                   "TABPFN_DISABLE_TELEMETRY": "1", "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2"}
    completed = subprocess.run(command, cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=240)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    identities = set()
    for index, name in enumerate(names):
        directory = output / f"{index}-{name}"
        manifest = json.loads((directory / f"{name}_manifest.json").read_text())
        results = json.loads((directory / f"{name}_metrics.json").read_text())
        assert manifest["model"] == name
        assert manifest["data_quality"]["sampling"]["selected_rows"] == 100
        assert manifest["config"]["model"]["name"] == name
        assert len(results) == 1 and np.isfinite(results[0]["RMSE"])
        identities.add(manifest["split_sha256"])
        # All archives, including the feature graph, can be restored on CPU.
        predictor = joblib.load(directory / f"{name}_music_track_energy.joblib")
        assert "music_track_valence" not in predictor.feature_columns
        assert predictor.run_metadata["run_id"] == manifest["run_id"]
        np.testing.assert_allclose(predictor.predict(frame.iloc[:7]), predictor.predict(frame.iloc[:7]), rtol=1e-6)
    assert len(identities) == 1


def test_core_models_run_together_with_isolated_outputs(tmp_path, cli_data):
    _run_sweep(tmp_path, cli_data, ["baseline", "elastic_net", "random_forest"])


def test_all_non_foundation_models_run_together(tmp_path, cli_data):
    for module in ["lightning", "lightgbm", "torch_geometric"]:
        pytest.importorskip(module)
    _run_sweep(tmp_path, cli_data, ["baseline", "elastic_net", "random_forest", "lightgbm",
                                   "neural_net", "transformer", "gnn"])


def test_all_eight_models_run_together_with_real_tabpfn(tmp_path, cli_data, monkeypatch):
    for module in ["lightning", "lightgbm", "torch_geometric"]:
        pytest.importorskip(module)
    if importlib.util.find_spec("tabpfn") is None:
        pytest.skip("Install the all extra for the real TabPFN CLI test.")
    checkpoint = os.environ.get("TABPFN_TEST_CHECKPOINT")
    if not checkpoint or not Path(checkpoint).is_file():
        pytest.skip("Set TABPFN_TEST_CHECKPOINT to trusted compatible regression weights; no test downloads weights.")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("TABPFN_NO_BROWSER", "1")
    monkeypatch.setenv("TABPFN_DISABLE_TELEMETRY", "1")
    _run_sweep(tmp_path, cli_data, ["baseline", "elastic_net", "random_forest", "lightgbm",
                                   "neural_net", "transformer", "gnn", "tabpfn"], checkpoint=checkpoint)
