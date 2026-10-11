import pytest
import torch

from utils.metrics import calculate_metrics


def test_metrics_flatten_column_vectors_without_broadcasting():
    metrics = calculate_metrics(torch.tensor([[1.0], [3.0]]), torch.tensor([1.0, 2.0]))
    assert metrics["RMSE"].item() == pytest.approx(0.5**0.5)
    assert metrics["R2"].item() == -1.0
    assert metrics["MAE"].item() == 0.5


def test_constant_targets_have_finite_r_squared():
    assert calculate_metrics([2, 2], [2, 2])["R2"] == 1
    assert calculate_metrics([1, 1], [2, 2])["R2"] == 0


@pytest.mark.parametrize("pred,true", [([], []), ([1], [1, 2]), ([float('nan')], [1])])
def test_invalid_metrics_are_rejected(pred, true):
    with pytest.raises(ValueError):
        calculate_metrics(pred, true)
