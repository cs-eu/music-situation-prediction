import torch


def _validate(y_pred, y_true):
    y_pred = torch.as_tensor(y_pred, dtype=torch.float64).reshape(-1)
    y_true = torch.as_tensor(y_true, dtype=torch.float64).reshape(-1)
    if y_pred.shape != y_true.shape or not y_true.numel():
        raise ValueError("Predictions and targets must be nonempty and have equal lengths.")
    if not torch.isfinite(y_pred).all() or not torch.isfinite(y_true).all():
        raise ValueError("Metrics require finite predictions and observed targets.")
    return y_pred, y_true


def RMSE(y_pred, y_true):
    """Root mean squared error."""
    y_pred, y_true = _validate(y_pred, y_true)
    return torch.sqrt(torch.mean((y_pred - y_true) ** 2))

def R2(y_pred, y_true):
    """R-squared score."""
    y_pred, y_true = _validate(y_pred, y_true)
    if y_true.numel() < 2:
        return torch.tensor(float("nan"), dtype=torch.float64)
    ss_res = torch.sum((y_true - y_pred) ** 2)
    ss_tot = torch.sum((y_true - torch.mean(y_true)) ** 2)
    if ss_tot == 0:
        return torch.tensor(1.0 if ss_res == 0 else 0.0, dtype=torch.float64)
    return 1 - ss_res / ss_tot

def calculate_metrics(y_pred, y_true):
    """Calculate and return all metrics."""
    y_pred, y_true = _validate(y_pred, y_true)
    return {
        "RMSE": RMSE(y_pred, y_true),
        "R2": R2(y_pred, y_true),
        "MAE": torch.mean(torch.abs(y_pred - y_true)),
    }
