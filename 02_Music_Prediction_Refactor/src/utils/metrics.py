import numpy as np
import torch

def RMSE(y_pred, y_true):
    """Root mean squared error."""
    return torch.sqrt(torch.mean((y_pred - y_true) ** 2))

def R2(y_pred, y_true):
    """R-squared score."""
    ss_res = torch.sum((y_true - y_pred) ** 2)
    ss_tot = torch.sum((y_true - torch.mean(y_true)) ** 2)
    return 1 - ss_res / ss_tot

def calculate_metrics(y_pred, y_true):
    """Calculate and return all metrics."""
    return {
        "RMSE": RMSE(y_pred, y_true),
        "R2": R2(y_pred, y_true)
    }
