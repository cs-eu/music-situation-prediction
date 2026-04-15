from torch.utils.data import DataLoader, Dataset
import torch
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple

class SituationDataset:
    """Dataset class for music situation prediction."""
    def __init__(self, data : np.ndarray, config: Dict[str, any]):
        self.X = torch.tensor(data.drop([config["target_columns"]]).values, dtype=torch.float32)
        self.y = torch.tensor(data[config["target_columns"]].values, dtype=torch.float32)

    def __get_item__(self, index: int):
        return self.X[index], self.y[index]

    def __len__(self):
        return len(self.y)

    
def get_dataloader(data: np.ndarray, config: Dict[str, any], batch_size:int=32, shuffle:bool=True, num_workers:int=4) -> DataLoader:
    """Get dataloader for the dataset."""
    dataset = SituationDataset(data, config)
    return DataLoader(dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers)