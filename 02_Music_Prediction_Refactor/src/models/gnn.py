from .base_model import LightningBaseModel
from typing import Dict, Any
import torch
import torch.nn as nn

class GNNModel(LightningBaseModel):

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        
