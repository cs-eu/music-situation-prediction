from abc import ABC
from typing import Dict, Any

class BaseTrainer(ABC):
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model = None