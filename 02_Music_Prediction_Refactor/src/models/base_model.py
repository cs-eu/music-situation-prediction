from abc import ABC, abstractmethod
import pickle
import os

class BaseModel(ABC):
    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None

    @abstractmethod
    def fit(self, X_train, y_train):
        pass

    @abstractmethod
    def predict(self, X_test):
        pass

    def save_model(self, output_dir="models"):
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, f"{self.model_name}.pkl"), "wb") as f:
            pickle.dump(self.model, f)

    def load_model(self, input_dir="models"):
        with open(os.path.join(input_dir, f"{self.model_name}.pkl"), "rb") as f:
            self.model = pickle.load(f)