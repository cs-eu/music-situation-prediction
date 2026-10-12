import os
import sys
from pathlib import Path

# A non-Lightning backend may initialize CUDA first in a multi-model sweep.
os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")

import hydra
from omegaconf import DictConfig, OmegaConf

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from training.trainer import Trainer
from utils.logging import setup_wandb


@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    config_dict = OmegaConf.to_container(cfg, resolve=True)
    model_config = cfg.get("model") if cfg.get("model") else {}
    model_name = (
        model_config.get("name", "neural_net")
        if hasattr(model_config, "get")
        else "neural_net"
    )
    run_name = cfg.get("run_name") or model_name
    wandb_config = cfg.get("wandb") if cfg.get("wandb") else {}
    project_name = (
        wandb_config.get("project", "music_prediction")
        if hasattr(wandb_config, "get")
        else "music_prediction"
    )

    run = setup_wandb(
        project_name=project_name,
        config=config_dict,
        run_name=run_name,
    )

    try:
        trainer = Trainer(cfg)
        results = trainer.train()
        print(OmegaConf.to_yaml(OmegaConf.create({"results": results})))
    finally:
        if run is not None:
            run.finish()


if __name__ == "__main__":
    main()
