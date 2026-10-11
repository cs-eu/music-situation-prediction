from pathlib import Path
from typing import Any

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """Compose the same authoritative Hydra configuration as main.py."""
    path = (Path(config_path).resolve() if config_path is not None else
            Path(__file__).resolve().parents[2] / "conf" / "config.yaml")
    with initialize_config_dir(version_base=None, config_dir=str(path.parent)):
        config = compose(config_name=path.stem)
    return OmegaConf.to_container(config, resolve=True)
