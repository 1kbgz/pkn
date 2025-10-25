from pathlib import Path
from typing import List, Optional

import hydra
from ccflow import RootModelRegistry, load_config
from ccflow.utils.hydra import cfg_explain_cli, cfg_run

__all__ = (
    "load",
    "explain",
    "main",
)


def load(
    config_dir: str = "",
    config_name: str = "",
    overrides: Optional[List[str]] = None,
    *,
    overwrite: bool = True,
    basepath: str = "",
) -> RootModelRegistry:
    return load_config(
        root_config_dir=str(Path(__file__).resolve().parent),
        root_config_name="base",
        config_dir=config_dir,
        config_name=config_name,
        overrides=overrides,
        overwrite=overwrite,
        basepath=basepath,
    )


def explain():
    cfg_explain_cli(config_path="config", config_name="base", hydra_main=main)


@hydra.main(config_path="config", config_name="base", version_base=None)
def main(cfg):
    cfg_run(cfg)
