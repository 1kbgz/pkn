from pathlib import Path
from typing import List, Optional

from ccflow import ModelRegistry
from ccflow.utils.hydra import ConfigLoadResult, load_config

__all__ = ("load",)


def load(
    overrides: Optional[List[str]] = None,
    overwrite: bool = False,
    config_dir: Optional[str] = None,
    config_key: Optional[str] = None,
    version_base: Optional[str] = None,
) -> ConfigLoadResult:
    parent_dir = str(Path(__file__).resolve().parent)
    result = load_config(
        root_config_dir=parent_dir,
        root_config_name="base",
        config_dir=config_dir,
        overrides=overrides,
        version_base=version_base,
        basepath=parent_dir,
        debug=False,
    )
    cfg = result.cfg
    if config_key is not None:
        cfg = cfg[config_key]
    r = ModelRegistry.root()
    r.load_config(cfg, overwrite=overwrite)
    return r
