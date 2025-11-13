import importlib
from typing import Dict, List

__all__ = ("make_lazy_getattr",)


def make_lazy_getattr(package, module_map: Dict[str, List[str]]):
    # build a reverse index mapping
    mapping = {}
    for mod, subs in module_map.items():
        if mod in mapping:
            # TODO: raise?
            continue
        mapping[mod] = lambda mod=mod, package=package: importlib.import_module(f".{mod}", package=package)

        for sub in subs:
            if sub in mapping:
                # TODO: raise?
                continue
            mapping[sub] = lambda name=sub, mod=mod, sub=sub, package=package: importlib.import_module(f".{mod}", package=package).__getattribute__(
                sub
            )

    def _lazy_load(name):
        if name in mapping:
            return mapping[name]()
        raise AttributeError(f"module '{package}' has no attribute '{name}'")

    return _lazy_load
