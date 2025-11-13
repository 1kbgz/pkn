__version__ = "0.3.1"


def _lazy_load(name):
    import importlib

    if name in (
        "ccflow",
        "infra",
        "logging",
        "pydantic",
    ):
        # import relative submodule
        return importlib.import_module(f".{name}", package=__name__)
    if name in (
        "Message",
        "SMTP",
        "Attachment",
        "Email",
        "get_import_path",
        "serialize_path_as_string",
        "ImportPath",
        "CallablePath",
        "Dict",
        "List",
    ):
        return importlib.import_module(".pydantic", package=__name__).__getattribute__(name)
    if name in ("default", "getLogger", "getSimpleLogger"):
        return importlib.import_module(".logging", package=__name__).__getattribute__(name)
    raise AttributeError(f"module 'pkn' has no attribute '{name}'")


__getattr__ = _lazy_load
