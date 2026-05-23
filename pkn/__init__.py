__version__ = "0.4.2"

from .importer import make_lazy_getattr

__getattr__ = make_lazy_getattr(
    __name__,
    {
        "infra": [],
        "importer": ["make_lazy_getattr"],
        "logging": ["default", "getLogger", "getSimpleLogger"],
        "pydantic": [
            "get_import_path",
            "serialize_path_as_string",
            "ImportPath",
            "CallablePath",
            "Dict",
            "List",
        ],
    },
)
