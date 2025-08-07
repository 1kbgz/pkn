from importlib.machinery import ModuleSpec, SourceFileLoader
from inspect import currentframe
from logging import Logger, getLogger as baseGetLogger
from logging.config import dictConfig
from typing import Literal, Union

__all__ = ("default", "getLogger")

LogLevelStr = Literal["CRITICAL", "FATAL", "ERROR", "WARNING", "WARN", "INFO", "DEBUG", "NOTSET"]
LogLevelInt = Literal[50, 40, 30, 20, 10, 0]
LogLevel = Union[LogLevelStr, LogLevelInt]


def default(
    file: str = "output.log",
    console_level: LogLevel = "INFO",
    file_level: LogLevel = "DEBUG",
) -> None:
    config = dict(
        version=1,
        disable_existing_loggers=False,
        formatters={
            "simple": {"format": "[%(asctime)s][%(threadName)s][%(name)s][%(levelname)s]: %(message)s"},
            "colorlog": {
                "()": "colorlog.ColoredFormatter",
                "format": "[%(cyan)s%(asctime)s%(reset)s][%(threadName)s][%(blue)s%(name)s%(reset)s][%(log_color)s%(levelname)s%(reset)s]: %(message)s",
                "log_colors": {
                    "DEBUG": "white",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red",
                },
            },
            "whenAndWhere": {"format": "[%(asctime)s][%(threadName)s][%(name)s][%(filename)s:%(lineno)d][%(levelname)s]: %(message)s"},
        },
        handlers={
            "console": {"level": console_level, "class": "ccflow.utils.logging.StreamHandler", "formatter": "colorlog", "stream": "ext://sys.stdout"},
        },
        root={"handlers": ["console"], "level": "DEBUG"},
    )
    if file:
        config["handlers"]["file"] = {"level": file_level, "class": "ccflow.FileHandler", "formatter": "whenAndWhere", "filename": file}
    dictConfig(config)


def getLogger() -> Logger:
    # Get fully qualified module name of parent caller
    cur_frame = currentframe()
    caller_frame = cur_frame.f_back

    spec = caller_frame.f_globals["__spec__"]
    loader = caller_frame.f_globals["__loader__"] or spec

    if isinstance(loader, SourceFileLoader):
        module_name = loader.name
    elif spec and isinstance(spec, ModuleSpec):
        module_name = spec.name
    return baseGetLogger(module_name)
