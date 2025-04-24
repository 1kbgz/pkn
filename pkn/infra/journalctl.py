from typing import Literal

from pydantic import BaseModel


class JournalCtl(BaseModel):
    commmand: Literal[
        "vacuum-size",
        "vacuum-files",
        "vacuum-time",
        "verify",
        "sync",
        "flush",
        "rotate",
    ]

    def __call__(self): ...
