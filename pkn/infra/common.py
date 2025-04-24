from typing import Literal

from pydantic import BaseModel

SIZE_SUFFIX = Literal["B", "K", "M", "G", "T"]

TIME_SUFFIX_SHORT = Literal["s", "m", "h"]
TIME_SUFFIX_LONG_SINGULAR = Literal["second", "minute", "hour", "day", "month", "week", "year"]
TIME_SUFFIX_LONG_PLURAL = Literal["seconds", "minutes", "hours", "days", "months", "weeks", "years"]


__all__ = ["Size", "Time"]


class Quantifiable(BaseModel):
    value: int
    suffix: str
    space: bool = False

    def __str__(self):
        return f"{self.value}{self.suffix}" if not self.space else f"{self.value} {self.suffix}"


class Size(BaseModel):
    suffix: SIZE_SUFFIX


class Time(BaseModel):
    suffix: TIME_SUFFIX_SHORT | TIME_SUFFIX_LONG_SINGULAR | TIME_SUFFIX_LONG_PLURAL
