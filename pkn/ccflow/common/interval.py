from typing import Literal

from ccflow import BaseModel
from pydantic import model_validator

__all__ = (
    "Offset",
    "Interval",
)


Offset = Literal[
    "B",  # business day frequency
    "C",  # custom business day frequency
    "D",  # calendar day frequency
    "W",  # weekly frequency
    "ME",  # month end frequency
    "SME",  # semi-month end frequency (15th and end of month)
    "BME",  # business month end frequency
    "CBME",  # custom business month end frequency
    "MS",  # month start frequency
    "SMS",  # semi-month start frequency (1st and 15th)
    "BMS",  # business month start frequency
    "CBMS",  # custom business month start frequency
    "QE",  # quarter end frequency
    "BQE",  # business quarter end frequency
    "QS",  # quarter start frequency
    "BQS",  # business quarter start frequency
    "YE",  # year end frequency
    "BYE",  # business year end frequency
    "YS",  # year start frequency
    "BYS",  # business year start frequency
    "h",  # hourly frequency
    "bh",  # business hour frequency
    "cbh",  # custom business hour frequency
    "min",  # minutely frequency
    "s",  # secondly frequency
    "ms",  # milliseconds
    "us",  # microseconds
    "ns",  # nanoseconds
]


class Interval(BaseModel):
    offset: Offset
    n: int = 1

    @model_validator(mode="before")
    @classmethod
    def validate_n(cls, v, info):
        if isinstance(v, str):
            # v can be of form: "{n}{offset}", e.g. "15D"
            # Split into n and offset
            for i, char in enumerate(v):
                if not char.isdigit():
                    n = int(v[:i])
                    offset = v[i:]
                    return Interval(offset=offset, n=n)
            raise ValueError(f"Invalid interval string: {v}")
        return v
