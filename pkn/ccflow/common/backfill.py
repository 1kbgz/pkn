from datetime import datetime
from typing import Generic, List, Literal, Type, TypeVar, Union

from ccflow import (
    BaseModel,
    CallableModel,
    CallableModelGenericType,
    ContextBase,
    ContextType,
    DatetimeRangeContext,
    Flow,
    GenericContext,
    GenericResult,
    NDArray,
    ResultBase,
    ResultType,
)
from numpy import datetime64
from pydantic import Field, SerializeAsAny, model_validator

__all__ = (
    "Offset",
    "Interval",
    "BackfillContext",
    "BackfillModel",
)


C = TypeVar("C", bound=ContextBase)
R = TypeVar("R", bound=ResultBase)
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


class BackfillContext(DatetimeRangeContext, Generic[C]):
    context: SerializeAsAny[C] = Field(default_factory=GenericContext)
    direction: Literal["forward", "backward"] = "forward"
    interval: Interval = Field(description="Interval between each backfill step")

    @model_validator(mode="before")
    @classmethod
    def validate_direction(cls, v):
        if v.get("direction") not in (None, "forward", "backward"):
            raise ValueError("direction must be either 'forward' or 'backward'")
        # Validate interval to not confuse ccflow
        if "interval" in v:
            interval = v["interval"]
            if isinstance(interval, str):
                v["interval"] = Interval.validate_n(interval, None)
        return v

    def steps(self, as_array: bool = False) -> Union[List[datetime], NDArray[datetime64]]:
        # Generate steps with pandas
        import pandas as pd

        # reassemble interval string post-validation
        range = pd.date_range(
            start=self.start_datetime,
            end=self.end_datetime,
            freq=f"{self.interval.n}{self.interval.offset}",
        )

        # Adjust for direction
        if self.direction == "backward":
            range = range.reverse()

        return range


class BackfillResult(GenericResult): ...


class BackfillModel(CallableModel, Generic[C, R]):
    model: CallableModelGenericType[C, R]

    @property
    def context_type(self) -> Type[ContextType]:
        return BackfillContext[self.model.context_type]

    @property
    def result_type(self) -> Type[ResultType]:
        return BackfillResult[self.model.result_type]

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, v):
        if not isinstance(v, dict):
            raise ValueError("model must be a dict representing a CallableModelGenericType")
        return v

    @Flow.call
    def __call__(self, context: BackfillContext[C]) -> R:
        result = {}
        for step in context.steps(as_array=False):
            step_context = context.context.model_copy(update={"datetime": step, "dt": step, "date": step.date()})
            result[step] = self.model(context=step_context)
        return BackfillResult(value=result)
