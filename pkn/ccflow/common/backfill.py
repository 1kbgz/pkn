from typing import Type, Literal, Generic, TypeVar
from ccflow import DatetimeRangeContext, ContextBase, GenericContext, CallableModel, ContextType, ResultType, ResultBase, Flow
from pydantic import SerializeAsAny, Field


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

class BackfillContext(DatetimeRangeContext, Generic[C]):
    context: SerializeAsAny[C] = Field(default_factory=GenericContext)

    direction: Literal["forward", "backward"] = "forward"

    interval:

class BackfillModel(CallableModel, Generic[R]):

    @property
    def context_type(self) -> Type[ContextType]:
        return BackfillContext

    @property
    def result_type(self) -> Type[ResultType]:
        return R

    @Flow.call
    def __call__(self, context: BackfillContext[C]) -> R:

        raise NotImplementedError()
