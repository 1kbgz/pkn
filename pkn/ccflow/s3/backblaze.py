from datetime import datetime
from typing import Optional, Type

from ccflow import Flow

from .base import S3Context, S3Model, S3Result

__all__ = (
    "BackblazeContext",
    "BackblazeS3Model",
)


class BackblazeContext(S3Context):
    dt: Optional[datetime] = None


class BackblazeS3Model(S3Model):
    @property
    def context_type(self) -> Type[BackblazeContext]:
        return BackblazeContext

    @Flow.call
    def __call__(self, context: BackblazeContext) -> S3Result:
        # Execute S3Model to extract data
        res = super().__call__(context)

        print("Executing call")

        # Extract res from gzipped CSV, process as needed
        return S3Result(value=res.value)
