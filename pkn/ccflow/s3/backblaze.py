from typing import Type

from ccflow import ContextType, Flow, GenericContext, GenericResult, ResultType

from .base import S3Model

__all__ = ("BackblazeS3Model",)


class BackblazeS3Model(S3Model):
    @property
    def context_type(self) -> Type[ContextType]:
        return GenericContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: GenericContext):
        # Execute S3Model to extract data
        res = super().__call__(context)

        # Extract res from gzipped CSV, process as needed
        return GenericResult(value=res.value)
