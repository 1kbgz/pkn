from ccflow import Flow, GenericResult

from .base import S3Model

__all__ = ("BackblazeS3Model",)


class BackblazeS3Model(S3Model):
    @Flow.call
    def __call__(self, context):
        # Execute S3Model to extract data
        res = super().__call__(context)

        # Extract res from gzipped CSV, process as needed
        return GenericResult(value=res.value)
