from gzip import decompress
from typing import List, Literal, Type, Union

from boto3 import Session
from botocore.config import Config
from ccflow import BaseModel, CallableModel, ContextType, Flow, GenericResult, NullContext, ResultType
from jinja2 import Environment, Template

try:
    from orjson import loads
except ImportError:
    from json import loads
from pydantic import Field

__all__ = (
    "S3Config",
    "S3Session",
    "S3Client",
    "S3Model",
)

ResultFormat = Literal["binary", "text", "json", "gzip"]


class S3Config(BaseModel):
    signature_version: str = "s3v4"

    @property
    def config(self) -> Config:
        return Config(signature_version=self.signature_version)


class S3Session(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str

    @property
    def session(self) -> Session:
        return Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )


class S3Client(BaseModel):
    endpoint_url: str
    session: S3Session
    config: S3Config = Field(default_factory=S3Config)

    @property
    def client(self):
        return self.session.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            config=self.config.config,
        )


class S3Model(CallableModel):
    bucket_name: str
    object_key: str
    client: S3Client

    mode: Literal["read", "write", "read_write"] = "read"
    format: Union[ResultFormat, List[ResultFormat]] = "binary"

    def template(self) -> Template:
        # Loads object_key as a Jinja2 template
        return Environment().from_string(self.object_key)

    @property
    def context_type(self) -> Type[ContextType]:
        return NullContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context):
        # TODO: write/readwrite
        # TODO: specify retry policy
        # Use the S3 client to get the object from S3
        s3_client = self.client.client
        response = s3_client.get_object(Bucket=self.bucket_name, Key=self.object_key)

        # Read as binary
        data = response["Body"].read()

        formats = [self.format] if not isinstance(self.format, list) else self.format

        for format in formats:
            match format:
                case "binary":
                    pass  # already binary
                case "text":
                    data = data.decode("utf-8")
                case "json":
                    data = loads(data)
                case "gzip":
                    data = decompress(data)
                case _:
                    raise ValueError(f"Unsupported result format: {format}")
        return GenericResult(value=data)
