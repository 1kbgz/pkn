from gzip import decompress
from typing import List, Literal, Optional, TypeVar, Union

from boto3 import Session
from botocore.config import Config
from ccflow import BaseModel, CallableModelGenericType, ContextBase, Flow, GenericResult, NullContext, ResultBase
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
    "S3Context",
    "S3Result",
    "S3Model",
)

ResultFormat = Literal["binary", "text", "json", "gzip"]
Context = TypeVar("C", bound=ContextBase)
Result = TypeVar("R", bound=ResultBase)


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


class S3Context(NullContext):
    bucket: Optional[str] = None
    object: Optional[str] = None
    # TODO: format?


class S3ReadContext(S3Context): ...


class S3WriteContext(S3Context): ...


class S3WriteDataContext(S3WriteContext):
    data: Union[bytes, str, dict]


class S3WriteFileContext(S3WriteContext):
    file: str


class S3ReadWriteContext(S3Context):
    read: S3ReadContext
    write: S3WriteContext


class S3Result(GenericResult): ...


class S3ReadResult(S3Result): ...


class S3WriteResult(S3Result): ...


class S3ReadWriteResult(S3Result):
    read: S3ReadResult
    write: S3WriteResult


class S3Model(CallableModelGenericType[S3Context, S3Result]):
    bucket: Optional[str] = None
    object: Optional[str] = None
    client: S3Client

    mode: Literal["read", "write", "read_write"] = "read"
    format: Union[ResultFormat, List[ResultFormat]] = "binary"

    def template(self) -> Template:
        # Loads object as a Jinja2 template
        return Environment().from_string(self.object)

    def _read_data(self, client: S3Client, bucket: str, object: str) -> S3ReadResult:
        read_response = client.client.get_object(Bucket=bucket, Key=object)

        # Read as binary
        read_data = read_response["Body"].read()

        formats = [self.format] if not isinstance(self.format, list) else self.format

        for format in formats:
            match format:
                case "binary":
                    pass  # already binary
                case "text":
                    read_data = read_data.decode("utf-8")
                case "json":
                    read_data = loads(read_data)
                case "gzip":
                    read_data = decompress(read_data)
                case _:
                    raise ValueError(f"Unsupported result format: {format}")
        return S3ReadResult(value=read_data)

    @Flow.call
    def __call__(self, context: S3Context) -> S3Result:
        # TODO: write/readwrite
        # TODO: specify retry policy
        # Use the S3 client to get the object from S3
        bucket = context.bucket or self.bucket
        object = context.object or self.object

        if not bucket or not object:
            raise ValueError("Both bucket and object must be specified either in the model or the context.")

        if isinstance(context, S3ReadContext) or self.mode in ["read", "read_write"]:
            read_result = self._read_data(self.client, bucket, object)
        else:
            read_result = None

        if isinstance(context, S3WriteContext) or self.mode in ["write", "read_write"]:
            write_result = None
        else:
            write_result = None

        if read_result and write_result:
            return S3ReadWriteResult(read=read_result, write=write_result)
        elif read_result:
            return read_result
        elif write_result:
            return write_result
        else:
            raise ValueError("No operation performed; check mode and context types.")
