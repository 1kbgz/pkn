from os import environ

import pytest

from pkn.ccflow import BackblazeS3Model, load


class TestS3:
    pytest.mark.skipif(environ.get("BACKBLAZE_S3_ENDPOINT_URL") is None, reason="BACKBLAZE_S3_ENDPOINT_URL not set")

    def test_s3_backblaze_example(self):
        cfg = load(["+context=[]", "+extract=backblaze/example"])
        assert isinstance(cfg["read"].model, BackblazeS3Model)

        result = cfg["read"].model(None)
        assert result is not None
        assert result.value.startswith(",Row ID")
