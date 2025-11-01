from os import environ

import pytest
from ccflow import PublisherModel

from pkn.ccflow import BackblazeS3Model, BackfillModel, load


class TestS3:
    pytest.mark.skipif(environ.get("BACKBLAZE_S3_ENDPOINT_URL") is None, reason="BACKBLAZE_S3_ENDPOINT_URL not set")

    def test_s3_backblaze_example(self):
        cfg = load(["+extract=backblaze", "+context=[]"], overwrite=True)
        assert isinstance(cfg["action"], PublisherModel)
        assert isinstance(cfg["action"].model, BackblazeS3Model)

        result = cfg["action"].model(None)
        assert result is not None
        assert result.value.startswith(",Row ID")

    def test_s3_backblaze_backfill_example(self):
        cfg = load(["+extract=backblaze", "+backfill=default", "+context=[2025-01-01,2025-01-10,[],forward,2D]"], overwrite=True)
        assert isinstance(cfg["backfill"], BackfillModel)
        assert isinstance(cfg["backfill"].model, PublisherModel)
        assert isinstance(cfg["backfill"].model.model, BackblazeS3Model)
