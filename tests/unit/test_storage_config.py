from pathlib import Path

import pytest

from src.common.exceptions import StorageError
from src.storage.storage_config import load_storage_config, validate_storage_config


def test_storage_config_valid():
    config = load_storage_config(Path("config/storage.yaml"))
    storage = config["storage"]
    assert storage["raw"]["dataset"]
    assert "risk_summary" in storage["curated"]["datasets"]
    assert storage["partitioning"]["granularity"]


def test_storage_config_missing_keys_raises():
    with pytest.raises(StorageError):
        validate_storage_config({"storage": {"raw": {}, "curated": {}, "partitioning": {}}})
