from pathlib import Path

import pytest

from src.common.exceptions import StorageError
from src.storage.storage_config import load_storage_config, validate_storage_config


def test_storage_config_valid():
    config = load_storage_config(Path("config/storage.yaml"))
    storage = config["storage"]
    assert storage["raw"]["dataset"]
    assert "risk_summary" in storage["curated"]["datasets"]
    assert {
        "daily_returns",
        "daily_volatility",
        "daily_risk_summary",
        "daily_market_freshness",
        "portfolio_daily_returns",
        "portfolio_daily_risk_summary",
        "portfolio_risk_attribution",
        "portfolio_risk_limit_evaluations",
        "portfolio_risk_notification_outbox",
    }.issubset(storage["curated"]["datasets"])
    assert storage["partitioning"]["granularity"]


def test_storage_config_missing_keys_raises():
    with pytest.raises(StorageError):
        validate_storage_config(
            {"storage": {"raw": {}, "curated": {}, "partitioning": {}}}
        )
