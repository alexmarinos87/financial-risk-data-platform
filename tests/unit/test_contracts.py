from __future__ import annotations

from pathlib import Path

import pytest

from src.common.exceptions import ContractError
from src.ingestion.schemas import load_market_event_contract


def test_load_market_event_contract_default_file():
    contract = load_market_event_contract()
    assert contract["version"] == "v1"
    assert "event_id" in contract["required_fields"]


def test_load_market_event_contract_raises_when_missing_contract(tmp_path):
    contract_file = tmp_path / "contracts.yaml"
    contract_file.write_text("contracts: {}\n", encoding="utf-8")

    with pytest.raises(ContractError):
        load_market_event_contract(Path(contract_file))
