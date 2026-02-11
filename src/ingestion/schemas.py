from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from ..common.config import load_yaml
from ..common.exceptions import ContractError

DEFAULT_CONTRACT_PATH = Path("config/data_contracts.yaml")
MARKET_EVENT_CONTRACT_NAME = "market_event"


class MarketEvent(BaseModel):
    event_id: str
    symbol: str
    price: float
    volume: int
    ts_event: datetime
    ts_ingest: datetime
    source: str = Field(default="stooq")
    contract_version: str = Field(default="v1")


def load_market_event_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    doc = load_yaml(path)

    try:
        contract = doc["contracts"][MARKET_EVENT_CONTRACT_NAME]
    except KeyError as exc:
        raise ContractError(
            f"Missing contract '{MARKET_EVENT_CONTRACT_NAME}' in {path}"
        ) from exc

    required_keys = {"version", "required_fields", "compatibility", "schema"}
    missing = required_keys - set(contract.keys())
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ContractError(
            f"Contract '{MARKET_EVENT_CONTRACT_NAME}' in {path} is missing keys: {missing_text}"
        )

    if not isinstance(contract["required_fields"], list) or not contract["required_fields"]:
        raise ContractError(
            f"Contract '{MARKET_EVENT_CONTRACT_NAME}' required_fields must be a non-empty list"
        )

    return contract
