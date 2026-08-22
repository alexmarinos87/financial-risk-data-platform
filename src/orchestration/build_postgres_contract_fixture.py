from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from ..common.exceptions import StorageError, ValidationError
from ..ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from ..ingestion.schemas import MarketEvent
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config
from .run_daily_risk import run_daily_risk
from .run_portfolio_attribution_history import run_portfolio_attribution_history
from .run_portfolio_risk import run_portfolio_risk
from .run_portfolio_risk_limits import run_portfolio_risk_limits

CONTRACT_START_DATE = date(2026, 1, 1)
CONTRACT_OBSERVATIONS = 26
CONTRACT_SYMBOLS = ("AAPL", "MSFT")
CONTRACT_PORTFOLIO_ID = "us-tech-equal"
CONTRACT_POLICY_ID = "us-tech-standard"
CONTRACT_COVARIANCE_WINDOW = 20
CONTRACT_DAILY_VOLATILITY_WINDOW = 5
CONTRACT_DAILY_VAR_WINDOW = 10
EXPECTED_DAILY_RETURNS_PER_SYMBOL = CONTRACT_OBSERVATIONS - 1
EXPECTED_ATTRIBUTION_SNAPSHOTS = (
    EXPECTED_DAILY_RETURNS_PER_SYMBOL - CONTRACT_COVARIANCE_WINDOW + 1
)
EXPECTED_LIMIT_EVALUATIONS = EXPECTED_ATTRIBUTION_SNAPSHOTS * 2


def _contract_price(symbol: str, index: int) -> float:
    if symbol == "AAPL":
        value = 100.0 + 0.55 * index + 1.4 * math.sin(index / 2.0)
    elif symbol == "MSFT":
        value = 220.0 + 0.42 * index + 1.8 * math.cos(index / 3.0)
    else:  # pragma: no cover - guarded by the fixed contract symbol tuple.
        raise ValidationError("unsupported PostgreSQL contract symbol")
    return round(value, 6)


def build_contract_daily_events() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index in range(CONTRACT_OBSERVATIONS):
        event_date = CONTRACT_START_DATE + timedelta(days=index)
        ts_event = datetime.combine(event_date, time.min, timezone.utc)
        for symbol_offset, symbol in enumerate(CONTRACT_SYMBOLS):
            event = MarketEvent(
                event_id=alpha_vantage_daily_event_id(symbol, event_date),
                symbol=symbol,
                price=_contract_price(symbol, index),
                volume=1_000_000 + index * 1_000 + symbol_offset,
                ts_event=ts_event,
                ts_ingest=ts_event
                + timedelta(hours=18, minutes=index + symbol_offset),
                source="alpha_vantage",
            )
            records.append(event.model_dump())
    return records


def _require_selected_count(
    summary: dict[str, Any],
    dataset: str,
    expected: int,
) -> None:
    actual = summary["curated_output"][dataset]["records_selected"]
    if actual != expected:
        raise ValidationError(
            f"PostgreSQL contract fixture expected {expected} {dataset} records, "
            f"received {actual}"
        )


def build_postgres_contract_fixture(
    *,
    storage_config_path: Path = Path("config/storage.yaml"),
    portfolio_config_path: Path = Path("config/portfolios.yaml"),
    limits_config_path: Path = Path("config/portfolio_risk_limits.yaml"),
) -> dict[str, Any]:
    storage_config = load_storage_config(storage_config_path)
    raw_events = build_contract_daily_events()
    raw_written = write_records(
        raw_events,
        kind="raw",
        dataset="market_events",
        storage_config=storage_config,
    )

    end_date = CONTRACT_START_DATE + timedelta(days=CONTRACT_OBSERVATIONS - 1)
    first_attribution_date = CONTRACT_START_DATE + timedelta(
        days=CONTRACT_COVARIANCE_WINDOW
    )

    daily_summaries: dict[str, dict[str, Any]] = {}
    for symbol in CONTRACT_SYMBOLS:
        summary = run_daily_risk(
            symbol=symbol,
            start_date=None,
            end_date=end_date,
            volatility_window=CONTRACT_DAILY_VOLATILITY_WINDOW,
            var_window=CONTRACT_DAILY_VAR_WINDOW,
            var_confidence=0.95,
            storage_config_path=storage_config_path,
        )
        _require_selected_count(
            summary,
            "daily_returns",
            EXPECTED_DAILY_RETURNS_PER_SYMBOL,
        )
        daily_summaries[symbol] = summary

    portfolio_summary = run_portfolio_risk(
        portfolio_id=CONTRACT_PORTFOLIO_ID,
        portfolio_config_path=portfolio_config_path,
        start_date=None,
        end_date=end_date,
        volatility_window=CONTRACT_DAILY_VOLATILITY_WINDOW,
        var_window=CONTRACT_DAILY_VAR_WINDOW,
        var_confidence=0.95,
        storage_config_path=storage_config_path,
    )
    _require_selected_count(
        portfolio_summary,
        "portfolio_daily_returns",
        EXPECTED_DAILY_RETURNS_PER_SYMBOL,
    )

    attribution_summary = run_portfolio_attribution_history(
        portfolio_id=CONTRACT_PORTFOLIO_ID,
        portfolio_config_path=portfolio_config_path,
        start_date=first_attribution_date,
        end_date=end_date,
        covariance_window=CONTRACT_COVARIANCE_WINDOW,
        max_snapshots=EXPECTED_ATTRIBUTION_SNAPSHOTS,
        storage_config_path=storage_config_path,
    )
    _require_selected_count(
        attribution_summary,
        "portfolio_risk_attribution",
        EXPECTED_ATTRIBUTION_SNAPSHOTS,
    )

    limits_summary = run_portfolio_risk_limits(
        policy_id=CONTRACT_POLICY_ID,
        limits_config_path=limits_config_path,
        portfolio_config_path=portfolio_config_path,
        start_date=first_attribution_date,
        end_date=end_date,
        max_evaluations=EXPECTED_LIMIT_EVALUATIONS,
        storage_config_path=storage_config_path,
    )
    _require_selected_count(
        limits_summary,
        "portfolio_risk_limit_evaluations",
        EXPECTED_LIMIT_EVALUATIONS,
    )

    return {
        "contract": "postgresql-source-to-serving-v1",
        "date_range": {
            "start_date": CONTRACT_START_DATE.isoformat(),
            "end_date": end_date.isoformat(),
            "first_attribution_date": first_attribution_date.isoformat(),
        },
        "raw": {
            "records_selected": len(raw_events),
            "records_written": raw_written,
            "records_already_present": len(raw_events) - raw_written,
        },
        "daily": {
            symbol: summary["curated_output"]
            for symbol, summary in daily_summaries.items()
        },
        "portfolio": portfolio_summary["curated_output"],
        "attribution": attribution_summary["curated_output"],
        "limits": limits_summary["curated_output"],
    }


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("Unable to write PostgreSQL contract fixture summary") from None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build deterministic local Parquet evidence for the real PostgreSQL "
            "warehouse contract without calling a provider."
        )
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument(
        "--limits-config",
        type=Path,
        default=Path("config/portfolio_risk_limits.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        summary = build_postgres_contract_fixture(
            storage_config_path=args.storage_config,
            portfolio_config_path=args.portfolio_config,
            limits_config_path=args.limits_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except (StorageError, ValidationError) as exc:
        print(f"PostgreSQL contract fixture failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print("PostgreSQL contract fixture failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
