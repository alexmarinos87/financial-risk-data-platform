from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypeAlias

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from .portfolio_risk import PortfolioDefinition, parse_portfolio_definition

MAX_MANDATES = 100
MANDATE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")

MandateInput: TypeAlias = Mapping[str, Any]
MandateRecord: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PortfolioMandate(PortfolioDefinition):
    mandate_id: str
    effective_from: date
    effective_to: date | None

    @property
    def constituent_definition_fingerprint(self) -> str:
        payload = {
            "base_currency": self.base_currency,
            "constituents": [
                {
                    "source": constituent.source,
                    "symbol": constituent.symbol,
                    "weight": constituent.weight,
                }
                for constituent in self.constituents
            ],
            "portfolio_id": self.portfolio_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"portfolio-{digest}"

    @property
    def fingerprint(self) -> str:
        payload = {
            "constituent_definition_fingerprint": (
                self.constituent_definition_fingerprint
            ),
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                self.effective_to.isoformat()
                if self.effective_to is not None
                else None
            ),
            "mandate_id": self.mandate_id,
            "portfolio_id": self.portfolio_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"portfolio-mandate-{digest}"

    def contains(self, event_date: date) -> bool:
        return self.effective_from <= event_date and (
            self.effective_to is None or event_date < self.effective_to
        )


def _strict_date(value: Any, label: str, *, allow_none: bool = False) -> date | None:
    if value is None and allow_none:
        return None
    if isinstance(value, datetime):
        raise ValidationError(f"{label} must be a calendar date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a calendar date")
    try:
        parsed = date.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(
            f"{label} must be a calendar date in YYYY-MM-DD format"
        ) from None
    if value.strip() != parsed.isoformat():
        raise ValidationError(
            f"{label} must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _mandate_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ValidationError("mandate_id must be text")
    parsed = value.strip().lower()
    if MANDATE_ID_PATTERN.fullmatch(parsed) is None:
        raise ValidationError("mandate_id has an invalid format")
    return parsed


def _portfolio_candidate(
    payload: Mapping[str, Any],
    portfolio_id: str,
) -> Mapping[str, Any]:
    portfolios = payload.get("portfolios")
    if not isinstance(portfolios, Mapping):
        raise ValidationError(
            "portfolio configuration must define a portfolios mapping"
        )
    candidate = portfolios.get(portfolio_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"portfolio '{portfolio_id}' is not configured")
    return candidate


def _build_mandate(
    *,
    portfolio_id: str,
    raw: MandateInput,
) -> PortfolioMandate:
    mandate_id = _mandate_id(raw.get("mandate_id"))
    effective_from = _strict_date(raw.get("effective_from"), "effective_from")
    effective_to = _strict_date(
        raw.get("effective_to"),
        "effective_to",
        allow_none=True,
    )
    if effective_from is None:  # pragma: no cover - required above.
        raise ValidationError("effective_from is required")
    if effective_to is not None and effective_to <= effective_from:
        raise ValidationError("effective_to must be after effective_from")

    base = parse_portfolio_definition(
        {
            "portfolios": {
                portfolio_id: {
                    "base_currency": raw.get("base_currency"),
                    "constituents": raw.get("constituents"),
                }
            }
        },
        portfolio_id,
    )
    return PortfolioMandate(
        portfolio_id=base.portfolio_id,
        base_currency=base.base_currency,
        constituents=base.constituents,
        mandate_id=mandate_id,
        effective_from=effective_from,
        effective_to=effective_to,
    )


def parse_portfolio_mandates(
    payload: Mapping[str, Any],
    portfolio_id: str,
) -> tuple[PortfolioMandate, ...]:
    if not isinstance(payload, Mapping):
        raise ValidationError("portfolio configuration must be a mapping")
    candidate = _portfolio_candidate(payload, portfolio_id)
    raw_mandates = candidate.get("mandates")

    if raw_mandates is None:
        raw_entries: list[MandateInput] = [candidate]
    else:
        if "base_currency" in candidate or "constituents" in candidate:
            raise ValidationError(
                "portfolio configuration must not mix direct and mandate definitions"
            )
        if (
            not isinstance(raw_mandates, list)
            or not 1 <= len(raw_mandates) <= MAX_MANDATES
        ):
            raise ValidationError(
                "portfolio mandates must contain between 1 and "
                f"{MAX_MANDATES} entries"
            )
        if any(not isinstance(entry, Mapping) for entry in raw_mandates):
            raise ValidationError("each portfolio mandate must be a mapping")
        raw_entries = list(raw_mandates)

    mandates = tuple(
        sorted(
            (
                _build_mandate(portfolio_id=portfolio_id, raw=entry)
                for entry in raw_entries
            ),
            key=lambda item: (item.effective_from, item.mandate_id),
        )
    )
    mandate_ids = [item.mandate_id for item in mandates]
    if len(mandate_ids) != len(set(mandate_ids)):
        raise ValidationError("portfolio mandate IDs must be unique")

    for previous, current in zip(mandates, mandates[1:], strict=False):
        if previous.effective_to is None:
            raise ValidationError(
                "an open-ended portfolio mandate must be the final mandate"
            )
        if current.effective_from < previous.effective_to:
            raise ValidationError("portfolio mandates must not overlap")
    return mandates


def select_portfolio_mandate(
    payload: Mapping[str, Any],
    portfolio_id: str,
    as_of_date: date,
) -> PortfolioMandate:
    if isinstance(as_of_date, datetime) or not isinstance(as_of_date, date):
        raise ValidationError("as_of_date must be a calendar date")
    mandates = parse_portfolio_mandates(payload, portfolio_id)
    matches = [mandate for mandate in mandates if mandate.contains(as_of_date)]
    if len(matches) != 1:
        raise ValidationError(
            f"portfolio '{portfolio_id}' has no unique mandate for "
            f"{as_of_date.isoformat()}"
        )
    return matches[0]


def load_portfolio_mandate(
    path: Path,
    portfolio_id: str,
    as_of_date: date,
) -> PortfolioMandate:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("portfolio configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("portfolio configuration must be a mapping")
    return select_portfolio_mandate(payload, portfolio_id, as_of_date)


def validate_mandate_range(
    mandate: PortfolioMandate,
    *,
    start_date: date | None,
    end_date: date,
) -> None:
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    if not mandate.contains(end_date):
        raise ValidationError("end_date is outside the selected portfolio mandate")
    if start_date is not None and not mandate.contains(start_date):
        raise ValidationError(
            "requested date range crosses a portfolio mandate boundary; "
            "split the request by mandate"
        )


def _record_date(record: MandateRecord) -> date:
    value = record.get("ts_event")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValidationError("portfolio mandate input timestamps must be aware")
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(
                "portfolio mandate input timestamps are invalid"
            ) from None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValidationError("portfolio mandate input timestamps must be aware")
        return parsed.date()
    raise ValidationError("portfolio mandate input timestamps are invalid")


def filter_records_to_mandate(
    records: Iterable[MandateRecord],
    mandate: PortfolioMandate,
) -> list[MandateRecord]:
    return [record for record in records if mandate.contains(_record_date(record))]


def mandate_metadata(mandate: PortfolioMandate) -> dict[str, Any]:
    return {
        "mandate_id": mandate.mandate_id,
        "mandate_fingerprint": mandate.fingerprint,
        "constituent_definition_fingerprint": (
            mandate.constituent_definition_fingerprint
        ),
        "effective_from": mandate.effective_from.isoformat(),
        "effective_to": (
            mandate.effective_to.isoformat()
            if mandate.effective_to is not None
            else None
        ),
    }
