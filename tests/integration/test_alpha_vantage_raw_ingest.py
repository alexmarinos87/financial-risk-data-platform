from __future__ import annotations

import json
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

import src.orchestration.ingest_alpha_vantage_daily as ingest_module
import src.storage.raw_event_writer as raw_writer_module
from src.common.exceptions import IngestionError, RawEventConflictError, StorageError
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.ingestion.schemas import MarketEvent
from src.orchestration.ingest_alpha_vantage_daily import ingest_alpha_vantage_daily
from src.storage.partitioning import partition_path
from tests.storage_config_helpers import build_storage_config, write_storage_config

RUN_TIME = datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc)
END_DATE = date(2025, 1, 31)
SECRET = "sentinel-alpha-vantage-secret"


def _event(
    event_date: date,
    ingested_at: datetime,
    *,
    price: float = 100.0,
    event_id: str | None = None,
    symbol: str = "IBM",
    source: str = "alpha_vantage",
) -> MarketEvent:
    return MarketEvent(
        event_id=event_id or alpha_vantage_daily_event_id(symbol, event_date),
        symbol=symbol,
        price=price,
        volume=1_000,
        ts_event=datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc),
        ts_ingest=ingested_at,
        source=source,
    )


def _fetching(
    rows: list[tuple[date, float]],
) -> Any:
    def fetcher(**kwargs: Any) -> list[MarketEvent]:
        return [
            _event(event_date, kwargs["ingested_at"], price=price)
            for event_date, price in rows
        ]

    return fetcher


def _run(
    tmp_path: Path,
    *,
    fetcher: Any,
    clock: Any = lambda: RUN_TIME,
    writer: Any = None,
) -> dict[str, Any]:
    storage_root = tmp_path / "storage"
    return ingest_alpha_vantage_daily(
        symbol="IBM",
        start_date=date(2025, 1, 1),
        end_date=END_DATE,
        max_records=31,
        storage_config_path=tmp_path / "unused.yaml",
        environment={ingest_module.API_KEY_ENV: SECRET},
        fetcher=fetcher,
        writer=writer,
        config_loader=lambda _: build_storage_config(storage_root),
        clock=clock,
    )


def _raw_rows(tmp_path: Path) -> list[tuple[str, float, int]]:
    files = sorted((tmp_path / "storage" / "raw" / "market_events").rglob("*.parquet"))
    if not files:
        return []
    with duckdb.connect() as connection:
        return connection.execute(
            "SELECT event_id, price, epoch_us(ts_ingest) "
            "FROM read_parquet(?, union_by_name=true, hive_partitioning=false) "
            "ORDER BY event_id",
            [[str(path) for path in files]],
        ).fetchall()


def test_parser_requires_explicit_supported_source_and_end_date() -> None:
    parser = ingest_module._build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--symbol", "IBM", "--end-date", "2025-01-31"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--source",
                "other",
                "--symbol",
                "IBM",
                "--end-date",
                "2025-01-31",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(["--source", "alpha_vantage", "--symbol", "IBM"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--source",
                "alpha_vantage",
                "--symbol",
                "IBM",
                "--end-date",
                "2025-W01-1",
            ]
        )


@pytest.mark.parametrize(
    ("environment", "end_date", "symbol", "max_records", "message"),
    [
        ({}, END_DATE, "IBM", 31, "API key is required"),
        ({ingest_module.API_KEY_ENV: "bad\nkey"}, END_DATE, "IBM", 31, "invalid format"),
        ({ingest_module.API_KEY_ENV: SECRET}, RUN_TIME.date(), "IBM", 31, "earlier than"),
        (
            {ingest_module.API_KEY_ENV: SECRET},
            RUN_TIME.date() + timedelta(days=1),
            "IBM",
            31,
            "earlier than",
        ),
        ({ingest_module.API_KEY_ENV: SECRET}, END_DATE, "ß", 31, "unsupported characters"),
        ({ingest_module.API_KEY_ENV: SECRET}, END_DATE, "IBM", 0, "between 1 and 100"),
    ],
)
def test_local_request_failures_happen_before_config_fetch_or_write(
    tmp_path: Path,
    environment: dict[str, str],
    end_date: date,
    symbol: str,
    max_records: Any,
    message: str,
) -> None:
    calls: list[str] = []

    with pytest.raises(IngestionError, match=message):
        ingest_alpha_vantage_daily(
            symbol=symbol,
            start_date=None,
            end_date=end_date,
            max_records=max_records,
            storage_config_path=tmp_path / "storage.yaml",
            environment=environment,
            fetcher=lambda **_: calls.append("fetch") or [],
            writer=lambda *_args, **_kwargs: calls.append("write") or 0,
            config_loader=lambda _: calls.append("config") or build_storage_config(tmp_path),
            clock=lambda: RUN_TIME,
        )

    assert calls == []


def test_non_integer_record_limit_fails_before_config_fetch_or_write(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    with pytest.raises(IngestionError, match="between 1 and 100"):
        ingest_alpha_vantage_daily(
            symbol="IBM",
            start_date=None,
            end_date=END_DATE,
            max_records=1.5,  # type: ignore[arg-type]
            storage_config_path=tmp_path / "storage.yaml",
            environment={ingest_module.API_KEY_ENV: SECRET},
            fetcher=lambda **_: calls.append("fetch") or [],
            writer=lambda *_args, **_kwargs: calls.append("write") or 0,
            config_loader=lambda _: calls.append("config") or build_storage_config(tmp_path),
            clock=lambda: RUN_TIME,
        )

    assert calls == []


def test_invalid_storage_config_fails_before_provider_call(tmp_path: Path) -> None:
    calls: list[str] = []

    def invalid_config(_: Path) -> dict[str, Any]:
        calls.append("config")
        raise StorageError("invalid storage config")

    with pytest.raises(StorageError, match="Storage configuration is invalid"):
        ingest_alpha_vantage_daily(
            symbol="IBM",
            start_date=None,
            end_date=END_DATE,
            max_records=31,
            storage_config_path=tmp_path / "storage.yaml",
            environment={ingest_module.API_KEY_ENV: SECRET},
            fetcher=lambda **_: calls.append("fetch") or [],
            writer=lambda *_args, **_kwargs: calls.append("write") or 0,
            config_loader=invalid_config,
            clock=lambda: RUN_TIME,
        )

    assert calls == ["config"]


def test_runtime_invalid_raw_destinations_fail_before_provider_call(
    tmp_path: Path,
) -> None:
    cases: list[dict[str, Any]] = []
    for index in range(4):
        storage_root = tmp_path / f"storage-{index}"
        config = build_storage_config(storage_root)
        cases.append(config)

    cases[0]["storage"]["format"] = "csv"
    cases[1]["storage"]["raw"]["dataset"] = "../escape"
    cases[2]["storage"]["raw"]["base_path"] = str(tmp_path / "outside")
    file_dataset = Path(cases[3]["storage"]["raw"]["base_path"]) / "market_events"
    file_dataset.parent.mkdir(parents=True)
    file_dataset.write_text("not a directory", encoding="utf-8")

    for config in cases:
        calls: list[str] = []
        with pytest.raises(StorageError, match="Storage configuration is invalid"):
            ingest_alpha_vantage_daily(
                symbol="IBM",
                start_date=None,
                end_date=END_DATE,
                max_records=31,
                storage_config_path=tmp_path / "unused.yaml",
                environment={ingest_module.API_KEY_ENV: SECRET},
                fetcher=lambda **_: calls.append("fetch") or [],
                writer=lambda *_args, **_kwargs: calls.append("write") or 0,
                config_loader=lambda _, configured=config: configured,
                clock=lambda: RUN_TIME,
            )
        assert calls == []


def test_provider_failure_and_empty_selection_do_not_write(
    tmp_path: Path,
) -> None:
    writer_calls = 0

    def writer(*_args: Any, **_kwargs: Any) -> int:
        nonlocal writer_calls
        writer_calls += 1
        return 0

    def failed_fetch(**_kwargs: Any) -> list[MarketEvent]:
        raise IngestionError("Alpha Vantage rate limit was reached")

    for fetcher, message in (
        (failed_fetch, "source request failed"),
        (lambda **_: [], "no daily records"),
    ):
        with pytest.raises(IngestionError, match=message):
            _run(
                tmp_path,
                fetcher=fetcher,
                writer=writer,
            )

    assert writer_calls == 0


def test_adapter_cannot_exceed_requested_record_limit(tmp_path: Path) -> None:
    writer_calls = 0

    def writer(*_args: Any, **_kwargs: Any) -> int:
        nonlocal writer_calls
        writer_calls += 1
        return 0

    with pytest.raises(IngestionError, match="exceeded the requested record limit"):
        ingest_alpha_vantage_daily(
            symbol="IBM",
            start_date=None,
            end_date=END_DATE,
            max_records=1,
            storage_config_path=tmp_path / "unused.yaml",
            environment={ingest_module.API_KEY_ENV: SECRET},
            fetcher=_fetching(
                [
                    (date(2025, 1, 2), 100.0),
                    (date(2025, 1, 3), 101.0),
                ]
            ),
            writer=writer,
            config_loader=lambda _: build_storage_config(tmp_path / "storage"),
            clock=lambda: RUN_TIME,
        )

    assert writer_calls == 0


@pytest.mark.parametrize("count", [True, 0.5, -1, 2])
def test_writer_must_return_an_integer_within_the_selected_count(
    tmp_path: Path,
    count: Any,
) -> None:
    with pytest.raises(
        ingest_module._RawPublicationAmbiguousError,
        match="may have been committed; rerun is safe",
    ):
        _run(
            tmp_path,
            fetcher=_fetching([(date(2025, 1, 2), 100.0)]),
            writer=lambda *_args, **_kwargs: count,
        )


def test_first_ingest_writes_one_raw_partition_and_credential_free_evidence(
    tmp_path: Path,
) -> None:
    summary = _run(
        tmp_path,
        fetcher=_fetching(
            [
                (date(2025, 1, 2), 100.0),
                (date(2025, 1, 3), 101.0),
            ]
        ),
    )

    raw_output = summary["raw_output"]
    assert raw_output == {
        "dataset": "market_events",
        "location": "local_parquet:market_events",
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
        "partitions_written": [partition_path(RUN_TIME)],
    }
    assert summary["selection"] == {
        "first_event_date": "2025-01-02",
        "last_event_date": "2025-01-03",
    }
    assert summary["source"]["semantics"] == "normalized_daily_close"
    assert len(_raw_rows(tmp_path)) == 2
    assert not (tmp_path / "storage" / "curated").exists()

    serialized = json.dumps(summary)
    assert SECRET not in serialized
    assert "apikey" not in serialized.lower()
    assert "https://" not in serialized
    assert "curated" not in serialized
    assert "volatility" not in serialized


def test_full_replay_preserves_first_ingest_and_reports_no_written_partition(
    tmp_path: Path,
) -> None:
    fetcher = _fetching([(date(2025, 1, 2), 100.0)])
    first = _run(tmp_path, fetcher=fetcher)
    later = RUN_TIME + timedelta(days=1)
    replay = _run(tmp_path, fetcher=fetcher, clock=lambda: later)

    assert first["raw_output"]["records_written"] == 1
    assert replay["raw_output"]["records_selected"] == 1
    assert replay["raw_output"]["records_written"] == 0
    assert replay["raw_output"]["records_already_present"] == 1
    assert replay["raw_output"]["partitions_written"] == []
    assert _raw_rows(tmp_path) == [
        (
            alpha_vantage_daily_event_id("IBM", date(2025, 1, 2)),
            100.0,
            int(RUN_TIME.timestamp() * 1_000_000),
        )
    ]


def test_mixed_replay_and_new_bar_reconciles_counts(tmp_path: Path) -> None:
    _run(tmp_path, fetcher=_fetching([(date(2025, 1, 2), 100.0)]))
    later = RUN_TIME + timedelta(days=1)

    summary = _run(
        tmp_path,
        fetcher=_fetching(
            [
                (date(2025, 1, 2), 100.0),
                (date(2025, 1, 3), 101.0),
            ]
        ),
        clock=lambda: later,
    )

    assert summary["raw_output"]["records_selected"] == 2
    assert summary["raw_output"]["records_written"] == 1
    assert summary["raw_output"]["records_already_present"] == 1
    assert summary["raw_output"]["partitions_written"] == [partition_path(later)]
    assert len(_raw_rows(tmp_path)) == 2


def test_correction_with_unseen_bar_publishes_nothing_from_failed_call(
    tmp_path: Path,
) -> None:
    _run(tmp_path, fetcher=_fetching([(date(2025, 1, 2), 100.0)]))

    with pytest.raises(RawEventConflictError):
        _run(
            tmp_path,
            fetcher=_fetching(
                [
                    (date(2025, 1, 2), 999.0),
                    (date(2025, 1, 3), 101.0),
                ]
            ),
            clock=lambda: RUN_TIME + timedelta(days=1),
        )

    assert _raw_rows(tmp_path) == [
        (
            alpha_vantage_daily_event_id("IBM", date(2025, 1, 2)),
            100.0,
            int(RUN_TIME.timestamp() * 1_000_000),
        )
    ]


@pytest.mark.parametrize("invalid_events", ["duplicate", "partitions"])
def test_adapter_invariants_fail_before_writer(
    tmp_path: Path,
    invalid_events: str,
) -> None:
    first = _event(date(2025, 1, 2), RUN_TIME)
    if invalid_events == "duplicate":
        events = [first, first.model_copy()]
        message = "duplicate event IDs"
    else:
        events = [
            first,
            _event(date(2025, 1, 3), RUN_TIME + timedelta(hours=1)),
        ]
        message = "captured ingest timestamp"
    writer_calls = 0

    def writer(*_args: Any, **_kwargs: Any) -> int:
        nonlocal writer_calls
        writer_calls += 1
        return 0

    with pytest.raises(IngestionError, match=message):
        _run(tmp_path, fetcher=lambda **_: events, writer=writer)
    assert writer_calls == 0


@pytest.mark.parametrize(
    ("event", "message"),
    [
        (
            _event(
                date(2025, 1, 2),
                RUN_TIME,
                event_id="av-daily-wrong-identity",
            ),
            "unexpected event identity",
        ),
        (
            _event(date(2025, 1, 2), RUN_TIME, symbol="ibm"),
            "unexpected symbol",
        ),
    ],
)
def test_adapter_identity_contract_fails_before_writer(
    tmp_path: Path,
    event: MarketEvent,
    message: str,
) -> None:
    writer_calls = 0

    def writer(*_args: Any, **_kwargs: Any) -> int:
        nonlocal writer_calls
        writer_calls += 1
        return 0

    with pytest.raises(IngestionError, match=message):
        _run(tmp_path, fetcher=lambda **_: [event], writer=writer)
    assert writer_calls == 0


def test_provider_and_writer_errors_are_sanitized_without_hidden_context(
    tmp_path: Path,
) -> None:
    secret_url = f"https://example.invalid/query?apikey={SECRET}"

    def failed_fetch(**_kwargs: Any) -> list[MarketEvent]:
        raise IngestionError(secret_url)

    with pytest.raises(IngestionError) as provider_error:
        _run(tmp_path, fetcher=failed_fetch)
    rendered_provider = "".join(
        traceback.format_exception(
            type(provider_error.value),
            provider_error.value,
            provider_error.value.__traceback__,
        )
    )
    assert SECRET not in rendered_provider
    assert "https://" not in rendered_provider
    assert provider_error.value.__context__ is None

    def failed_writer(*_args: Any, **_kwargs: Any) -> int:
        raise StorageError(secret_url)

    with pytest.raises(ingest_module._RawPublicationAmbiguousError) as writer_error:
        _run(
            tmp_path,
            fetcher=_fetching([(date(2025, 1, 2), 100.0)]),
            writer=failed_writer,
        )
    rendered_writer = "".join(
        traceback.format_exception(
            type(writer_error.value),
            writer_error.value,
            writer_error.value.__traceback__,
        )
    )
    assert SECRET not in rendered_writer
    assert "https://" not in rendered_writer
    assert writer_error.value.__context__ is None


def test_post_commit_writer_failure_is_reported_as_ambiguous_and_replay_is_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_metadata = raw_writer_module._metadata

    def fail_after_publication(path: Path, *, message: str) -> Any:
        if message == "Published raw parquet metadata is unreadable":
            raise StorageError("untrusted post-publication failure")
        return original_metadata(path, message=message)

    monkeypatch.setattr(raw_writer_module, "_metadata", fail_after_publication)
    with pytest.raises(
        ingest_module._RawPublicationAmbiguousError,
        match="may have been committed; rerun is safe",
    ) as captured:
        _run(
            tmp_path,
            fetcher=_fetching([(date(2025, 1, 2), 100.0)]),
        )

    assert captured.value.__context__ is None
    assert len(_raw_rows(tmp_path)) == 1

    monkeypatch.setattr(raw_writer_module, "_metadata", original_metadata)
    replay = _run(
        tmp_path,
        fetcher=_fetching([(date(2025, 1, 2), 100.0)]),
    )
    assert replay["raw_output"]["records_written"] == 0


def test_cli_reports_an_ambiguous_raw_publication_without_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_url = f"https://example.invalid/query?apikey={SECRET}"

    def fail(**_kwargs: Any) -> dict[str, Any]:
        error = ingest_module._RawPublicationAmbiguousError(secret_url)
        raise error

    monkeypatch.setattr(ingest_module, "ingest_alpha_vantage_daily", fail)
    result = ingest_module.main(
        [
            "--source",
            "alpha_vantage",
            "--symbol",
            "IBM",
            "--end-date",
            "2025-01-31",
        ]
    )

    output = capsys.readouterr()
    assert result == 1
    assert "may have been committed" in output.err
    assert "rerun is safe" in output.err
    assert SECRET not in output.err
    assert "https://" not in output.err


def test_hostile_mutated_event_error_is_not_treated_as_a_trusted_contract_message(
    tmp_path: Path,
) -> None:
    secret_url = f"https://example.invalid/query?apikey={SECRET}"

    class HostileEventId(str):
        def __hash__(self) -> int:
            raise IngestionError(secret_url)

    event = _event(date(2025, 1, 2), RUN_TIME)
    event.event_id = HostileEventId(event.event_id)

    with pytest.raises(IngestionError) as captured:
        _run(tmp_path, fetcher=lambda **_: [event])

    rendered = "".join(
        traceback.format_exception(
            type(captured.value),
            captured.value,
            captured.value.__traceback__,
        )
    )
    assert str(captured.value) == "Alpha Vantage source returned invalid daily data"
    assert SECRET not in rendered
    assert "https://" not in rendered
    assert captured.value.__context__ is None


def test_cli_never_prints_unexpected_exception_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_url = f"https://example.invalid/query?apikey={SECRET}"

    def fail(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(secret_url)

    monkeypatch.setattr(ingest_module, "ingest_alpha_vantage_daily", fail)
    result = ingest_module.main(
        [
            "--source",
            "alpha_vantage",
            "--symbol",
            "IBM",
            "--end-date",
            "2025-01-31",
        ]
    )

    output = capsys.readouterr()
    assert result == 1
    assert "unexpected local failure" in output.err
    assert SECRET not in output.err
    assert "https://" not in output.err


def test_cli_reads_environment_key_and_uses_no_live_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    storage_root = tmp_path / "storage"
    storage_root.mkdir()
    config_path = write_storage_config(storage_root)
    captured_keys: list[str] = []

    def fetcher(**kwargs: Any) -> list[MarketEvent]:
        captured_keys.append(kwargs["api_key"])
        return [_event(date(2025, 1, 2), kwargs["ingested_at"])]

    monkeypatch.setenv(ingest_module.API_KEY_ENV, SECRET)
    monkeypatch.setattr(ingest_module, "utc_now", lambda: RUN_TIME)
    monkeypatch.setattr(ingest_module, "fetch_alpha_vantage_daily_events", fetcher)

    result = ingest_module.main(
        [
            "--source",
            "alpha_vantage",
            "--symbol",
            "IBM",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-31",
            "--max-records",
            "31",
            "--storage-config",
            str(config_path),
        ]
    )

    output = capsys.readouterr()
    assert result == 0
    assert captured_keys == [SECRET]
    assert SECRET not in output.out
    assert output.err == ""
    parsed = json.loads(output.out)
    assert parsed["raw_output"]["records_written"] == 1
    assert parsed["source"]["symbol"] == "IBM"
