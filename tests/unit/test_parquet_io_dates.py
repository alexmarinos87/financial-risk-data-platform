from datetime import date

from src.storage.parquet_io import (
    batch_file_name,
)


def test_batch_file_name_supports_calendar_dates() -> None:
    first = batch_file_name(
        [
            {
                "policy_effective_from": date(
                    2026,
                    1,
                    1,
                ),
                "policy_effective_to": date(
                    2026,
                    7,
                    1,
                ),
            }
        ],
        "parquet",
    )
    second = batch_file_name(
        [
            {
                "policy_effective_to": date(
                    2026,
                    7,
                    1,
                ),
                "policy_effective_from": date(
                    2026,
                    1,
                    1,
                ),
            }
        ],
        "parquet",
    )
    assert first == second
    assert first.endswith(".parquet")
