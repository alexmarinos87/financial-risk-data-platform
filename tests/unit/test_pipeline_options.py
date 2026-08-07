from __future__ import annotations

import sys
from pathlib import Path

import pytest

from src.orchestration.run_pipeline import main as run_pipeline_main
from src.orchestration.run_pipeline import run_pipeline


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--window-minutes", "0"),
        ("--window-minutes", "-1"),
        ("--vol-window", "0"),
        ("--vol-window", "-1"),
    ],
)
def test_cli_rejects_non_positive_window_options(
    option: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", ["run_pipeline", option, value])

    with pytest.raises(SystemExit) as exc_info:
        run_pipeline_main()

    assert exc_info.value.code == 2
    assert f"argument {option}: must be greater than zero" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("window_minutes", "vol_window", "expected_message"),
    [
        (0, 5, "window_minutes must be greater than zero"),
        (5, 0, "vol_window must be greater than zero"),
    ],
)
def test_run_pipeline_rejects_non_positive_windows_before_loading_input(
    tmp_path: Path,
    window_minutes: int,
    vol_window: int,
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        run_pipeline(
            input_path=tmp_path / "input-is-not-loaded.json",
            thresholds_path=tmp_path / "thresholds-are-not-loaded.yaml",
            late_seconds=60,
            window_minutes=window_minutes,
            vol_window=vol_window,
            storage_config_path=tmp_path / "storage-is-not-loaded.yaml",
        )
