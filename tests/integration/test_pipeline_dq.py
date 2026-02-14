from __future__ import annotations

from pathlib import Path

import pytest

from src.common.exceptions import DataQualityError
from src.orchestration.run_pipeline import run_pipeline


def _strict_thresholds_file(path: Path) -> Path:
    path.write_text(
        "\n".join(
            [
                "thresholds:",
                "  volatility_5m:",
                "    warn: 0.03",
                "    critical: 0.07",
                "  data_quality:",
                "    max_late_rate: 0.0",
                "    max_duplicate_rate: 0.0",
            ]
        ),
        encoding="utf-8",
    )
    return path


def test_run_pipeline_fails_when_dq_breaches_threshold(tmp_path):
    thresholds_path = _strict_thresholds_file(tmp_path / "thresholds.yaml")

    with pytest.raises(DataQualityError):
        run_pipeline(
            input_path=None,
            thresholds_path=thresholds_path,
            contracts_path=Path("config/data_contracts.yaml"),
            output_root=tmp_path / "lake",
            run_id="dq-fail-001",
            fail_on_dq=True,
            late_seconds=1,
            window_minutes=5,
            vol_window=5,
        )


def test_run_pipeline_can_continue_when_dq_breaches_threshold(tmp_path):
    thresholds_path = _strict_thresholds_file(tmp_path / "thresholds.yaml")
    output_root = tmp_path / "lake"

    summary = run_pipeline(
        input_path=None,
        thresholds_path=thresholds_path,
        contracts_path=Path("config/data_contracts.yaml"),
        output_root=output_root,
        run_id="dq-soft-001",
        fail_on_dq=False,
        late_seconds=1,
        window_minutes=5,
        vol_window=5,
    )

    assert summary["dq_passed"] is False
    assert summary["run_id"] == "dq-soft-001"
    assert (output_root / "_runs" / "dq-soft-001.json").exists()
