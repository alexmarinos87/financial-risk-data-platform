from __future__ import annotations

from pathlib import Path
from typing import Any

from .run_pipeline import run_pipeline


def run_glue_job(
    job_name: str,
    *,
    input_path: Path | None = None,
    thresholds_path: Path = Path("config/risk_thresholds.yaml"),
    contracts_path: Path = Path("config/data_contracts.yaml"),
    output_root: Path = Path("data_lake"),
    run_id: str | None = None,
) -> dict[str, Any]:
    return run_pipeline(
        input_path=input_path,
        thresholds_path=thresholds_path,
        contracts_path=contracts_path,
        output_root=output_root,
        run_id=run_id or f"glue-{job_name}",
        fail_on_dq=True,
        late_seconds=300,
        window_minutes=5,
        vol_window=5,
    )
