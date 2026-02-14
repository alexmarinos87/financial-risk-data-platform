from src.orchestration.backfill import run_backfill


def test_run_backfill_writes_manifest(tmp_path):
    output_root = tmp_path / "lake"
    manifest = run_backfill(
        "2025-01-01",
        "2025-01-03",
        output_root=output_root,
        fail_on_dq=False,
    )

    assert manifest["runs_requested"] == 3
    assert manifest["runs_completed"] == 3
    assert manifest["runs_failed"] == 0
    assert manifest["run_ids"] == [
        "backfill-2025-01-01",
        "backfill-2025-01-02",
        "backfill-2025-01-03",
    ]
    assert (output_root / "_runs" / "backfill-2025-01-01-2025-01-03.json").exists()
