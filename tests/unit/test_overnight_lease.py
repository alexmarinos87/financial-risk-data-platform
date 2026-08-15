from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import stat
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import scripts.overnight_lease as lease
from scripts.overnight_manifest_verifier import (
    ProtectedBaseManifest,
    ValidatedManifestContract,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)
POLICY_SHA = "b" * 64


def _manifest(
    manifest_id: str = "lease-test-0001",
    manifest_sha256: str = "a" * 64,
    *,
    expires: datetime = NOW + timedelta(hours=4),
    runtime_minutes: int = 120,
) -> ProtectedBaseManifest:
    record_key = hashlib.sha256(manifest_id.encode("ascii")).hexdigest()
    contract = ValidatedManifestContract(
        manifest_id=manifest_id,
        authorization_issued_at=NOW - timedelta(hours=1),
        authorization_expires=expires,
        arc42_primary_block="analytics",
        context_goal="Exercise one lease",
        allowed_paths=("src/analytics/example.py", "tests/unit/test_example.py"),
        runtime_scenario="One trusted controller owns the local registry",
        quality_scenario="A concurrent controller cannot become an owner",
        recovery_scenario="Incomplete state remains for human recovery",
        acceptance_criteria=("One owner is durable",),
        validation_targets=(
            "security-check",
            "quality-check",
            "readiness-check",
            "git-diff-check",
        ),
        maximum_changed_lines=100,
        maximum_changed_files=2,
        maximum_commits=1,
        maximum_pushes=1,
        maximum_runtime_minutes=runtime_minutes,
    )
    return ProtectedBaseManifest(
        contract=contract,
        base_commit_sha="1" * 40,
        base_tree_sha="2" * 40,
        manifest_path=f"docs/overnight-manifests/{manifest_id}.json",
        git_blob_oid="3" * 40,
        manifest_sha256=manifest_sha256,
        record_key=record_key,
        object_format="sha1",
        verified_at=NOW,
    )


def _common_dir(tmp_path: Path) -> Path:
    common = tmp_path / "repository.git"
    common.mkdir(mode=0o700)
    return common


def _acquire(common: Path, manifest: ProtectedBaseManifest | None = None) -> lease.AcquireResult:
    return lease.acquire_repository_lease(
        common,
        _manifest() if manifest is None else manifest,
        policy_sha256=POLICY_SHA,
        now=NOW,
    )


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii")


def _tree_bytes(root: Path) -> dict[str, tuple[int, bytes]]:
    snapshot: dict[str, tuple[int, bytes]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        info = path.lstat()
        if stat.S_ISREG(info.st_mode):
            snapshot[relative] = (stat.S_IMODE(info.st_mode), path.read_bytes())
        else:
            snapshot[relative] = (stat.S_IFMT(info.st_mode) | stat.S_IMODE(info.st_mode), b"")
    return snapshot


def _owned(common: Path) -> lease.LeaseHandle:
    result = _acquire(common)
    assert result.status is lease.AcquireStatus.OWNED
    assert result.handle is not None
    return result.handle


def _race_worker(common: str, start: Any, output: Any) -> None:
    start.wait()
    result = lease.acquire_repository_lease(
        Path(common), _manifest(), policy_sha256=POLICY_SHA, now=NOW
    )
    output.put(result.status.value)


def test_lifecycle_is_durable_redacted_and_single_use(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    acquired = _acquire(common)

    assert acquired.status is lease.AcquireStatus.OWNED
    assert acquired.handle is not None
    handle = acquired.handle
    evidence = acquired.redacted_evidence()
    assert evidence["publication_authorized"] is False
    assert evidence["candidate_isolation_verified"] is False
    assert evidence["publisher_verified"] is False
    assert "git_common_dir" not in evidence
    assert "process_id" not in evidence
    assert "fence_nonce" not in evidence
    assert handle.fence_nonce.hex() not in repr(handle)
    disk = b"".join(
        path.read_bytes()
        for path in (common / lease.REGISTRY_NAME).rglob("*.json")
    )
    assert handle.fence_nonce.hex().encode("ascii") not in disk
    assert handle.fence_digest_sha256.encode("ascii") in disk

    checkpoint = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(minutes=1))
    assert checkpoint.status is lease.CheckpointStatus.CHECKPOINTED
    assert checkpoint.handle is not None
    handle = checkpoint.handle
    assert handle.phase == 1

    finalized = lease.finalize_repository_lease(
        handle,
        outcome="completed",
        external_state="none",
        now=NOW + timedelta(minutes=2),
    )
    assert finalized.status is lease.FinalizeStatus.FINALIZED
    registry = common / lease.REGISTRY_NAME
    assert not (registry / "active").exists()
    terminal = registry / "terminal" / handle.record_key
    history = registry / "history" / handle.run_id
    assert (terminal / "result.json").is_file()
    assert (history / "release.json").is_file()
    assert json.loads((terminal / "result.json").read_bytes())["external_state"] == "none"

    duplicate = _acquire(common)
    assert duplicate.status is lease.AcquireStatus.DUPLICATE_NOOP
    assert duplicate.handle is None
    assert not (registry / "active").exists()
    assert len(list((registry / "history").iterdir())) == 2


def test_manifest_id_reuse_with_changed_blob_is_denied(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    assert lease.finalize_repository_lease(
        handle, outcome="completed", external_state="none", now=NOW
    ).status is lease.FinalizeStatus.FINALIZED

    result = _acquire(common, _manifest(manifest_sha256="c" * 64))

    assert result.status is lease.AcquireStatus.DENIED_ID_REUSE
    assert not (common / lease.REGISTRY_NAME / "active").exists()


def test_existing_active_is_never_mutated_or_taken_over(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    _owned(common)
    registry = common / lease.REGISTRY_NAME
    before = _tree_bytes(registry)

    second = _acquire(common, _manifest("another-lease-0002", "c" * 64))

    assert second.status is lease.AcquireStatus.BUSY_NOOP
    assert _tree_bytes(registry) == before


def test_existing_active_fences_expired_or_malformed_new_inputs(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    _owned(common)
    registry = common / lease.REGISTRY_NAME
    before = _tree_bytes(registry)

    expired = lease.acquire_repository_lease(
        common,
        _manifest(expires=NOW - timedelta(seconds=1)),
        policy_sha256=POLICY_SHA,
        now=NOW,
    )
    malformed = lease.acquire_repository_lease(
        common, _manifest(), policy_sha256="not-a-hash", now=NOW
    )

    assert expired.status is lease.AcquireStatus.BUSY_NOOP
    assert malformed.status is lease.AcquireStatus.BUSY_NOOP
    assert _tree_bytes(registry) == before


def test_active_disappearing_after_observation_is_still_a_busy_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = _common_dir(tmp_path)
    _owned(common)
    registry = common / lease.REGISTRY_NAME
    original = lease._list
    observations = 0

    def archive_after_observation(parent_fd: int) -> list[str]:
        nonlocal observations
        result = original(parent_fd)
        if "active" in result:
            observations += 1
            history_fd = os.open(
                registry / "history",
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            )
            try:
                os.rename(
                    "active",
                    "00000000-0000-4000-8000-000000000001",
                    src_dir_fd=parent_fd,
                    dst_dir_fd=history_fd,
                )
            finally:
                os.close(history_fd)
        return result

    monkeypatch.setattr(lease, "_list", archive_after_observation)
    result = _acquire(common, _manifest("another-lease-0002", "c" * 64))

    assert result.status is lease.AcquireStatus.BUSY_NOOP
    assert observations == 1


def test_multiprocess_race_has_exactly_one_owner(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    context = multiprocessing.get_context("fork")
    start = context.Event()
    output = context.Queue()
    processes = [
        context.Process(target=_race_worker, args=(str(common), start, output))
        for _ in range(8)
    ]
    for process in processes:
        process.start()
    start.set()
    statuses = [output.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    assert statuses.count(lease.AcquireStatus.OWNED.value) == 1
    assert set(statuses) <= {
        lease.AcquireStatus.OWNED.value,
        lease.AcquireStatus.BUSY_NOOP.value,
    }


def test_preclaim_mkdir_collision_is_reclassified_as_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = _common_dir(tmp_path)
    original = lease._create_claim

    def collide(
        claims_fd: int, registry: lease._Registry, handle: lease.LeaseHandle
    ) -> int:
        created_fd = original(claims_fd, registry, handle)
        os.close(created_fd)
        raise FileExistsError

    monkeypatch.setattr(lease, "_create_claim", collide)
    result = _acquire(common)

    assert result.status is lease.AcquireStatus.DUPLICATE_NOOP
    registry = common / lease.REGISTRY_NAME
    assert not (registry / "active").exists()
    assert len(list((registry / "claims").iterdir())) == 1


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema_version":1,"schema_version":1}',
        b'{"value":NaN}',
        b'{ "value": 1 }',
        b'\xff',
    ],
)
def test_noncanonical_or_ambiguous_owner_json_requires_recovery(
    tmp_path: Path, raw: bytes
) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    owner = common / lease.REGISTRY_NAME / "active" / "owner.json"
    owner.write_bytes(raw)
    owner.chmod(0o600)

    before = _tree_bytes(common / lease.REGISTRY_NAME)
    result = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=1))

    assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert _tree_bytes(common / lease.REGISTRY_NAME) == before


def test_immutable_binding_tamper_requires_recovery(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    owner = common / lease.REGISTRY_NAME / "active" / "owner.json"
    value = json.loads(owner.read_bytes())
    value["policy_sha256"] = "d" * 64
    owner.write_bytes(_canonical(value))
    owner.chmod(0o600)

    result = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=1))

    assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert (common / lease.REGISTRY_NAME / "active").is_dir()


@pytest.mark.parametrize(
    ("field", "value"),
    [("started_at", "2020-01-01T00:00:00Z"), ("process_id", 999_999)],
)
def test_owner_start_and_process_identity_are_exact_bindings(
    tmp_path: Path, field: str, value: Any
) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    owner = common / lease.REGISTRY_NAME / "active" / "owner.json"
    record = json.loads(owner.read_bytes())
    record[field] = value
    owner.write_bytes(_canonical(record))
    owner.chmod(0o600)

    result = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=1))

    assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert (common / lease.REGISTRY_NAME / "active").is_dir()


def test_boolean_schema_version_is_not_integer_one(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    owner = common / lease.REGISTRY_NAME / "active" / "owner.json"
    record = json.loads(owner.read_bytes())
    record["schema_version"] = True
    owner.write_bytes(_canonical(record))
    owner.chmod(0o600)

    assert lease.checkpoint_repository_lease(
        handle, now=NOW + timedelta(seconds=1)
    ).status is lease.CheckpointStatus.RECOVERY_REQUIRED


@pytest.mark.parametrize(
    "raw",
    [
        ('{"process_id":' + "9" * 5_000 + "}").encode("ascii"),
        ("[" * 1_200 + "]" * 1_200).encode("ascii"),
    ],
)
def test_pathological_json_is_a_fixed_recovery_result(tmp_path: Path, raw: bytes) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    owner = common / lease.REGISTRY_NAME / "active" / "owner.json"
    owner.write_bytes(raw)
    owner.chmod(0o600)

    result = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=1))

    assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED


def test_wrong_and_replayed_nonce_never_mutate_state(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    wrong = replace(handle, fence_nonce=b"x" * 32)
    before = _tree_bytes(common / lease.REGISTRY_NAME)

    denied = lease.checkpoint_repository_lease(wrong, now=NOW + timedelta(seconds=1))

    assert denied.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert _tree_bytes(common / lease.REGISTRY_NAME) == before
    assert lease.finalize_repository_lease(
        handle,
        outcome="implementation_failed",
        external_state="none",
        now=NOW + timedelta(seconds=2),
    ).status is lease.FinalizeStatus.FINALIZED
    replay = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=3))
    assert replay.status is lease.CheckpointStatus.RECOVERY_REQUIRED


def test_deadline_never_extends_and_expired_checkpoint_is_read_only(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _acquire(common, _manifest(runtime_minutes=1)).handle
    assert handle is not None
    before = _tree_bytes(common / lease.REGISTRY_NAME)

    expired = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(minutes=1))

    assert expired.status is lease.CheckpointStatus.DEADLINE_EXPIRED
    assert _tree_bytes(common / lease.REGISTRY_NAME) == before
    assert lease.finalize_repository_lease(
        handle,
        outcome="publication_ambiguous",
        external_state="unknown",
        now=NOW + timedelta(minutes=2),
    ).status is lease.FinalizeStatus.FINALIZED


def test_checkpoint_clock_rollback_is_rejected_without_writing(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    first = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(minutes=2))
    assert first.handle is not None
    before = _tree_bytes(common / lease.REGISTRY_NAME)

    rollback = lease.checkpoint_repository_lease(
        first.handle, now=NOW + timedelta(minutes=1)
    )

    assert rollback.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert _tree_bytes(common / lease.REGISTRY_NAME) == before


@pytest.mark.parametrize("attack", ["hardlink", "symlink", "mode", "directory", "fifo"])
def test_unsafe_owner_objects_require_recovery(tmp_path: Path, attack: str) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    owner = common / lease.REGISTRY_NAME / "active" / "owner.json"
    saved = owner.read_bytes()
    if attack == "mode":
        owner.chmod(0o644)
    else:
        owner.unlink()
        target = tmp_path / "target"
        if attack == "hardlink":
            target.write_bytes(saved)
            target.chmod(0o600)
            os.link(target, owner)
        elif attack == "symlink":
            target.write_bytes(saved)
            target.chmod(0o600)
            owner.symlink_to(target)
        elif attack == "directory":
            owner.mkdir(mode=0o700)
        else:
            os.mkfifo(owner, mode=0o600)

    result = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=1))

    assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert (common / lease.REGISTRY_NAME / "active").exists()


def test_wrong_active_type_or_mode_blocks_without_mutation(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    registry = common / lease.REGISTRY_NAME
    registry.mkdir(mode=0o700)
    for name in ("claims", "history", "terminal"):
        (registry / name).mkdir(mode=0o700)
    active = registry / "active"
    active.write_bytes(b"do-not-touch")
    active.chmod(0o600)
    before = _tree_bytes(registry)

    result = _acquire(common)

    assert result.status is lease.AcquireStatus.BUSY_NOOP
    assert _tree_bytes(registry) == before


@pytest.mark.parametrize("attack", ["symlink", "regular", "mode"])
def test_unsafe_collection_objects_return_fixed_recovery_without_mutation(
    tmp_path: Path, attack: str
) -> None:
    common = _common_dir(tmp_path)
    registry = common / lease.REGISTRY_NAME
    registry.mkdir(mode=0o700)
    for name in ("claims", "history"):
        (registry / name).mkdir(mode=0o700)
    terminal = registry / "terminal"
    if attack == "symlink":
        target = tmp_path / "terminal-target"
        target.mkdir(mode=0o700)
        terminal.symlink_to(target, target_is_directory=True)
    elif attack == "regular":
        terminal.write_bytes(b"not-a-directory")
        terminal.chmod(0o600)
    else:
        terminal.mkdir(mode=0o700)
        terminal.chmod(0o755)
    before = _tree_bytes(registry)

    result = _acquire(common)

    assert result.status is lease.AcquireStatus.RECOVERY_REQUIRED
    assert _tree_bytes(registry) == before


def test_partial_owned_directory_open_does_not_leak_file_descriptors(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    terminal = common / lease.REGISTRY_NAME / "terminal"
    terminal.rmdir()
    terminal.write_bytes(b"not-a-directory")
    terminal.chmod(0o600)
    before = len(os.listdir("/proc/self/fd"))

    for offset in range(20):
        result = lease.checkpoint_repository_lease(
            handle, now=NOW + timedelta(seconds=offset + 1)
        )
        assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED

    assert len(os.listdir("/proc/self/fd")) == before


def test_checkpoint_partial_write_retains_fenced_recovery_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    original = lease._write_json_exclusive
    calls = 0

    def fail_second_heartbeat(
        parent_fd: int, name: str, value: dict[str, Any], device: int
    ) -> None:
        nonlocal calls
        if name == "00000000000000000001.json":
            calls += 1
            if calls == 2:
                raise lease._InvariantError
        original(parent_fd, name, value, device)

    monkeypatch.setattr(lease, "_write_json_exclusive", fail_second_heartbeat)
    result = lease.checkpoint_repository_lease(handle, now=NOW + timedelta(seconds=1))

    assert result.status is lease.CheckpointStatus.RECOVERY_REQUIRED
    assert (common / lease.REGISTRY_NAME / "active").is_dir()
    monkeypatch.setattr(lease, "_write_json_exclusive", original)
    assert lease.checkpoint_repository_lease(
        handle, now=NOW + timedelta(seconds=2)
    ).status is lease.CheckpointStatus.RECOVERY_REQUIRED


def test_result_write_failure_precedes_any_terminal_move(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    original = lease._write_json_exclusive

    def fail_result(parent_fd: int, name: str, value: dict[str, Any], device: int) -> None:
        if name == "result.json":
            raise lease._InvariantError
        original(parent_fd, name, value, device)

    monkeypatch.setattr(lease, "_write_json_exclusive", fail_result)
    result = lease.finalize_repository_lease(
        handle, outcome="validation_failed", external_state="none", now=NOW
    )

    registry = common / lease.REGISTRY_NAME
    assert result.status is lease.FinalizeStatus.RECOVERY_REQUIRED
    assert (registry / "active").is_dir()
    assert (registry / "claims" / handle.record_key).is_dir()
    assert not (registry / "terminal" / handle.record_key).exists()


def test_terminal_destination_collision_never_clobbers_or_releases_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    original = lease._rename_noreplace

    def collide(old_fd: int, old: str, new_fd: int, new: str) -> None:
        if old == handle.record_key:
            os.mkdir(new, 0o700, dir_fd=new_fd)
        original(old_fd, old, new_fd, new)

    monkeypatch.setattr(lease, "_rename_noreplace", collide)
    result = lease.finalize_repository_lease(
        handle, outcome="completed", external_state="none", now=NOW
    )

    registry = common / lease.REGISTRY_NAME
    assert result.status is lease.FinalizeStatus.RECOVERY_REQUIRED
    assert (registry / "active").is_dir()
    assert (registry / "claims" / handle.record_key / "result.json").is_file()
    assert (registry / "terminal" / handle.record_key).is_dir()


def test_history_collision_occurs_only_after_terminal_and_release_are_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    original = lease._rename_noreplace

    def collide(old_fd: int, old: str, new_fd: int, new: str) -> None:
        if old == "active":
            os.mkdir(new, 0o700, dir_fd=new_fd)
        original(old_fd, old, new_fd, new)

    monkeypatch.setattr(lease, "_rename_noreplace", collide)
    result = lease.finalize_repository_lease(
        handle, outcome="completed", external_state="draft_pr", now=NOW
    )

    registry = common / lease.REGISTRY_NAME
    assert result.status is lease.FinalizeStatus.RECOVERY_REQUIRED
    assert (registry / "terminal" / handle.record_key / "result.json").is_file()
    assert (registry / "active" / "release.json").is_file()
    assert (registry / "history" / handle.run_id).is_dir()


def test_malformed_terminal_heartbeat_retains_new_active_for_recovery(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    assert lease.finalize_repository_lease(
        handle, outcome="completed", external_state="none", now=NOW
    ).status is lease.FinalizeStatus.FINALIZED
    heartbeat = (
        common
        / lease.REGISTRY_NAME
        / "terminal"
        / handle.record_key
        / "heartbeats"
        / "00000000000000000000.json"
    )
    heartbeat.write_bytes(b'{"corrupted":true}')
    heartbeat.chmod(0o600)

    result = _acquire(common)

    assert result.status is lease.AcquireStatus.RECOVERY_REQUIRED
    assert (common / lease.REGISTRY_NAME / "active").is_dir()


def test_malformed_orphan_claim_heartbeat_retains_active_for_recovery(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    registry = common / lease.REGISTRY_NAME
    orphaned_active = tmp_path / "orphaned-active"
    (registry / "active").rename(orphaned_active)
    heartbeat = (
        registry
        / "claims"
        / handle.record_key
        / "heartbeats"
        / "00000000000000000000.json"
    )
    heartbeat.write_bytes(b'{"corrupted":true}')
    heartbeat.chmod(0o600)

    result = _acquire(common)

    assert result.status is lease.AcquireStatus.RECOVERY_REQUIRED
    assert (registry / "active").is_dir()


def test_final_evidence_binds_terminal_state_without_secret_or_path(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    handle = _owned(common)
    finalized = lease.finalize_repository_lease(
        handle,
        outcome="publication_ambiguous",
        external_state="unknown",
        now=NOW + timedelta(seconds=3),
    )

    evidence = finalized.redacted_evidence()

    assert evidence["manifest_sha256"] == handle.manifest_sha256
    assert evidence["policy_sha256"] == handle.policy_sha256
    assert evidence["fence_digest_sha256"] == handle.fence_digest_sha256
    assert evidence["finalized_at"] == "2026-08-15T20:00:03Z"
    assert "fence_nonce" not in evidence
    assert "git_common_dir" not in evidence
    assert "process_id" not in evidence
    assert handle.fence_nonce.hex() not in repr(finalized)


def test_acquisition_rejects_clock_rollback_and_malformed_public_inputs(
    tmp_path: Path,
) -> None:
    common = _common_dir(tmp_path)
    with pytest.raises(lease.LeaseProtocolError, match="LEASE_AUTHORIZATION_INACTIVE"):
        lease.acquire_repository_lease(
            common,
            _manifest(),
            policy_sha256=POLICY_SHA,
            now=NOW - timedelta(hours=2),
        )
    with pytest.raises(lease.LeaseProtocolError, match="LEASE_INPUT_INVALID"):
        invalid_common: Any = str(common)
        lease.acquire_repository_lease(
            invalid_common, _manifest(), policy_sha256=POLICY_SHA, now=NOW
        )
    handle = _owned(common)
    with pytest.raises(lease.LeaseProtocolError, match="LEASE_INPUT_INVALID"):
        lease.finalize_repository_lease(
            handle, outcome=[], external_state="none", now=NOW  # type: ignore[arg-type]
        )


def test_acquisition_revalidates_runtime_and_exact_manifest_times(tmp_path: Path) -> None:
    common = _common_dir(tmp_path)
    overlong = _manifest(runtime_minutes=121)
    with pytest.raises(lease.LeaseProtocolError, match="LEASE_INPUT_INVALID"):
        _acquire(common, overlong)
    assert not (common / lease.REGISTRY_NAME).exists()

    fractional = replace(_manifest(), verified_at=NOW + timedelta(microseconds=1))
    with pytest.raises(lease.LeaseProtocolError, match="LEASE_INPUT_INVALID"):
        _acquire(common, fractional)
    assert not (common / lease.REGISTRY_NAME).exists()

    contract = replace(
        _manifest().contract,
        authorization_issued_at=NOW + timedelta(minutes=1),
    )
    invalid_order = replace(_manifest(), contract=contract)
    with pytest.raises(lease.LeaseProtocolError, match="LEASE_AUTHORIZATION_INACTIVE"):
        _acquire(common, invalid_order)
    assert not (common / lease.REGISTRY_NAME).exists()


def test_common_dir_and_registry_symlinks_are_rejected(tmp_path: Path) -> None:
    real = _common_dir(tmp_path)
    linked = tmp_path / "linked.git"
    linked.symlink_to(real, target_is_directory=True)
    assert _acquire(linked).status is lease.AcquireStatus.RECOVERY_REQUIRED

    registry_target = tmp_path / "registry-target"
    registry_target.mkdir(mode=0o700)
    (real / lease.REGISTRY_NAME).symlink_to(registry_target, target_is_directory=True)
    assert _acquire(real).status is lease.AcquireStatus.RECOVERY_REQUIRED
