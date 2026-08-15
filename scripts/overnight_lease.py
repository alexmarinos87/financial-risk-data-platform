from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import secrets
import stat
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn

from scripts.overnight_manifest_verifier import ProtectedBaseManifest


UTC = timezone.utc
REGISTRY_NAME = "overnight-candidates"
SCHEMA_VERSION = 1
MAX_RECORD_BYTES = 32 * 1024
MAX_REGISTRY_ENTRIES = 10_000
RENAME_NOREPLACE = 1
HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
RUN_ID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
MANIFEST_ID = re.compile(r"[a-z0-9][a-z0-9-]{7,63}")
PHASE_FILE = re.compile(r"([0-9]{20})\.json")
TERMINAL_OUTCOMES = frozenset({
    "completed",
    "implementation_failed",
    "publication_ambiguous",
    "validation_failed",
})
EXTERNAL_STATES = frozenset({"none", "branch_only", "draft_pr", "unknown"})

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
_READ_FLAGS = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW


class AcquireStatus(str, Enum):
    OWNED = "OWNED"
    BUSY_NOOP = "BUSY_NOOP"
    DUPLICATE_NOOP = "DUPLICATE_NOOP"
    DENIED_ID_REUSE = "DENIED_ID_REUSE"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class CheckpointStatus(str, Enum):
    CHECKPOINTED = "CHECKPOINTED"
    DEADLINE_EXPIRED = "DEADLINE_EXPIRED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class FinalizeStatus(str, Enum):
    FINALIZED = "FINALIZED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class LeaseProtocolError(Exception):
    """A fixed, non-sensitive caller or platform error."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _InvariantError(Exception):
    pass


@dataclass(frozen=True, slots=True, repr=False)
class LeaseHandle:
    git_common_dir: Path
    common_device: int
    common_inode: int
    manifest_id: str
    record_key: str
    manifest_sha256: str
    base_commit_sha: str
    policy_sha256: str
    run_id: str
    process_id: int
    fence_nonce: bytes
    fence_digest_sha256: str
    started_at: datetime
    deadline: datetime
    phase: int

    def redacted_evidence(self) -> dict[str, Any]:
        return {
            "status": "repository-lease-owned",
            "publication_authorized": False,
            "candidate_isolation_verified": False,
            "publisher_verified": False,
            "manifest_id": self.manifest_id,
            "record_key": self.record_key,
            "manifest_sha256": self.manifest_sha256,
            "base_commit_sha": self.base_commit_sha,
            "policy_sha256": self.policy_sha256,
            "run_id": self.run_id,
            "fence_digest_sha256": self.fence_digest_sha256,
            "started_at": _timestamp(self.started_at),
            "deadline": _timestamp(self.deadline),
            "heartbeat_phase": self.phase,
        }


@dataclass(frozen=True, slots=True, repr=False)
class AcquireResult:
    status: AcquireStatus
    handle: LeaseHandle | None = None

    def redacted_evidence(self) -> dict[str, Any]:
        if self.handle is not None:
            return self.handle.redacted_evidence()
        return {
            "status": self.status.value.lower().replace("_", "-"),
            "publication_authorized": False,
            "candidate_isolation_verified": False,
            "publisher_verified": False,
        }


@dataclass(frozen=True, slots=True, repr=False)
class CheckpointResult:
    status: CheckpointStatus
    handle: LeaseHandle | None = None

    def redacted_evidence(self) -> dict[str, Any]:
        if self.handle is not None:
            evidence = self.handle.redacted_evidence()
            evidence["status"] = "repository-lease-checkpointed"
            return evidence
        return {
            "status": self.status.value.lower().replace("_", "-"),
            "publication_authorized": False,
            "candidate_isolation_verified": False,
            "publisher_verified": False,
        }


@dataclass(frozen=True, slots=True, repr=False)
class FinalizeResult:
    status: FinalizeStatus
    outcome: str | None = None
    external_state: str | None = None
    terminal: TerminalEvidence | None = None

    def redacted_evidence(self) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "status": self.status.value.lower().replace("_", "-"),
            "publication_authorized": False,
            "candidate_isolation_verified": False,
            "publisher_verified": False,
        }
        if self.status is FinalizeStatus.FINALIZED:
            if self.terminal is None:
                raise LeaseProtocolError("LEASE_EVIDENCE_INVALID")
            evidence.update(self.terminal.redacted_evidence())
        return evidence


@dataclass(frozen=True, slots=True, repr=False)
class TerminalEvidence:
    manifest_id: str
    record_key: str
    manifest_sha256: str
    base_commit_sha: str
    policy_sha256: str
    run_id: str
    fence_digest_sha256: str
    deadline: datetime
    final_phase: int
    finalized_at: datetime
    outcome: str
    external_state: str

    def redacted_evidence(self) -> dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "record_key": self.record_key,
            "manifest_sha256": self.manifest_sha256,
            "base_commit_sha": self.base_commit_sha,
            "policy_sha256": self.policy_sha256,
            "run_id": self.run_id,
            "fence_digest_sha256": self.fence_digest_sha256,
            "deadline": _timestamp(self.deadline),
            "final_phase": self.final_phase,
            "finalized_at": _timestamp(self.finalized_at),
            "outcome": self.outcome,
            "external_state": self.external_state,
        }


@dataclass(slots=True)
class _Registry:
    common_fd: int
    root_fd: int
    common_device: int
    common_inode: int

    def close(self) -> None:
        os.close(self.root_fd)
        os.close(self.common_fd)


@dataclass(slots=True)
class _OwnedState:
    active_fd: int
    claims_fd: int
    claim_fd: int
    history_fd: int
    checked_at: datetime


def _raise_protocol(reason: str) -> NoReturn:
    raise LeaseProtocolError(reason)


def _canonical_json(value: dict[str, Any]) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (OverflowError, RecursionError, TypeError, ValueError, UnicodeError):
        raise _InvariantError from None


def _reject_constant(_: str) -> NoReturn:
    raise _InvariantError


def _object_from_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _InvariantError
        result[key] = value
    return result


def _strict_json(raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("ascii")
        value = json.loads(
            text,
            object_pairs_hook=_object_from_pairs,
            parse_constant=_reject_constant,
        )
    except (RecursionError, UnicodeError, ValueError, _InvariantError):
        raise _InvariantError from None
    if type(value) is not dict or _canonical_json(value) != raw:
        raise _InvariantError
    return value


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise _InvariantError
    normalized = value.astimezone(UTC)
    if normalized.microsecond:
        raise _InvariantError
    return normalized.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now(value: datetime | None) -> datetime:
    current = datetime.now(UTC) if value is None else value
    if type(current) is not datetime or current.tzinfo is None or current.utcoffset() is None:
        _raise_protocol("LEASE_INPUT_INVALID")
    return current.astimezone(UTC).replace(microsecond=0)


def _exact_keys(value: dict[str, Any], keys: frozenset[str]) -> None:
    if value.keys() != keys:
        raise _InvariantError


def _string(value: Any, pattern: re.Pattern[str] | None = None) -> str:
    if type(value) is not str or not value or (pattern is not None and pattern.fullmatch(value) is None):
        raise _InvariantError
    return value


def _integer(value: Any, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise _InvariantError
    return value


def _open_absolute_directory(path: Path) -> int:
    raw = os.fspath(path)
    if type(raw) is not str or not raw.startswith("/") or "\0" in raw:
        _raise_protocol("LEASE_COMMON_DIR_INVALID")
    if os.path.normpath(raw) != raw:
        _raise_protocol("LEASE_COMMON_DIR_INVALID")
    fd = os.open("/", _DIRECTORY_FLAGS)
    try:
        for component in raw.split("/")[1:]:
            if not component or component in {".", ".."}:
                _raise_protocol("LEASE_COMMON_DIR_INVALID")
            next_fd = os.open(component, _DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            _raise_protocol("LEASE_COMMON_DIR_INVALID")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _directory_ok(info: os.stat_result, device: int, *, exact_mode: bool = True) -> bool:
    return (
        stat.S_ISDIR(info.st_mode)
        and info.st_uid == os.getuid()
        and info.st_dev == device
        and (not exact_mode or stat.S_IMODE(info.st_mode) == 0o700)
    )


def _open_directory(parent_fd: int, name: str, device: int) -> int:
    try:
        fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError:
        raise _InvariantError from None
    if not _directory_ok(os.fstat(fd), device):
        os.close(fd)
        raise _InvariantError
    return fd


def _entry(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        raise _InvariantError from None


def _list(parent_fd: int) -> list[str]:
    try:
        names = os.listdir(parent_fd)
    except OSError:
        raise _InvariantError from None
    if len(names) > MAX_REGISTRY_ENTRIES or any(type(name) is not str for name in names):
        raise _InvariantError
    return names


def _ensure_directory(parent_fd: int, name: str, device: int) -> int:
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
    except FileExistsError:
        pass
    except OSError:
        raise _InvariantError from None
    return _open_directory(parent_fd, name, device)


def _open_registry(git_common_dir: Path, *, create: bool) -> _Registry:
    common_fd = _open_absolute_directory(git_common_dir)
    common_info = os.fstat(common_fd)
    root_fd = -1
    try:
        if _entry(common_fd, REGISTRY_NAME) is None:
            if not create:
                raise _InvariantError
            try:
                os.mkdir(REGISTRY_NAME, 0o700, dir_fd=common_fd)
                os.fsync(common_fd)
            except FileExistsError:
                pass
        root_fd = _open_directory(common_fd, REGISTRY_NAME, common_info.st_dev)
        allowed = {"active", "claims", "history", "terminal"}
        if not set(_list(root_fd)) <= allowed:
            raise _InvariantError
        return _Registry(
            common_fd=common_fd,
            root_fd=root_fd,
            common_device=common_info.st_dev,
            common_inode=common_info.st_ino,
        )
    except BaseException:
        if root_fd >= 0:
            os.close(root_fd)
        os.close(common_fd)
        raise


def _observe_existing_active(git_common_dir: Path) -> AcquireStatus | None:
    common_fd = root_fd = -1
    try:
        common_fd = _open_absolute_directory(git_common_dir)
        common_info = os.fstat(common_fd)
        if _entry(common_fd, REGISTRY_NAME) is None:
            return None
        root_fd = _open_directory(common_fd, REGISTRY_NAME, common_info.st_dev)
        names = set(_list(root_fd))
        if "active" in names:
            return AcquireStatus.BUSY_NOOP
        if not names <= {"claims", "history", "terminal"}:
            return AcquireStatus.RECOVERY_REQUIRED
        return None
    except LeaseProtocolError:
        raise
    except (OSError, _InvariantError):
        return AcquireStatus.RECOVERY_REQUIRED
    finally:
        if root_fd >= 0:
            os.close(root_fd)
        if common_fd >= 0:
            os.close(common_fd)


def _file_info(fd: int, device: int) -> os.stat_result:
    info = os.fstat(fd)
    if not (
        stat.S_ISREG(info.st_mode)
        and stat.S_IMODE(info.st_mode) == 0o600
        and info.st_uid == os.getuid()
        and info.st_nlink == 1
        and info.st_dev == device
        and 0 <= info.st_size <= MAX_RECORD_BYTES
    ):
        raise _InvariantError
    return info


def _read_json(parent_fd: int, name: str, device: int) -> dict[str, Any]:
    before = _entry(parent_fd, name)
    if before is None or not (
        stat.S_ISREG(before.st_mode)
        and stat.S_IMODE(before.st_mode) == 0o600
        and before.st_uid == os.getuid()
        and before.st_nlink == 1
        and before.st_dev == device
        and 0 <= before.st_size <= MAX_RECORD_BYTES
    ):
        raise _InvariantError
    try:
        fd = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
    except OSError:
        raise _InvariantError from None
    try:
        info = _file_info(fd, device)
        if (info.st_dev, info.st_ino) != (before.st_dev, before.st_ino):
            raise _InvariantError
        chunks: list[bytes] = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                raise _InvariantError
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(fd, 1) or os.fstat(fd).st_size != info.st_size:
            raise _InvariantError
        return _strict_json(b"".join(chunks))
    finally:
        os.close(fd)


def _write_json_exclusive(parent_fd: int, name: str, value: dict[str, Any], device: int) -> None:
    raw = _canonical_json(value)
    if not raw or len(raw) > MAX_RECORD_BYTES:
        raise _InvariantError
    try:
        fd = os.open(name, _CREATE_FLAGS, 0o600, dir_fd=parent_fd)
    except OSError:
        raise _InvariantError from None
    try:
        _file_info(fd, device)
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise _InvariantError
            view = view[written:]
        os.fsync(fd)
        _file_info(fd, device)
    except BaseException:
        raise
    finally:
        os.close(fd)
    os.fsync(parent_fd)


def _rename_noreplace(old_fd: int, old: str, new_fd: int, new: str) -> None:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except (AttributeError, OSError):
        raise _InvariantError from None
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        old_fd,
        old.encode("ascii"),
        new_fd,
        new.encode("ascii"),
        RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error in {errno.ENOSYS, errno.EINVAL, errno.ENOTSUP, errno.EEXIST}:
            raise _InvariantError
        raise _InvariantError
    os.fsync(old_fd)
    if new_fd != old_fd:
        os.fsync(new_fd)


_BINDING_KEYS = frozenset({
    "base_commit_sha",
    "deadline",
    "fence_digest_sha256",
    "manifest_id",
    "manifest_sha256",
    "policy_sha256",
    "process_id",
    "record_key",
    "run_id",
    "started_at",
})


def _binding(handle: LeaseHandle) -> dict[str, Any]:
    return {
        "base_commit_sha": handle.base_commit_sha,
        "deadline": _timestamp(handle.deadline),
        "fence_digest_sha256": handle.fence_digest_sha256,
        "manifest_id": handle.manifest_id,
        "manifest_sha256": handle.manifest_sha256,
        "policy_sha256": handle.policy_sha256,
        "process_id": handle.process_id,
        "record_key": handle.record_key,
        "run_id": handle.run_id,
        "started_at": _timestamp(handle.started_at),
    }


def _validate_binding(value: dict[str, Any], expected: dict[str, Any] | None = None) -> None:
    for key in _BINDING_KEYS:
        item = value.get(key)
        if key == "manifest_id":
            manifest_id = _string(item)
            try:
                encoded = manifest_id.encode("ascii")
            except UnicodeError:
                raise _InvariantError from None
            if hashlib.sha256(encoded).hexdigest() != value.get("record_key"):
                raise _InvariantError
        elif key == "record_key" or key.endswith("sha256"):
            _string(item, HEX_SHA256)
        elif key == "base_commit_sha":
            _string(item, COMMIT_SHA)
        elif key == "run_id":
            _string(item, RUN_ID)
        elif key in {"deadline", "started_at"}:
            timestamp = _string(item)
            try:
                parsed = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            except ValueError:
                raise _InvariantError from None
            if _timestamp(parsed) != timestamp:
                raise _InvariantError
        elif key == "process_id":
            _integer(item, 1)
    if expected is not None and any(value.get(key) != expected[key] for key in _BINDING_KEYS):
        raise _InvariantError


def _owner_record(handle: LeaseHandle) -> dict[str, Any]:
    return {
        **_binding(handle),
        "record_type": "owner",
        "schema_version": SCHEMA_VERSION,
    }


def _claim_record(handle: LeaseHandle) -> dict[str, Any]:
    return {**_binding(handle), "record_type": "claim", "schema_version": SCHEMA_VERSION}


def _phase_record(handle: LeaseHandle, phase: int, checked_at: datetime) -> dict[str, Any]:
    return {
        **_binding(handle),
        "checked_at": _timestamp(checked_at),
        "phase": phase,
        "record_type": "heartbeat",
        "schema_version": SCHEMA_VERSION,
    }


def _validate_owner(value: dict[str, Any], expected: dict[str, Any]) -> None:
    _exact_keys(
        value,
        _BINDING_KEYS | {"record_type", "schema_version"},
    )
    _validate_binding(value, expected)
    if (
        type(value["record_type"]) is not str
        or value["record_type"] != "owner"
        or type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
    ):
        raise _InvariantError


def _validate_claim(value: dict[str, Any], expected: dict[str, Any] | None = None) -> None:
    _exact_keys(value, _BINDING_KEYS | {"record_type", "schema_version"})
    _validate_binding(value, expected)
    if (
        type(value["record_type"]) is not str
        or value["record_type"] != "claim"
        or type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
    ):
        raise _InvariantError


def _phase_name(phase: int) -> str:
    if type(phase) is not int or not 0 <= phase < 10**20:
        raise _InvariantError
    return f"{phase:020d}.json"


def _validate_phases(
    phases_fd: int,
    device: int,
    expected: dict[str, Any],
) -> tuple[int, tuple[bytes, ...], datetime]:
    names = sorted(_list(phases_fd))
    if not names:
        raise _InvariantError
    raw_records: list[bytes] = []
    previous = datetime.strptime(expected["started_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=UTC
    )
    deadline = datetime.strptime(expected["deadline"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=UTC
    )
    for expected_phase, name in enumerate(names):
        match = PHASE_FILE.fullmatch(name)
        if match is None or int(match.group(1)) != expected_phase:
            raise _InvariantError
        record = _read_json(phases_fd, name, device)
        _exact_keys(
            record,
            _BINDING_KEYS
            | {"checked_at", "phase", "record_type", "schema_version"},
        )
        _validate_binding(record, expected)
        if (
            type(record["record_type"]) is not str
            or record["record_type"] != "heartbeat"
            or type(record["schema_version"]) is not int
            or record["schema_version"] != SCHEMA_VERSION
            or _integer(record["phase"]) != expected_phase
        ):
            raise _InvariantError
        _string(record["checked_at"])
        try:
            parsed = datetime.strptime(record["checked_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC
            )
        except ValueError:
            raise _InvariantError from None
        if _timestamp(parsed) != record["checked_at"]:
            raise _InvariantError
        if parsed < previous or parsed >= deadline or (expected_phase == 0 and parsed != previous):
            raise _InvariantError
        previous = parsed
        raw_records.append(_canonical_json(record))
    return len(names) - 1, tuple(raw_records), previous


def _scan_collection(fd: int, device: int, pattern: re.Pattern[str]) -> None:
    for name in _list(fd):
        if pattern.fullmatch(name) is None:
            raise _InvariantError
        info = _entry(fd, name)
        if info is None or not _directory_ok(info, device):
            raise _InvariantError


def _verify_handle_root(handle: LeaseHandle, registry: _Registry) -> None:
    if (
        not isinstance(handle.git_common_dir, Path)
        or type(handle.common_device) is not int
        or type(handle.common_inode) is not int
        or type(handle.process_id) is not int
        or type(handle.fence_nonce) is not bytes
        or len(handle.fence_nonce) != 32
        or type(handle.phase) is not int
        or handle.phase < 0
        or registry.common_device != handle.common_device
        or registry.common_inode != handle.common_inode
        or hashlib.sha256(handle.fence_nonce).hexdigest() != handle.fence_digest_sha256
        or os.getpid() != handle.process_id
    ):
        raise _InvariantError


def _verify_owned(
    registry: _Registry, handle: LeaseHandle
) -> _OwnedState:
    _verify_handle_root(handle, registry)
    if set(_list(registry.root_fd)) != {"active", "claims", "history", "terminal"}:
        raise _InvariantError
    active_fd = claims_fd = terminal_fd = history_fd = claim_fd = -1
    try:
        active_fd = _open_directory(registry.root_fd, "active", registry.common_device)
        claims_fd = _open_directory(registry.root_fd, "claims", registry.common_device)
        terminal_fd = _open_directory(registry.root_fd, "terminal", registry.common_device)
        history_fd = _open_directory(registry.root_fd, "history", registry.common_device)
        _scan_collection(claims_fd, registry.common_device, HEX_SHA256)
        _scan_collection(terminal_fd, registry.common_device, HEX_SHA256)
        _scan_collection(history_fd, registry.common_device, RUN_ID)
        if _entry(terminal_fd, handle.record_key) is not None:
            raise _InvariantError
        claim_fd = _open_directory(claims_fd, handle.record_key, registry.common_device)
        expected = _binding(handle)
        if set(_list(active_fd)) != {"heartbeats", "owner.json"}:
            raise _InvariantError
        if set(_list(claim_fd)) != {"claim.json", "heartbeats"}:
            raise _InvariantError
        _validate_owner(_read_json(active_fd, "owner.json", registry.common_device), expected)
        _validate_claim(_read_json(claim_fd, "claim.json", registry.common_device), expected)
        active_phases = _open_directory(active_fd, "heartbeats", registry.common_device)
        claim_phases = _open_directory(claim_fd, "heartbeats", registry.common_device)
        try:
            active_phase, active_raw, active_checked_at = _validate_phases(
                active_phases, registry.common_device, expected
            )
            claim_phase, claim_raw, claim_checked_at = _validate_phases(
                claim_phases, registry.common_device, expected
            )
        finally:
            os.close(active_phases)
            os.close(claim_phases)
        if (
            active_phase != claim_phase
            or active_raw != claim_raw
            or active_checked_at != claim_checked_at
            or active_phase != handle.phase
        ):
            raise _InvariantError
        owned = _OwnedState(
            active_fd=active_fd,
            claims_fd=claims_fd,
            claim_fd=claim_fd,
            history_fd=history_fd,
            checked_at=active_checked_at,
        )
        os.close(terminal_fd)
        terminal_fd = -1
        return owned
    except BaseException:
        for fd in (claim_fd, active_fd, claims_fd, terminal_fd, history_fd):
            if fd >= 0:
                os.close(fd)
        raise


def _close_owned(owned: _OwnedState) -> None:
    for fd in (owned.active_fd, owned.claims_fd, owned.claim_fd, owned.history_fd):
        os.close(fd)


def _validated_deadline(
    manifest: ProtectedBaseManifest, policy_sha256: str, now: datetime
) -> datetime:
    if type(manifest) is not ProtectedBaseManifest or type(policy_sha256) is not str:
        _raise_protocol("LEASE_INPUT_INVALID")
    manifest_id = manifest.contract.manifest_id
    if type(manifest_id) is not str:
        _raise_protocol("LEASE_INPUT_INVALID")
    try:
        encoded_id = manifest_id.encode("ascii")
    except UnicodeError:
        _raise_protocol("LEASE_INPUT_INVALID")
    record_key = hashlib.sha256(encoded_id).hexdigest()
    if (
        MANIFEST_ID.fullmatch(manifest_id) is None
        or HEX_SHA256.fullmatch(policy_sha256) is None
        or type(manifest.manifest_sha256) is not str
        or HEX_SHA256.fullmatch(manifest.manifest_sha256) is None
        or type(manifest.base_commit_sha) is not str
        or not COMMIT_SHA.fullmatch(manifest.base_commit_sha)
        or manifest.record_key != record_key
        or type(manifest.object_format) is not str
        or manifest.object_format not in {"sha1", "sha256"}
        or len(manifest.base_commit_sha) != (40 if manifest.object_format == "sha1" else 64)
        or type(manifest.contract.maximum_runtime_minutes) is not int
        or not 1 <= manifest.contract.maximum_runtime_minutes <= 120
    ):
        _raise_protocol("LEASE_INPUT_INVALID")
    issued_source = manifest.contract.authorization_issued_at
    expires_source = manifest.contract.authorization_expires
    verified_source = manifest.verified_at
    if any(
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.microsecond != 0
        for value in (issued_source, expires_source, verified_source)
    ):
        _raise_protocol("LEASE_INPUT_INVALID")
    issued_at = issued_source.astimezone(UTC).replace(microsecond=0)
    expires_at = expires_source.astimezone(UTC).replace(microsecond=0)
    verified_at = verified_source.astimezone(UTC).replace(microsecond=0)
    if not issued_at <= verified_at < expires_at or now < verified_at or now >= expires_at:
        _raise_protocol("LEASE_AUTHORIZATION_INACTIVE")
    deadline = min(
        now + timedelta(minutes=manifest.contract.maximum_runtime_minutes),
        expires_at,
    )
    if now >= deadline:
        _raise_protocol("LEASE_AUTHORIZATION_INACTIVE")
    return deadline


def _new_handle(
    git_common_dir: Path,
    registry: _Registry,
    manifest: ProtectedBaseManifest,
    policy_sha256: str,
    now: datetime,
    deadline: datetime,
) -> LeaseHandle:
    manifest_id = manifest.contract.manifest_id
    record_key = hashlib.sha256(manifest_id.encode("ascii")).hexdigest()
    try:
        run_id = str(uuid.uuid4())
        fence_nonce = secrets.token_bytes(32)
    except (OSError, RuntimeError):
        _raise_protocol("LEASE_RANDOMNESS_UNAVAILABLE")
    if (
        type(run_id) is not str
        or RUN_ID.fullmatch(run_id) is None
        or type(fence_nonce) is not bytes
        or len(fence_nonce) != 32
    ):
        _raise_protocol("LEASE_RANDOMNESS_UNAVAILABLE")
    return LeaseHandle(
        git_common_dir=git_common_dir,
        common_device=registry.common_device,
        common_inode=registry.common_inode,
        manifest_id=manifest_id,
        record_key=record_key,
        manifest_sha256=manifest.manifest_sha256,
        base_commit_sha=manifest.base_commit_sha,
        policy_sha256=policy_sha256,
        run_id=run_id,
        process_id=os.getpid(),
        fence_nonce=fence_nonce,
        fence_digest_sha256=hashlib.sha256(fence_nonce).hexdigest(),
        started_at=now,
        deadline=deadline,
        phase=0,
    )


def _create_active(registry: _Registry, handle: LeaseHandle) -> int:
    try:
        os.mkdir("active", 0o700, dir_fd=registry.root_fd)
        os.fsync(registry.root_fd)
    except FileExistsError:
        raise
    except OSError:
        raise _InvariantError from None
    active_fd = _open_directory(registry.root_fd, "active", registry.common_device)
    try:
        phases_fd = _ensure_directory(active_fd, "heartbeats", registry.common_device)
        try:
            _write_json_exclusive(
                phases_fd,
                _phase_name(0),
                _phase_record(handle, 0, handle.started_at),
                registry.common_device,
            )
        finally:
            os.close(phases_fd)
        _write_json_exclusive(
            active_fd, "owner.json", _owner_record(handle), registry.common_device
        )
        os.fsync(active_fd)
        return active_fd
    except BaseException:
        os.close(active_fd)
        raise


def _create_claim(claims_fd: int, registry: _Registry, handle: LeaseHandle) -> int:
    try:
        os.mkdir(handle.record_key, 0o700, dir_fd=claims_fd)
        os.fsync(claims_fd)
    except FileExistsError:
        raise
    except OSError:
        raise _InvariantError from None
    claim_fd = _open_directory(claims_fd, handle.record_key, registry.common_device)
    try:
        phases_fd = _ensure_directory(claim_fd, "heartbeats", registry.common_device)
        try:
            _write_json_exclusive(
                phases_fd,
                _phase_name(0),
                _phase_record(handle, 0, handle.started_at),
                registry.common_device,
            )
        finally:
            os.close(phases_fd)
        _write_json_exclusive(
            claim_fd, "claim.json", _claim_record(handle), registry.common_device
        )
        os.fsync(claim_fd)
        return claim_fd
    except BaseException:
        os.close(claim_fd)
        raise


def _record_matches(record_fd: int, device: int, handle: LeaseHandle, terminal: bool) -> bool:
    names = set(_list(record_fd))
    required = {"claim.json", "heartbeats", "result.json"} if terminal else {
        "claim.json",
        "heartbeats",
    }
    if names != required:
        raise _InvariantError
    claim = _read_json(record_fd, "claim.json", device)
    _validate_claim(claim)
    if claim["record_key"] != handle.record_key:
        raise _InvariantError
    expected = {key: claim[key] for key in _BINDING_KEYS}
    phases_fd = _open_directory(record_fd, "heartbeats", device)
    try:
        phase, _raw, checked_at = _validate_phases(phases_fd, device, expected)
    finally:
        os.close(phases_fd)
    if terminal:
        result = _read_json(record_fd, "result.json", device)
        _validate_terminal_record(result, "result")
        if any(result[key] != claim[key] for key in _BINDING_KEYS):
            raise _InvariantError
        finalized_at = datetime.strptime(result["finalized_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
        if result["final_phase"] != phase or finalized_at < checked_at:
            raise _InvariantError
    return claim["manifest_id"] == handle.manifest_id and claim["manifest_sha256"] == handle.manifest_sha256


def _classify_manifest_record(
    claims_fd: int,
    terminal_fd: int,
    registry: _Registry,
    handle: LeaseHandle,
) -> tuple[AcquireStatus, str] | None:
    claim_exists = _entry(claims_fd, handle.record_key) is not None
    terminal_exists = _entry(terminal_fd, handle.record_key) is not None
    if claim_exists and terminal_exists:
        raise _InvariantError
    if not claim_exists and not terminal_exists:
        return None
    parent_fd = terminal_fd if terminal_exists else claims_fd
    record_fd = _open_directory(parent_fd, handle.record_key, registry.common_device)
    try:
        matches = _record_matches(record_fd, registry.common_device, handle, terminal_exists)
    finally:
        os.close(record_fd)
    if matches:
        return AcquireStatus.DUPLICATE_NOOP, "duplicate_noop"
    return AcquireStatus.DENIED_ID_REUSE, "denied_id_reuse"


def _release_without_claim(
    registry: _Registry,
    active_fd: int,
    history_fd: int,
    handle: LeaseHandle,
    outcome: str,
) -> None:
    release = {
        **_binding(handle),
        "external_state": "none",
        "final_phase": 0,
        "finalized_at": _timestamp(handle.started_at),
        "outcome": outcome,
        "record_type": "release",
        "schema_version": SCHEMA_VERSION,
    }
    _write_json_exclusive(active_fd, "release.json", release, registry.common_device)
    os.fsync(active_fd)
    _rename_noreplace(registry.root_fd, "active", history_fd, handle.run_id)


def acquire_repository_lease(
    git_common_dir: Path,
    manifest: ProtectedBaseManifest,
    *,
    policy_sha256: str,
    now: datetime | None = None,
) -> AcquireResult:
    """Acquire one local repository lease; it never accesses candidate content."""

    if not isinstance(git_common_dir, Path):
        _raise_protocol("LEASE_INPUT_INVALID")
    observation = _observe_existing_active(git_common_dir)
    if observation is not None:
        return AcquireResult(observation)
    current = _now(now)
    deadline = _validated_deadline(manifest, policy_sha256, current)
    try:
        registry = _open_registry(git_common_dir, create=True)
    except LeaseProtocolError:
        raise
    except (OSError, _InvariantError):
        return AcquireResult(AcquireStatus.RECOVERY_REQUIRED)
    try:
        if _entry(registry.root_fd, "active") is not None:
            return AcquireResult(AcquireStatus.BUSY_NOOP)
        for name in ("claims", "history", "terminal"):
            fd = _ensure_directory(registry.root_fd, name, registry.common_device)
            os.close(fd)
        layout = set(_list(registry.root_fd))
        if "active" in layout:
            return AcquireResult(AcquireStatus.BUSY_NOOP)
        if layout != {"claims", "history", "terminal"}:
            return AcquireResult(AcquireStatus.RECOVERY_REQUIRED)
        handle = _new_handle(
            git_common_dir, registry, manifest, policy_sha256, current, deadline
        )
        try:
            active_fd = _create_active(registry, handle)
        except FileExistsError:
            return AcquireResult(AcquireStatus.BUSY_NOOP)
        except (OSError, _InvariantError):
            return AcquireResult(AcquireStatus.RECOVERY_REQUIRED)
        claims_fd = terminal_fd = history_fd = -1
        try:
            claims_fd = _open_directory(registry.root_fd, "claims", registry.common_device)
            terminal_fd = _open_directory(registry.root_fd, "terminal", registry.common_device)
            history_fd = _open_directory(registry.root_fd, "history", registry.common_device)
            _scan_collection(claims_fd, registry.common_device, HEX_SHA256)
            _scan_collection(terminal_fd, registry.common_device, HEX_SHA256)
            _scan_collection(history_fd, registry.common_device, RUN_ID)
            classification = _classify_manifest_record(
                claims_fd, terminal_fd, registry, handle
            )
            if classification is not None:
                status, outcome = classification
                _release_without_claim(registry, active_fd, history_fd, handle, outcome)
                return AcquireResult(status)
            try:
                claim_fd = _create_claim(claims_fd, registry, handle)
            except FileExistsError:
                classification = _classify_manifest_record(
                    claims_fd, terminal_fd, registry, handle
                )
                if classification is None:
                    raise _InvariantError
                status, outcome = classification
                _release_without_claim(registry, active_fd, history_fd, handle, outcome)
                return AcquireResult(status)
            os.close(claim_fd)
            verified = _verify_owned(registry, handle)
            _close_owned(verified)
            return AcquireResult(AcquireStatus.OWNED, handle)
        except (OSError, _InvariantError):
            return AcquireResult(AcquireStatus.RECOVERY_REQUIRED)
        finally:
            os.close(active_fd)
            for fd in (claims_fd, terminal_fd, history_fd):
                if fd >= 0:
                    os.close(fd)
    except LeaseProtocolError:
        raise
    except (OSError, _InvariantError):
        return AcquireResult(AcquireStatus.RECOVERY_REQUIRED)
    finally:
        registry.close()


def checkpoint_repository_lease(
    handle: LeaseHandle, *, now: datetime | None = None
) -> CheckpointResult:
    if type(handle) is not LeaseHandle:
        _raise_protocol("LEASE_INPUT_INVALID")
    current = _now(now)
    try:
        registry = _open_registry(handle.git_common_dir, create=False)
        try:
            owned = _verify_owned(registry, handle)
            active_fd = owned.active_fd
            claim_fd = owned.claim_fd
            try:
                if current < owned.checked_at:
                    raise _InvariantError
                if current >= handle.deadline:
                    return CheckpointResult(CheckpointStatus.DEADLINE_EXPIRED)
                expected = _binding(handle)
                next_phase = handle.phase + 1
                if next_phase >= MAX_REGISTRY_ENTRIES:
                    raise _InvariantError
                active_phases = _open_directory(
                    active_fd, "heartbeats", registry.common_device
                )
                claim_phases = _open_directory(claim_fd, "heartbeats", registry.common_device)
                try:
                    record = _phase_record(handle, next_phase, current)
                    _write_json_exclusive(
                        claim_phases,
                        _phase_name(next_phase),
                        record,
                        registry.common_device,
                    )
                    _write_json_exclusive(
                        active_phases,
                        _phase_name(next_phase),
                        record,
                        registry.common_device,
                    )
                    active_phase, active_raw, active_checked_at = _validate_phases(
                        active_phases, registry.common_device, expected
                    )
                    claim_phase, claim_raw, claim_checked_at = _validate_phases(
                        claim_phases, registry.common_device, expected
                    )
                    if (
                        active_phase != next_phase
                        or claim_phase != next_phase
                        or active_raw != claim_raw
                        or active_checked_at != current
                        or claim_checked_at != current
                    ):
                        raise _InvariantError
                finally:
                    os.close(active_phases)
                    os.close(claim_phases)
                next_handle = replace(handle, phase=next_phase)
                return CheckpointResult(CheckpointStatus.CHECKPOINTED, next_handle)
            finally:
                _close_owned(owned)
        finally:
            registry.close()
    except (OSError, _InvariantError, LeaseProtocolError):
        return CheckpointResult(CheckpointStatus.RECOVERY_REQUIRED)


_TERMINAL_KEYS = _BINDING_KEYS | {
    "external_state",
    "final_phase",
    "finalized_at",
    "outcome",
    "record_type",
    "schema_version",
}


def _validate_terminal_record(value: dict[str, Any], record_type: str) -> None:
    _exact_keys(value, _TERMINAL_KEYS)
    _validate_binding(value)
    actual_type = _string(value["record_type"])
    outcome = _string(value["outcome"])
    external_state = _string(value["external_state"])
    if (
        actual_type != record_type
        or type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
        or outcome not in TERMINAL_OUTCOMES
        or external_state not in EXTERNAL_STATES
    ):
        raise _InvariantError
    _integer(value["final_phase"])
    _string(value["finalized_at"])
    try:
        parsed = datetime.strptime(value["finalized_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        raise _InvariantError from None
    if _timestamp(parsed) != value["finalized_at"]:
        raise _InvariantError


def finalize_repository_lease(
    handle: LeaseHandle,
    *,
    outcome: str,
    external_state: str,
    now: datetime | None = None,
) -> FinalizeResult:
    if type(handle) is not LeaseHandle:
        _raise_protocol("LEASE_INPUT_INVALID")
    if (
        type(outcome) is not str
        or type(external_state) is not str
        or outcome not in TERMINAL_OUTCOMES
        or external_state not in EXTERNAL_STATES
    ):
        _raise_protocol("LEASE_INPUT_INVALID")
    current = _now(now)
    try:
        registry = _open_registry(handle.git_common_dir, create=False)
        try:
            owned = _verify_owned(registry, handle)
            active_fd = owned.active_fd
            claims_fd = owned.claims_fd
            claim_fd = owned.claim_fd
            history_fd = owned.history_fd
            terminal_fd = -1
            try:
                if current < owned.checked_at:
                    raise _InvariantError
                terminal_fd = _open_directory(
                    registry.root_fd, "terminal", registry.common_device
                )
                record = {
                    **_binding(handle),
                    "external_state": external_state,
                    "final_phase": handle.phase,
                    "finalized_at": _timestamp(current),
                    "outcome": outcome,
                    "record_type": "result",
                    "schema_version": SCHEMA_VERSION,
                }
                _write_json_exclusive(
                    claim_fd, "result.json", record, registry.common_device
                )
                os.fsync(claim_fd)
                _rename_noreplace(
                    claims_fd, handle.record_key, terminal_fd, handle.record_key
                )
                release = {**record, "record_type": "release"}
                _write_json_exclusive(
                    active_fd, "release.json", release, registry.common_device
                )
                os.fsync(active_fd)
                _rename_noreplace(registry.root_fd, "active", history_fd, handle.run_id)
                terminal = TerminalEvidence(
                    manifest_id=handle.manifest_id,
                    record_key=handle.record_key,
                    manifest_sha256=handle.manifest_sha256,
                    base_commit_sha=handle.base_commit_sha,
                    policy_sha256=handle.policy_sha256,
                    run_id=handle.run_id,
                    fence_digest_sha256=handle.fence_digest_sha256,
                    deadline=handle.deadline,
                    final_phase=handle.phase,
                    finalized_at=current,
                    outcome=outcome,
                    external_state=external_state,
                )
                return FinalizeResult(
                    FinalizeStatus.FINALIZED, outcome, external_state, terminal
                )
            finally:
                if terminal_fd >= 0:
                    os.close(terminal_fd)
                _close_owned(owned)
        finally:
            registry.close()
    except (OSError, _InvariantError, LeaseProtocolError):
        return FinalizeResult(FinalizeStatus.RECOVERY_REQUIRED)
