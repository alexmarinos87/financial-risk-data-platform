from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.overnight_manifest_verifier import (
    MAX_CANDIDATE_BLOB_BYTES,
    MAX_CANDIDATE_TOTAL_BLOB_BYTES,
    CommittedCandidateHistory,
    CommittedHistoryEdge,
    CommittedPathChange,
    ManifestDenied,
    VerifiedFileContent,
    VerifierFailure,
    verify_committed_candidate_history,
)


LINE_COUNT_ALGORITHM = "frdp-lf-minimal-insdel-v1"
FINGERPRINT_SCHEMA = "frdp.overnight.candidate-content.v1"
FINGERPRINT_ALGORITHM = "sha256"
FINGERPRINT_DOMAIN = FINGERPRINT_SCHEMA.encode("ascii") + b"\0"
REPOSITORY_ID = b"alexmarinos87/financial-risk-data-platform"
MAX_LINE_RECORD_BYTES = 64 * 1024
MAX_LINE_RECORDS_PER_BLOB = 100_000
MAX_TOTAL_LINE_RECORDS = 200_000
MAX_EDIT_WORK_UNITS = 5_000_000
MAX_EDIT_COMPARISON_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True, repr=False)
class CandidateEdgeLineCount:
    parent_commit_sha: str
    parent_tree_sha: str
    commit_sha: str
    commit_tree_sha: str
    additions: int
    deletions: int

    @property
    def changed_lines(self) -> int:
        return self.additions + self.deletions

    def redacted_evidence(self) -> dict[str, Any]:
        return {
            "parent_commit_sha": self.parent_commit_sha,
            "parent_tree_sha": self.parent_tree_sha,
            "commit_sha": self.commit_sha,
            "commit_tree_sha": self.commit_tree_sha,
            "additions": self.additions,
            "deletions": self.deletions,
            "changed_lines": self.changed_lines,
        }


@dataclass(frozen=True, slots=True, repr=False)
class CommittedCandidateContent:
    history: CommittedCandidateHistory
    edge_line_counts: tuple[CandidateEdgeLineCount, ...]
    cumulative_additions: int
    cumulative_deletions: int
    candidate_fingerprint_sha256: str
    fingerprinted_blob_count: int
    fingerprinted_blob_bytes: int

    @property
    def cumulative_changed_lines(self) -> int:
        return self.cumulative_additions + self.cumulative_deletions

    def redacted_evidence(self) -> dict[str, Any]:
        evidence = self.history.redacted_evidence()
        evidence.update({
            "status": "candidate-content-observed",
            "publication_authorized": False,
            "object_store_isolation_verified": False,
            "worktree_cleanliness_verified": False,
            "validation_verified": False,
            "push_budget_verified": False,
            "changed_line_budget_verified": True,
            "content_fingerprint_verified": True,
            "line_count_algorithm": LINE_COUNT_ALGORITHM,
            "cumulative_additions": self.cumulative_additions,
            "cumulative_deletions": self.cumulative_deletions,
            "cumulative_changed_lines": self.cumulative_changed_lines,
            "edge_line_counts": [item.redacted_evidence() for item in self.edge_line_counts],
            "candidate_fingerprint_schema": FINGERPRINT_SCHEMA,
            "candidate_fingerprint_algorithm": FINGERPRINT_ALGORITHM,
            "candidate_fingerprint": f"sha256:{self.candidate_fingerprint_sha256}",
            "fingerprinted_blob_count": self.fingerprinted_blob_count,
            "fingerprinted_blob_bytes": self.fingerprinted_blob_bytes,
        })
        return evidence


@dataclass(slots=True)
class _EditWork:
    units: int = 0
    comparison_bytes: int = 0

    def step(self) -> None:
        self.units += 1
        if self.units > MAX_EDIT_WORK_UNITS:
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")

    def equal(self, left: bytes, right: bytes) -> bool:
        self.step()
        self.comparison_bytes += len(left) + len(right)
        if self.comparison_bytes > MAX_EDIT_COMPARISON_BYTES:
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        return left == right


@dataclass(frozen=True, slots=True, repr=False)
class _CountedChange:
    change: CommittedPathChange
    additions: int
    deletions: int


@dataclass(frozen=True, slots=True, repr=False)
class _CountedEdge:
    edge: CommittedHistoryEdge
    changes: tuple[_CountedChange, ...]
    line_count: CandidateEdgeLineCount


class _Transcript:
    def __init__(self) -> None:
        self._digest = hashlib.sha256(FINGERPRINT_DOMAIN)

    def field(self, tag: str, value: bytes) -> None:
        try:
            encoded_tag = tag.encode("ascii")
            length = len(value).to_bytes(8, "big")
        except (OverflowError, UnicodeError):
            raise VerifierFailure from None
        if not encoded_tag or len(encoded_tag) > 255:
            raise VerifierFailure
        self._digest.update(bytes((len(encoded_tag),)))
        self._digest.update(encoded_tag)
        self._digest.update(length)
        self._digest.update(value)

    def integer(self, tag: str, value: int) -> None:
        if type(value) is not int or not 0 <= value < 2**64:
            raise VerifierFailure
        self.field(tag, value.to_bytes(8, "big"))

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _line_records(content: bytes) -> tuple[bytes, ...]:
    records: list[bytes] = []
    start = 0
    while start < len(content):
        newline = content.find(b"\n", start)
        end = len(content) if newline < 0 else newline + 1
        record = content[start:end]
        if len(record) > MAX_LINE_RECORD_BYTES:
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        records.append(record)
        if len(records) > MAX_LINE_RECORDS_PER_BLOB:
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        start = end
    return tuple(records)


def _bounded_edit_distance(
    old: tuple[bytes, ...], new: tuple[bytes, ...], limit: int, work: _EditWork
) -> int:
    prefix = 0
    while prefix < len(old) and prefix < len(new) and work.equal(old[prefix], new[prefix]):
        prefix += 1
    old_end = len(old)
    new_end = len(new)
    while (
        old_end > prefix
        and new_end > prefix
        and work.equal(old[old_end - 1], new[new_end - 1])
    ):
        old_end -= 1
        new_end -= 1
    old_middle = old[prefix:old_end]
    new_middle = new[prefix:new_end]
    old_count = len(old_middle)
    new_count = len(new_middle)
    if abs(old_count - new_count) > limit:
        raise ManifestDenied("CANDIDATE_BUDGET_EXCEEDED")
    if not old_middle or not new_middle:
        distance = old_count + new_count
        if distance > limit:
            raise ManifestDenied("CANDIDATE_BUDGET_EXCEEDED")
        return distance

    frontier = {1: 0}
    for distance in range(limit + 1):
        for diagonal in range(-distance, distance + 1, 2):
            work.step()
            if diagonal == -distance or (
                diagonal != distance
                and frontier.get(diagonal - 1, -1) < frontier.get(diagonal + 1, -1)
            ):
                x = frontier.get(diagonal + 1, 0)
            else:
                x = frontier.get(diagonal - 1, 0) + 1
            y = x - diagonal
            while (
                x < old_count
                and y < new_count
                and work.equal(old_middle[x], new_middle[y])
            ):
                x += 1
                y += 1
            frontier[diagonal] = x
            if x >= old_count and y >= new_count:
                return distance
    raise ManifestDenied("CANDIDATE_BUDGET_EXCEEDED")


def _collect_blobs(history: CommittedCandidateHistory) -> tuple[VerifiedFileContent, ...]:
    blobs: dict[str, VerifiedFileContent] = {}
    changes = [change for edge in history.edges for change in edge.changes]
    changes.extend(history.final_changes)
    for change in changes:
        for endpoint in (change.old, change.new):
            if endpoint is None:
                continue
            existing = blobs.get(endpoint.git_oid)
            if existing is not None and existing != endpoint:
                raise VerifierFailure
            try:
                git_hasher = hashlib.new(
                    history.manifest.object_format, usedforsecurity=False
                )
            except (TypeError, ValueError):
                raise VerifierFailure from None
            git_hasher.update(
                b"blob " + str(len(endpoint.content)).encode("ascii") + b"\0"
            )
            git_hasher.update(endpoint.content)
            if (
                len(endpoint.content) > MAX_CANDIDATE_BLOB_BYTES
                or git_hasher.hexdigest() != endpoint.git_oid
                or hashlib.sha256(endpoint.content).hexdigest() != endpoint.sha256
            ):
                raise VerifierFailure
            blobs[endpoint.git_oid] = endpoint
    if not blobs:
        raise VerifierFailure
    if sum(len(blob.content) for blob in blobs.values()) > MAX_CANDIDATE_TOTAL_BLOB_BYTES:
        raise VerifierFailure
    return tuple(blobs[oid] for oid in sorted(blobs))


def _is_hex(value: str, length: int) -> bool:
    return (
        type(value) is str
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _valid_endpoint(value: VerifiedFileContent | None, oid_length: int) -> bool:
    return value is None or (
        value.mode == "100644"
        and _is_hex(value.git_oid, oid_length)
        and _is_hex(value.sha256, 64)
    )


def _validate_history_observation(
    history: CommittedCandidateHistory,
    base_sha: str,
    manifest_id: str,
    candidate_sha: str,
) -> None:
    oid_length = {"sha1": 40, "sha256": 64}.get(history.manifest.object_format)
    if oid_length is None or not (
        len(history.commit_shas)
        == len(history.commit_object_sha256s)
        == len(history.edges)
        > 0
    ):
        raise VerifierFailure
    contract = history.manifest.contract
    if (
        history.manifest.base_commit_sha != base_sha
        or contract.manifest_id != manifest_id
        or history.candidate_commit_sha != candidate_sha
        or len(history.commit_shas) > contract.maximum_commits
        or len(set(history.commit_shas)) != len(history.commit_shas)
        or history.manifest.base_commit_sha in history.commit_shas
        or not _is_hex(history.base_commit_sha256, 64)
    ):
        raise VerifierFailure
    previous_commit = history.manifest.base_commit_sha
    previous_tree = history.manifest.base_tree_sha
    touched_paths: set[str] = set()
    base_state: dict[str, VerifiedFileContent | None] = {}
    current_state: dict[str, VerifiedFileContent | None] = {}
    for commit, digest, edge in zip(
        history.commit_shas, history.commit_object_sha256s, history.edges
    ):
        paths = tuple(change.path for change in edge.changes)
        if (
            not _is_hex(commit, oid_length)
            or not _is_hex(digest, 64)
            or edge.parent_commit_sha != previous_commit
            or edge.parent_tree_sha != previous_tree
            or edge.commit_sha != commit
            or edge.commit_sha == edge.parent_commit_sha
            or not _is_hex(edge.commit_tree_sha, oid_length)
            or edge.commit_tree_sha == edge.parent_tree_sha
            or not paths
            or paths != tuple(sorted(set(paths)))
        ):
            raise VerifierFailure
        for change in edge.changes:
            if change.path not in history.manifest.contract.allowed_paths:
                raise VerifierFailure
            if change.old is None and change.new is None:
                raise VerifierFailure
            if change.old == change.new:
                raise VerifierFailure
            if not all(_valid_endpoint(endpoint, oid_length) for endpoint in (change.old, change.new)):
                raise VerifierFailure
            if change.path in current_state:
                if current_state[change.path] != change.old:
                    raise VerifierFailure
            else:
                base_state[change.path] = change.old
            current_state[change.path] = change.new
        touched_paths.update(paths)
        previous_commit = edge.commit_sha
        previous_tree = edge.commit_tree_sha
    expected_final = tuple(
        CommittedPathChange(path, base_state[path], current_state[path])
        for path in sorted(current_state)
        if base_state[path] != current_state[path]
    )
    final_paths = tuple(change.path for change in history.final_changes)
    if (
        previous_commit != history.candidate_commit_sha
        or previous_tree != history.candidate_tree_sha
        or tuple(sorted(touched_paths)) != history.touched_paths
        or len(touched_paths) > contract.maximum_changed_files
        or not final_paths
        or final_paths != tuple(sorted(set(final_paths)))
        or final_paths != history.final_paths
        or history.final_changes != expected_final
    ):
        raise VerifierFailure
    tree_digests = history.tree_object_sha256s
    tree_oids = tuple(oid for oid, _ in tree_digests)
    if (
        tree_digests != tuple(sorted(tree_digests))
        or len(set(tree_oids)) != len(tree_oids)
    ):
        raise VerifierFailure
    observed_tree_oids = set(tree_oids)
    required_tree_oids = {history.manifest.base_tree_sha, history.candidate_tree_sha}
    required_tree_oids.update(edge.parent_tree_sha for edge in history.edges)
    required_tree_oids.update(edge.commit_tree_sha for edge in history.edges)
    if not required_tree_oids <= observed_tree_oids:
        raise VerifierFailure
    if any(
        not _is_hex(oid, oid_length) or not _is_hex(digest, 64)
        for oid, digest in tree_digests
    ):
        raise VerifierFailure
    for change in history.final_changes:
        if change.path not in history.manifest.contract.allowed_paths:
            raise VerifierFailure
        if change.old is None and change.new is None:
            raise VerifierFailure
        if change.old == change.new:
            raise VerifierFailure
        if not all(_valid_endpoint(endpoint, oid_length) for endpoint in (change.old, change.new)):
            raise VerifierFailure


def _count_change(
    change: CommittedPathChange,
    lines: dict[str, tuple[bytes, ...]],
    remaining: int,
    work: _EditWork,
) -> tuple[int, int]:
    old = () if change.old is None else lines[change.old.git_oid]
    new = () if change.new is None else lines[change.new.git_oid]
    distance = _bounded_edit_distance(old, new, remaining, work)
    additions_numerator = distance + len(new) - len(old)
    deletions_numerator = distance + len(old) - len(new)
    if additions_numerator % 2 or deletions_numerator % 2:
        raise VerifierFailure
    return additions_numerator // 2, deletions_numerator // 2


def _endpoint(transcript: _Transcript, prefix: str, value: VerifiedFileContent | None) -> None:
    transcript.field(f"{prefix}-present", b"\x00" if value is None else b"\x01")
    if value is None:
        return
    transcript.field(f"{prefix}-mode", value.mode.encode("ascii"))
    transcript.field(f"{prefix}-oid", value.git_oid.encode("ascii"))
    transcript.field(f"{prefix}-sha256", value.sha256.encode("ascii"))
    transcript.integer(f"{prefix}-size", len(value.content))


def _path_change(
    transcript: _Transcript,
    value: CommittedPathChange,
    additions: int | None = None,
    deletions: int | None = None,
) -> None:
    transcript.field("path", value.path.encode("ascii"))
    _endpoint(transcript, "old", value.old)
    _endpoint(transcript, "new", value.new)
    if additions is not None and deletions is not None:
        transcript.integer("additions", additions)
        transcript.integer("deletions", deletions)


def _fingerprint(
    history: CommittedCandidateHistory,
    counted_edges: tuple[_CountedEdge, ...],
    blobs: tuple[VerifiedFileContent, ...],
    cumulative_additions: int,
    cumulative_deletions: int,
) -> str:
    if not (
        len(history.commit_shas)
        == len(history.commit_object_sha256s)
        == len(history.edges)
        == len(counted_edges)
    ):
        raise VerifierFailure
    transcript = _Transcript()
    transcript.field("repository", REPOSITORY_ID)
    transcript.field("object-format", history.manifest.object_format.encode("ascii"))
    transcript.field("manifest-sha256", history.manifest.manifest_sha256.encode("ascii"))
    transcript.field("line-count-algorithm", LINE_COUNT_ALGORITHM.encode("ascii"))
    transcript.field("base-commit", history.manifest.base_commit_sha.encode("ascii"))
    transcript.field("base-commit-sha256", history.base_commit_sha256.encode("ascii"))
    transcript.field("base-tree", history.manifest.base_tree_sha.encode("ascii"))
    transcript.field("candidate-commit", history.candidate_commit_sha.encode("ascii"))
    transcript.field("candidate-tree", history.candidate_tree_sha.encode("ascii"))
    transcript.integer("commit-count", len(history.commit_shas))
    for commit, digest, edge in zip(
        history.commit_shas, history.commit_object_sha256s, history.edges
    ):
        transcript.field("commit-oid", commit.encode("ascii"))
        transcript.field("commit-sha256", digest.encode("ascii"))
        transcript.field("commit-tree", edge.commit_tree_sha.encode("ascii"))
    transcript.integer("tree-object-count", len(history.tree_object_sha256s))
    for oid, digest in history.tree_object_sha256s:
        transcript.field("tree-oid", oid.encode("ascii"))
        transcript.field("tree-sha256", digest.encode("ascii"))
    transcript.integer("edge-count", len(counted_edges))
    for counted in counted_edges:
        edge = counted.edge
        transcript.field("edge-parent-commit", edge.parent_commit_sha.encode("ascii"))
        transcript.field("edge-parent-tree", edge.parent_tree_sha.encode("ascii"))
        transcript.field("edge-commit", edge.commit_sha.encode("ascii"))
        transcript.field("edge-tree", edge.commit_tree_sha.encode("ascii"))
        transcript.integer("edge-change-count", len(counted.changes))
        for change in counted.changes:
            _path_change(
                transcript, change.change, change.additions, change.deletions
            )
    transcript.integer("final-change-count", len(history.final_changes))
    for final_change in history.final_changes:
        _path_change(transcript, final_change)
    transcript.integer("blob-count", len(blobs))
    for blob in blobs:
        transcript.field("blob-oid", blob.git_oid.encode("ascii"))
        transcript.integer("blob-size", len(blob.content))
        transcript.field("blob-content", blob.content)
    transcript.integer("cumulative-additions", cumulative_additions)
    transcript.integer("cumulative-deletions", cumulative_deletions)
    transcript.integer("maximum-changed-lines", history.manifest.contract.maximum_changed_lines)
    return transcript.hexdigest()


def verify_committed_candidate_content(
    repository: Path,
    base_sha: str,
    manifest_id: str,
    candidate_sha: str,
    *,
    now: datetime | None = None,
) -> CommittedCandidateContent:
    history = verify_committed_candidate_history(
        repository, base_sha, manifest_id, candidate_sha, now=now
    )
    _validate_history_observation(history, base_sha, manifest_id, candidate_sha)
    blobs = _collect_blobs(history)
    line_cache: dict[str, tuple[bytes, ...]] = {}
    total_line_records = 0
    for blob in blobs:
        records = _line_records(blob.content)
        total_line_records += len(records)
        if total_line_records > MAX_TOTAL_LINE_RECORDS:
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        line_cache[blob.git_oid] = records

    maximum = history.manifest.contract.maximum_changed_lines
    cumulative_additions = 0
    cumulative_deletions = 0
    counted_edges: list[_CountedEdge] = []
    work = _EditWork()
    for edge in history.edges:
        counted_changes: list[_CountedChange] = []
        edge_additions = 0
        edge_deletions = 0
        for change in edge.changes:
            remaining = maximum - cumulative_additions - cumulative_deletions
            additions, deletions = _count_change(change, line_cache, remaining, work)
            cumulative_additions += additions
            cumulative_deletions += deletions
            edge_additions += additions
            edge_deletions += deletions
            if cumulative_additions + cumulative_deletions > maximum:
                raise ManifestDenied("CANDIDATE_BUDGET_EXCEEDED")
            counted_changes.append(_CountedChange(change, additions, deletions))
        line_count = CandidateEdgeLineCount(
            edge.parent_commit_sha,
            edge.parent_tree_sha,
            edge.commit_sha,
            edge.commit_tree_sha,
            edge_additions,
            edge_deletions,
        )
        counted_edges.append(_CountedEdge(edge, tuple(counted_changes), line_count))

    counted = tuple(counted_edges)
    fingerprint = _fingerprint(
        history, counted, blobs, cumulative_additions, cumulative_deletions
    )
    return CommittedCandidateContent(
        history,
        tuple(item.line_count for item in counted),
        cumulative_additions,
        cumulative_deletions,
        fingerprint,
        len(blobs),
        sum(len(blob.content) for blob in blobs),
    )
