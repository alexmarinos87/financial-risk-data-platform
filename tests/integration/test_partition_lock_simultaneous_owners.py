"""Competing processes retain ownership until every acquisition result is observed."""

from __future__ import annotations

import json
import multiprocessing
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks


def _contend(
    root: str, partitions: list[str], owner: str, start: Any, release: Any,
    result: Connection,
) -> None:
    held: list[Path] = []
    try:
        start.wait(timeout=10)
        try:
            held = acquire_partition_locks(Path(root), partitions, owner=owner)
        except OverlapError:
            result.send(("blocked", owner, []))
            return
        result.send(("acquired", owner, [str(path) for path in held]))
        # Do not let a fast winner finish before its competitor has attempted.
        if not release.wait(timeout=30):
            raise TimeoutError("Parent did not release the test lock owner")
    finally:
        release_partition_locks(held)
        result.close()


@pytest.mark.parametrize("requests,expected_owners", [
    pytest.param([["shared"], ["shared"]], 1, id="same-partition"),
    pytest.param([["b", "a", "a"], ["a", "b"]], 1, id="reversed-duplicates"),
    pytest.param([["a", "shared"], ["b", "shared"]], 1, id="private-before-shared"),
    pytest.param([["a"], ["b"]], 2, id="disjoint-partitions"),
])
def test_simultaneous_processes_preserve_exclusive_ownership_and_allow_retry(
    tmp_path: Path, requests: list[list[str]], expected_owners: int,
) -> None:
    context = multiprocessing.get_context("spawn")
    start = context.Barrier(3)
    release = context.Event()
    channels = [context.Pipe(duplex=False) for _ in requests]
    processes = [
        context.Process(
            target=_contend,
            args=(str(tmp_path), partitions, f"owner-{index}", start, release, channels[index][1]),
        )
        for index, partitions in enumerate(requests)
    ]
    try:
        for process, (_, sender) in zip(processes, channels, strict=True):
            process.start()
            sender.close()
        start.wait(timeout=10)
        outcomes = []
        for receiver, _ in channels:
            assert receiver.poll(15), "Contender did not report an acquisition outcome"
            outcomes.append(receiver.recv())
        assert sorted(item[0] for item in outcomes) == sorted(
            ["acquired"] * expected_owners + ["blocked"] * (2 - expected_owners)
        )
        expected_paths: set[Path] = set()
        for index, (status, owner, paths) in enumerate(outcomes):
            assert owner == f"owner-{index}"
            if status == "blocked":
                assert paths == []
                continue
            expected = [
                tmp_path / ".orchestration_locks" / partition / ".lock"
                for partition in sorted(set(requests[index]))
            ]
            assert [Path(path) for path in paths] == expected
            expected_paths.update(expected)
            for path in expected:
                assert json.loads(path.read_text(encoding="utf-8"))["owner"] == owner
        # A losing contender's private earlier acquisition must not be left behind.
        assert set(tmp_path.rglob(".lock")) == expected_paths
    finally:
        release.set()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=5)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=5)
        for receiver, sender in channels:
            receiver.close()
            sender.close()
    assert all(process.exitcode == 0 for process in processes)
    assert list(tmp_path.rglob(".lock")) == []
    all_partitions = sorted({partition for request in requests for partition in request})
    retried = acquire_partition_locks(tmp_path, all_partitions, owner="retry")
    try:
        assert len(retried) == len(all_partitions)
    finally:
        release_partition_locks(retried)
    assert list(tmp_path.rglob(".lock")) == []
