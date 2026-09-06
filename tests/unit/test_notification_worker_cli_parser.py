from __future__ import annotations

import argparse
import contextlib
import io
import sys
from pathlib import Path

import pytest

from src.orchestration.notification_worker_cli_parser import (
    PROGRAM, USAGE_ERROR, build_preflight_parser,
)

SELECTION = ["--worker-id", "worker", "--selected-transition-id", "transition",
             "--scheduled-for", "2026-09-06T12:00:00+00:00"]
SENTINEL = "synthetic-sensitive-value-NOT-A-REAL-CREDENTIAL"


@pytest.mark.parametrize("arguments", [
    ["--read-current", *SELECTION, "--dsn", SENTINEL],
    ["--read-current", *SELECTION, f"--dsn={SENTINEL}"],
    ["--read-current", *SELECTION, SENTINEL],
    [f"--read-current={SENTINEL}", *SELECTION],
    ["--read-current", "--snapshot", SENTINEL, *SELECTION],
    ["--read-current", *SELECTION, "--worker-config"],
    ["--read", *SELECTION], [],
])
def test_usage_errors_never_echo_values(arguments: list[str]) -> None:
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        with pytest.raises(SystemExit) as caught:
            build_preflight_parser().parse_args(arguments)
    assert caught.value.code == 2
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == USAGE_ERROR
    assert SENTINEL not in stderr.getvalue()


def test_standard_argparse_reproduces_the_original_echo() -> None:
    parser = argparse.ArgumentParser(prog=PROGRAM)
    parser.add_argument("--read-current", action="store_true")
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr), pytest.raises(SystemExit):
        parser.parse_args(["--read-current", "--dsn", SENTINEL])
    assert SENTINEL in stderr.getvalue()


@pytest.mark.parametrize("source", [["--read-current"], ["--snapshot", "captured.json"]])
def test_valid_selection_preserves_existing_defaults(source: list[str]) -> None:
    parsed = build_preflight_parser().parse_args([*source, *SELECTION])
    assert parsed.worker_id == "worker"
    assert parsed.selected_transition_id == "transition"
    assert parsed.worker_config == Path("config/notification_workers.yaml")
    assert parsed.delivery_config == Path("config/notification_delivery.yaml")
    assert parsed.destination_config == Path("config/notification_destinations.yaml")
    assert parsed.read_current is (source[0] == "--read-current")


def test_help_is_available_and_uses_a_fixed_program_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", [f"/private/{SENTINEL}/worker.py"])
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        with pytest.raises(SystemExit) as caught:
            build_preflight_parser().parse_args(["--help"])
    assert caught.value.code == 0
    assert PROGRAM in stdout.getvalue()
    assert "--snapshot" in stdout.getvalue() and "--read-current" in stdout.getvalue()
    assert SENTINEL not in stdout.getvalue()
    assert stderr.getvalue() == ""


def test_usage_errors_ignore_dynamic_program_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", [SENTINEL])
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr), pytest.raises(SystemExit):
        build_preflight_parser().parse_args([])
    assert stderr.getvalue() == USAGE_ERROR
