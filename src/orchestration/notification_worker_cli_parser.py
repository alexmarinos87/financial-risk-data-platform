"""Value-redacting argument parser for diagnostic worker preflight commands."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import NoReturn

PROGRAM = "check-notification-worker-preflight"
USAGE_ERROR = "Worker preflight usage is invalid; use --help. No execution permission granted.\n"


class WorkerPreflightParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        # The supplied argparse message may contain rejected credential values.
        self.exit(2, USAGE_ERROR)


def build_preflight_parser() -> argparse.ArgumentParser:
    parser = WorkerPreflightParser(
        prog=PROGRAM, allow_abbrev=False,
        description="Validate captured worker authority or explicitly read current authority; never execute.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--snapshot", type=Path, help="Validate retained evidence without database access")
    source.add_argument("--read-current", action="store_true", help="Explicitly read current PostgreSQL authority")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--selected-transition-id", required=True)
    parser.add_argument("--scheduled-for", required=True)
    parser.add_argument("--worker-config", type=Path, default=Path("config/notification_workers.yaml"))
    parser.add_argument("--delivery-config", type=Path, default=Path("config/notification_delivery.yaml"))
    parser.add_argument("--destination-config", type=Path, default=Path("config/notification_destinations.yaml"))
    return parser
