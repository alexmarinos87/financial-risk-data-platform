from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = ROOT / ".sandbox" / "overnight"
CLOUD_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "AZURE_CLIENT_SECRET",
)

COMMANDS = (
    ("git-status", ["git", "status", "--short", "--branch"]),
    ("security-check", ["make", "security-check"]),
    ("readiness-check", ["make", "readiness-check"]),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_command(name: str, command: list[str], log_dir: Path, timeout_seconds: int) -> dict[str, object]:
    started_at = utc_now()
    output_path = log_dir / f"{name}.log"

    with output_path.open("w", encoding="utf-8") as output:
        output.write(f"$ {' '.join(command)}\n")
        output.write(f"started_at={started_at.isoformat()}\n\n")
        output.flush()

        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=output,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )

    finished_at = utc_now()
    return {
        "name": name,
        "command": command,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "returncode": result.returncode,
        "log": output_path.relative_to(ROOT).as_posix(),
    }


def check_environment(allow_cloud_env: bool) -> list[str]:
    failures: list[str] = []

    if not shutil.which("git"):
        failures.append("git is not available")
    if not shutil.which("make"):
        failures.append("make is not available")

    if not allow_cloud_env:
        present_cloud_vars = [name for name in CLOUD_ENV_VARS if os.environ.get(name)]
        if present_cloud_vars:
            failures.append(
                "cloud credential environment variables are present: "
                + ", ".join(present_cloud_vars)
            )

    return failures


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a timeboxed local validation loop for overnight repository checks. "
            "This command does not push, merge, deploy, or run Terraform apply."
        )
    )
    parser.add_argument("--hours", type=float, default=8.0, help="maximum runtime in hours")
    parser.add_argument("--cycles", type=int, default=None, help="maximum number of cycles")
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=1800,
        help="pause between successful cycles",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=1200,
        help="timeout per validation command",
    )
    parser.add_argument(
        "--allow-cloud-env",
        action="store_true",
        help="allow cloud credential environment variables to be present",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    failures = check_environment(allow_cloud_env=args.allow_cloud_env)
    if failures:
        for failure in failures:
            print(f"Refusing to start overnight sandbox: {failure}", file=sys.stderr)
        return 1

    run_id = utc_now().strftime("%Y%m%d-%H%M%S")
    run_dir = LOG_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    deadline = utc_now() + timedelta(hours=args.hours)
    cycle = 0
    summary: dict[str, object] = {
        "run_id": run_id,
        "started_at": utc_now().isoformat(),
        "deadline": deadline.isoformat(),
        "commands": [name for name, _ in COMMANDS],
        "cycles": [],
        "guardrails": {
            "push": False,
            "merge": False,
            "deploy": False,
            "terraform_apply": False,
            "cloud_environment_allowed": args.allow_cloud_env,
        },
    }

    print(f"Overnight sandbox logs: {run_dir.relative_to(ROOT)}", flush=True)

    while utc_now() < deadline and (args.cycles is None or cycle < args.cycles):
        cycle += 1
        cycle_dir = run_dir / f"cycle-{cycle:03d}"
        cycle_dir.mkdir(parents=True, exist_ok=True)
        print(f"Starting cycle {cycle}", flush=True)

        cycle_result: dict[str, object] = {
            "cycle": cycle,
            "started_at": utc_now().isoformat(),
            "commands": [],
        }

        failed = False
        for name, command in COMMANDS:
            result = run_command(name, command, cycle_dir, args.timeout_seconds)
            cycle_result["commands"].append(result)  # type: ignore[index]
            if result["returncode"] != 0:
                failed = True
                break

        cycle_result["finished_at"] = utc_now().isoformat()
        cycle_result["status"] = "failed" if failed else "passed"
        summary["cycles"].append(cycle_result)  # type: ignore[index]
        write_json(run_dir / "summary.json", summary)

        if failed:
            print(f"Cycle {cycle} failed. See {cycle_dir.relative_to(ROOT)}", flush=True)
            summary["finished_at"] = utc_now().isoformat()
            summary["status"] = "failed"
            write_json(run_dir / "summary.json", summary)
            return 1

        if args.cycles is not None and cycle >= args.cycles:
            break
        if utc_now() + timedelta(seconds=args.sleep_seconds) >= deadline:
            break

        print(f"Cycle {cycle} passed. Sleeping {args.sleep_seconds} seconds.", flush=True)
        time.sleep(args.sleep_seconds)

    summary["finished_at"] = utc_now().isoformat()
    summary["status"] = "passed"
    write_json(run_dir / "summary.json", summary)
    print(f"Overnight sandbox completed: {run_dir.relative_to(ROOT) / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
