from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_OVERNIGHT_COMMANDS = {
    "git-status": ["git", "status", "--short", "--branch"],
    "security-check": ["make", "security-check"],
    "readiness-check": ["make", "readiness-check"],
}
REQUIRED_OVERNIGHT_GUARDRAILS = {
    "push": False,
    "merge": False,
    "deploy": False,
    "terraform_apply": False,
    "cloud_environment_allowed": False,
}
RISK_RULES = (
    (
        100,
        "cloud, deployment, or CI control",
        (".github/", "deploy/", "infra/"),
    ),
    (
        95,
        "database schema or source-system contract",
        ("sql/", "mongo/", "docker-compose.yml"),
    ),
    (
        90,
        "warehouse load, backfill, or lock behaviour",
        ("src/warehouse/", "src/orchestration/backfill.py", "src/orchestration/locks.py"),
    ),
    (
        85,
        "data-quality or risk threshold behaviour",
        ("src/analytics/data_quality.py", "config/risk_thresholds.yaml"),
    ),
    (
        80,
        "security, validation, dependency, or repository control",
        (
            "AGENTS.md",
            "Makefile",
            "Dockerfile",
            ".gitignore",
            ".dockerignore",
            "pyproject.toml",
            "config/",
            "scripts/security_check.py",
            "scripts/overnight_sandbox.py",
            "scripts/morning_review.py",
        ),
    ),
    (70, "test evidence for the acceptance-package control", ("tests/unit/test_morning_review.py",)),
)
SECRET_LIKE_SUFFIXES = (".env", ".key", ".pem", ".p12", ".pfx", ".tfvars")
SECRET_LIKE_NAMES = {"credentials", "id_rsa", "id_ed25519", ".netrc"}
SENSITIVE_NAME_TOKENS = {
    "aws",
    "cloud",
    "credential",
    "credentials",
    "iam",
    "password",
    "policies",
    "policy",
    "secret",
    "secrets",
    "token",
}


class ReviewError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def decode_git(value: bytes) -> str:
    return value.decode("utf-8", errors="backslashreplace")


def run_git(root: Path, args: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def require_git(root: Path, args: list[str], label: str) -> bytes:
    result = run_git(root, args)
    if result.returncode != 0:
        detail = decode_git(result.stderr).strip() or "unknown Git error"
        raise ReviewError(f"Unable to collect {label}: {detail}")
    return result.stdout


def parse_name_status(output: bytes) -> list[dict[str, Any]]:
    parts = output.rstrip(b"\0").split(b"\0") if output else []
    entries: list[dict[str, Any]] = []
    index = 0
    while index < len(parts):
        status = decode_git(parts[index])
        index += 1
        if not status:
            continue
        path_count = 2 if status[0] in {"R", "C"} else 1
        if index + path_count > len(parts):
            raise ReviewError("Git returned an incomplete name-status record")
        paths = [decode_git(value) for value in parts[index : index + path_count]]
        index += path_count
        entries.append({"status": status, "paths": paths})
    return entries


def parse_paths(output: bytes) -> list[str]:
    return [decode_git(value) for value in output.rstrip(b"\0").split(b"\0") if value]


def collect_untracked_metadata(root: Path, paths: list[str]) -> list[dict[str, object]]:
    metadata: list[dict[str, object]] = []
    for path in paths:
        candidate = root / path
        try:
            details = candidate.lstat()
        except OSError as exc:
            metadata.append({"path": path, "kind": "unavailable", "error": str(exc)})
            continue
        item: dict[str, object] = {"path": path, "size_bytes": details.st_size}
        if stat.S_ISLNK(details.st_mode):
            item["kind"] = "symlink"
        elif stat.S_ISREG(details.st_mode):
            item["kind"] = "file"
            if details.st_size <= 5_000_000:
                try:
                    line_count = 0
                    last_byte = b""
                    with candidate.open("rb") as source:
                        for chunk in iter(lambda: source.read(65536), b""):
                            line_count += chunk.count(b"\n")
                            last_byte = chunk[-1:]
                    if details.st_size and last_byte != b"\n":
                        line_count += 1
                    item["line_count"] = line_count
                except OSError as exc:
                    item["line_count_error"] = str(exc)
        else:
            item["kind"] = "other"
        metadata.append(item)
    return metadata


def git_snapshot(root: Path) -> dict[str, Any]:
    head_result = run_git(root, ["rev-parse", "--verify", "HEAD"])
    head = decode_git(head_result.stdout).strip() if head_result.returncode == 0 else None
    branch_result = run_git(root, ["symbolic-ref", "--quiet", "--short", "HEAD"])
    branch = (
        decode_git(branch_result.stdout).strip()
        if branch_result.returncode == 0
        else "DETACHED_OR_UNBORN"
    )
    status = require_git(
        root,
        ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        "repository status",
    )
    return {
        "head_sha": head,
        "branch": branch,
        "path_status_sha256": hashlib.sha256(status).hexdigest(),
    }


def resolve_base(root: Path, base_ref: str, head_sha: object) -> dict[str, Any]:
    result: dict[str, Any] = {
        "requested_ref": base_ref,
        "base_sha": None,
        "merge_base_sha": None,
        "status": "MISSING",
        "message": "HEAD or base ref is unavailable; committed comparison was not collected.",
    }
    if not isinstance(head_sha, str):
        return result

    base = run_git(
        root,
        ["rev-parse", "--verify", "--end-of-options", f"{base_ref}^{{commit}}"],
    )
    if base.returncode != 0:
        result["message"] = f"Base ref {base_ref!r} did not resolve; no fetch was attempted."
        return result

    base_sha = decode_git(base.stdout).strip()
    merge_base = run_git(root, ["merge-base", "--", base_sha, head_sha])
    result["base_sha"] = base_sha
    if merge_base.returncode != 0:
        result["status"] = "UNRELATED"
        result["message"] = "Base and HEAD have no merge base; committed comparison is unavailable."
        return result

    result.update(
        {
            "merge_base_sha": decode_git(merge_base.stdout).strip(),
            "status": "AVAILABLE",
            "message": "Base resolved locally; no fetch was attempted.",
        }
    )
    return result


def collect_git_evidence(root: Path, base_ref: str) -> dict[str, Any]:
    inside = run_git(root, ["rev-parse", "--is-inside-work-tree"])
    if inside.returncode != 0 or decode_git(inside.stdout).strip() != "true":
        raise ReviewError(f"Not a Git worktree: {root}")

    start = git_snapshot(root)
    base = resolve_base(root, base_ref, start["head_sha"])
    merge_base = base["merge_base_sha"]

    committed: list[dict[str, Any]] = []
    if isinstance(merge_base, str) and isinstance(start["head_sha"], str):
        committed = parse_name_status(
            require_git(
                root,
                ["diff", "--name-status", "-z", f"{merge_base}..{start['head_sha']}"],
                "committed changes",
            )
        )

    staged = parse_name_status(
        require_git(root, ["diff", "--cached", "--name-status", "-z"], "staged changes")
    )
    unstaged = parse_name_status(
        require_git(root, ["diff", "--name-status", "-z"], "unstaged changes")
    )
    untracked = parse_paths(
        require_git(
            root,
            ["ls-files", "--others", "--exclude-standard", "-z"],
            "untracked paths",
        )
    )
    conflicts = parse_paths(
        require_git(
            root,
            ["diff", "--name-only", "--diff-filter=U", "-z"],
            "unresolved conflicts",
        )
    )

    diff_args = ["diff", "--stat"]
    if isinstance(merge_base, str):
        diff_args.append(merge_base)
    elif isinstance(start["head_sha"], str):
        diff_args.append("HEAD")

    diff_stat_result = run_git(root, diff_args)
    if diff_stat_result.returncode != 0:
        raise ReviewError(
            "Unable to collect diff statistics: "
            + (decode_git(diff_stat_result.stderr).strip() or "unknown Git error")
        )
    entries = [*committed, *staged, *unstaged]
    changed_paths: set[str] = set()
    for entry in entries:
        paths = entry["paths"]
        if isinstance(paths, list):
            changed_paths.update(path for path in paths if isinstance(path, str))
    changed_paths.update(untracked)
    changed_paths.update(conflicts)

    return {
        "start": start,
        "base": base,
        "inventories": {
            "committed": committed,
            "staged": staged,
            "unstaged": unstaged,
            "untracked": untracked,
            "untracked_metadata": collect_untracked_metadata(root, untracked),
            "conflicts": conflicts,
        },
        "changed_paths": sorted(changed_paths),
        "diff_stat": [line for line in decode_git(diff_stat_result.stdout).splitlines() if line],
    }


def classify_path(path: str) -> dict[str, Any] | None:
    matches: list[str] = []
    score = 0
    for rule_score, reason, prefixes in RISK_RULES:
        if any(
            path.startswith(prefix) if prefix.endswith("/") else path == prefix
            for prefix in prefixes
        ):
            score = max(score, rule_score)
            matches.append(reason)

    name = Path(path).name.lower()
    name_tokens = set(re.split(r"[^a-z0-9]+", path.lower()))
    if (
        name in SECRET_LIKE_NAMES
        or name == ".env"
        or name.startswith(".env.")
        or any(name.endswith(suffix) for suffix in SECRET_LIKE_SUFFIXES)
    ):
        score = max(score, 100)
        matches.append("secret-like path name")
    if name_tokens.intersection(SENSITIVE_NAME_TOKENS):
        score = max(score, 90)
        matches.append("secret, credential, IAM, or cloud-related path name")

    if not matches:
        return None
    return {"path": path, "score": score, "reasons": sorted(set(matches))}


def collect_risk_flags(paths: list[str]) -> list[dict[str, Any]]:
    flags = [flag for path in paths if (flag := classify_path(path)) is not None]
    return sorted(flags, key=lambda item: (-int(item["score"]), str(item["path"])))


def parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def collect_overnight_evidence(
    root: Path,
    now: datetime,
    stale_hours: float,
) -> dict[str, Any]:
    overnight_root = root / ".sandbox" / "overnight"
    if not overnight_root.exists():
        return {"status": "MISSING", "reason": "No overnight run directory exists."}

    sandbox_root = root / ".sandbox"
    sandbox_resolved = sandbox_root.resolve()
    if sandbox_root.is_symlink() or not sandbox_resolved.is_relative_to(root.resolve()):
        return {"status": "FAIL", "reason": ".sandbox escapes the repository through a symlink."}
    overnight_resolved = overnight_root.resolve()
    if overnight_root.is_symlink() or not overnight_resolved.is_relative_to(sandbox_resolved):
        return {"status": "FAIL", "reason": "Overnight root escapes .sandbox through a symlink."}
    try:
        run_entries = sorted(
            overnight_root.iterdir(),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
    except OSError as exc:
        return {"status": "FAIL", "reason": f"Overnight run discovery failed: {exc}"}
    if not run_entries:
        return {"status": "MISSING", "reason": "No overnight run directory exists."}

    run_dir = run_entries[0]
    if (
        run_dir.is_symlink()
        or not run_dir.is_dir()
        or not run_dir.resolve().is_relative_to(overnight_resolved)
    ):
        return {"status": "FAIL", "reason": "Newest overnight entry is not a safe run directory."}
    summary_path = run_dir / "summary.json"
    relative_path = summary_path.relative_to(root).as_posix()
    if summary_path.is_symlink() or not summary_path.is_file():
        return {
            "status": "FAIL",
            "reason": "The newest overnight run has no summary and may be incomplete.",
            "path": relative_path,
        }

    try:
        before = summary_path.stat()
        if before.st_size > 1_000_000:
            raise ValueError("summary exceeds the 1 MB safety limit")
        raw = summary_path.read_bytes()
        after = summary_path.stat()
        if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
            raise ValueError("summary changed while it was being read")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("summary root is not an object")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "FAIL", "reason": f"Overnight summary is unusable: {exc}", "path": relative_path}

    cycles = payload.get("cycles")
    finished_at = parse_utc(payload.get("finished_at"))
    result: dict[str, Any] = {
        "status": "FAIL",
        "reason": "Overnight summary is incomplete or failed.",
        "path": relative_path,
        "run_id": payload.get("run_id") if isinstance(payload.get("run_id"), str) else None,
        "started_at": payload.get("started_at") if isinstance(payload.get("started_at"), str) else None,
        "finished_at": finished_at.isoformat() if finished_at else None,
        "cycle_count": len(cycles) if isinstance(cycles, list) else 0,
    }
    if payload.get("status") != "passed" or not isinstance(cycles, list) or not cycles:
        return result

    guardrails = payload.get("guardrails")
    if not isinstance(guardrails, dict) or any(
        guardrails.get(name) is not expected
        for name, expected in REQUIRED_OVERNIGHT_GUARDRAILS.items()
    ):
        result["reason"] = "Overnight guardrails are missing or unsafe."
        return result

    for cycle in cycles:
        if not isinstance(cycle, dict) or cycle.get("status") != "passed":
            return result
        commands = cycle.get("commands")
        if not isinstance(commands, list) or not commands:
            return result
        if len(commands) != len(EXPECTED_OVERNIGHT_COMMANDS):
            return result
        observed: dict[str, dict[str, object]] = {}
        for command in commands:
            if not isinstance(command, dict) or not isinstance(command.get("name"), str):
                return result
            name = command["name"]
            if name in observed:
                return result
            observed[name] = command
        if set(observed) != set(EXPECTED_OVERNIGHT_COMMANDS):
            return result
        for name, expected_command in EXPECTED_OVERNIGHT_COMMANDS.items():
            command = observed[name]
            returncode = command.get("returncode")
            if command.get("command") != expected_command or type(returncode) is not int or returncode != 0:
                return result

    if finished_at is None:
        result["reason"] = "Passed run has no valid completion timestamp."
        return result
    if finished_at > now:
        result["reason"] = "Overnight completion timestamp is in the future."
        return result
    age_hours = (now - finished_at).total_seconds() / 3600
    result["age_hours"] = round(age_hours, 2)
    if age_hours > stale_hours:
        result.update({"status": "STALE", "reason": "Overnight evidence is older than the freshness limit."})
        return result

    result.update(
        {
            "status": "STALE",
            "reason": (
                "Run completed successfully but the current sandbox schema does not record a "
                "content-complete Git fingerprint, so it is historical evidence only."
            ),
        }
    )
    return result


def escaped_text(value: object) -> str:
    text = str(value)
    return "".join(
        character
        if ord(character) >= 32 and ord(character) != 127
        else f"\\x{ord(character):02x}"
        for character in text
    )


def code_span(value: object) -> str:
    value_text = escaped_text(value)
    longest = 0
    current = 0
    for character in value_text:
        if character == "`":
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    delimiter = "`" * max(1, longest + 1)
    return f"{delimiter} {value_text} {delimiter}"


def render_entries(entries: list[dict[str, Any]]) -> list[str]:
    if not entries:
        return ["- None recorded."]
    lines: list[str] = []
    for entry in entries:
        raw_paths = entry["paths"]
        if not isinstance(raw_paths, list):
            raise ReviewError("Invalid path inventory")
        paths = " -> ".join(code_span(path) for path in raw_paths)
        lines.append(f"- {code_span(entry['status'])}: {paths}")
    return lines


def render_report(evidence: dict[str, Any]) -> str:
    git = evidence["git"]
    inventories = git["inventories"]
    base = git["base"]
    snapshot = git["end"]
    risk_flags = evidence["risk_flags"]
    overnight = evidence["validation"]["overnight"]
    lines = [
        "# Morning Acceptance Package",
        "",
        f"Generated: {code_span(evidence['generated_at'])}",
        "",
        "Decision: **PENDING — human acceptance required**",
        "",
        "> This package is an automated inventory, not a semantic code review, approval,",
        "> merge recommendation, or deployment authorisation. No network fetch was run.",
        "",
        "## Repository Snapshot",
        "",
        f"- Branch: {code_span(snapshot['branch'])}",
        f"- HEAD: {code_span(snapshot['head_sha'] or 'UNAVAILABLE')}",
        f"- Requested base: {code_span(base['requested_ref'])}",
        f"- Base SHA: {code_span(base['base_sha'] or 'UNAVAILABLE')}",
        f"- Merge base: {code_span(base['merge_base_sha'] or 'UNAVAILABLE')}",
        f"- Base status: **{escaped_text(base['status'])}** — {code_span(base['message'])}",
        "- Path/status inventory unchanged during collection: "
        f"**{'YES' if git['path_status_unchanged'] else 'NO'}** ",
        "  (this does not verify unchanged file contents)",
        "",
    ]

    for title, key in (
        ("Committed Since Merge Base", "committed"),
        ("Staged", "staged"),
        ("Unstaged", "unstaged"),
    ):
        lines.extend([f"### {title}", "", *render_entries(inventories[key]), ""])

    for title, key in (("Untracked", "untracked"), ("Unresolved Conflicts", "conflicts")):
        lines.extend([f"### {title}", ""])
        values = inventories[key]
        if key == "untracked":
            metadata_by_path = {item["path"]: item for item in inventories["untracked_metadata"]}
            for path in values:
                item = metadata_by_path[path]
                detail = f"{item.get('size_bytes', 'unknown')} bytes"
                if item.get("line_count") is not None:
                    detail += f", {item['line_count']} lines"
                lines.append(f"- {code_span(path)} — {escaped_text(detail)}")
            if not values:
                lines.append("- None recorded.")
        else:
            lines.extend([f"- {code_span(path)}" for path in values] or ["- None recorded."])
        lines.append("")

    lines.extend(["## Tracked Diff Statistics", ""])
    lines.extend([f"- {code_span(line)}" for line in git["diff_stat"]] or ["- None recorded."])
    lines.extend(["", "## Sensitive-Path Flags", ""])
    if risk_flags:
        for flag in risk_flags:
            reasons = ", ".join(escaped_text(reason) for reason in flag["reasons"])
            lines.append(f"- {code_span(flag['path'])}: {reasons}")
    else:
        lines.append("- No repository-specific sensitive path rule matched.")
    lines.extend(
        [
            "",
            "These flags prioritise human inspection; they are not secret scanning or proof of risk.",
            "",
            "## Inspect First",
            "",
        ]
    )
    for index, path in enumerate(evidence["inspection_candidates"], start=1):
        lines.append(f"{index}. {code_span(path)}")
    if not evidence["inspection_candidates"]:
        lines.append("No changed path was recorded.")

    lines.extend(
        [
            "",
            "## Automated Evidence",
            "",
            "- Git diff check: **NOT RUN BY THIS COMMAND**",
            f"- Overnight validation: **{escaped_text(overnight['status'])}** — "
            f"{code_span(overnight['reason'])}",
        ]
    )
    if overnight.get("finished_at"):
        lines.append(f"- Overnight completion: {code_span(overnight['finished_at'])}")
    if overnight.get("age_hours") is not None:
        lines.append(f"- Overnight age (hours): {code_span(overnight['age_hours'])}")
    for tool, available in evidence["tool_availability"].items():
        lines.append(f"- {escaped_text(tool)} CLI: **{'AVAILABLE' if available else 'UNAVAILABLE'}**")
    lines.append("- Docker-backed database validation: **NOT COLLECTED BY THIS COMMAND**")
    lines.extend(
        [
            "",
            "## Read-Only Reviewer Findings",
            "",
            "- [ ] Correctness and maintainability findings triaged.",
            "- [ ] Production-failure challenge findings triaged.",
            "- [ ] Accepted fixes were re-reviewed and affected checks were rerun.",
            "",
            "## Human Review Notes",
            "",
            "- [ ] Objective and scope are understood.",
            "- [ ] Architecture and important data/control paths are understood.",
            "- [ ] State, side effects, idempotency, retries, backfills, and locks are understood.",
            "- [ ] Assumptions and production failure modes are challenged.",
            "- [ ] Rollback, recovery, and observability are adequate.",
            "- [ ] Security, permissions, and network exposure are appropriate.",
            "- [ ] Performance, operational, and cost implications are understood.",
            "- [ ] Test evidence and unavailable checks were verified for this snapshot.",
            "- [ ] Unresolved questions and reviewer findings are explicitly dispositioned.",
            "",
            "## Human Decision",
            "",
            "**PENDING.** This generator cannot mark a change accepted or merge-ready.",
            "",
        ]
    )
    return "\n".join(lines)


def path_is_ignored(root: Path, path: Path) -> bool:
    relative = path.relative_to(root).as_posix()
    result = run_git(root, ["check-ignore", "--quiet", "--no-index", "--", relative])
    return result.returncode == 0


def prepare_output_dir(root: Path, requested: Path | None, now: datetime) -> Path:
    root_resolved = root.resolve()
    review_root = root / ".sandbox" / "review-packages"
    review_resolved = review_root.resolve(strict=False)
    if not review_resolved.is_relative_to(root_resolved):
        raise ReviewError("Review root escapes the repository through a symlink")

    run_name = now.strftime("%Y%m%d-%H%M%S-%f")
    candidate = review_root / run_name if requested is None else requested
    if not candidate.is_absolute():
        candidate = review_root / candidate
    candidate_resolved = candidate.resolve(strict=False)
    if not candidate_resolved.is_relative_to(review_resolved):
        raise ReviewError("Output must stay beneath .sandbox/review-packages")
    if candidate.exists():
        raise ReviewError(f"Output already exists: {candidate}")
    if not path_is_ignored(root, candidate):
        raise ReviewError("Output path is not ignored by Git")

    review_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(review_root, 0o700)
    candidate.mkdir(mode=0o700)
    return candidate


def atomic_write(path: Path, payload: str) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(payload)
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def generate_package(
    root: Path,
    base_ref: str,
    requested_output: Path | None,
    now: datetime,
    stale_hours: float,
) -> tuple[Path, Path, dict[str, Any]]:
    git = collect_git_evidence(root, base_ref)
    output_dir = prepare_output_dir(root, requested_output, now)
    start = git["start"]

    changed_paths = git["changed_paths"]
    risk_flags = collect_risk_flags(changed_paths)
    risk_scores = {str(flag["path"]): int(flag["score"]) for flag in risk_flags}
    inspection_candidates = sorted(
        changed_paths,
        key=lambda path: (-risk_scores.get(path, 0), path),
    )[:10]
    overnight = collect_overnight_evidence(root, now, stale_hours)
    end = git_snapshot(root)
    git["end"] = end
    git["path_status_unchanged"] = start == end
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": now.isoformat(),
        "decision": "PENDING",
        "git": git,
        "risk_flags": risk_flags,
        "inspection_candidates": inspection_candidates,
        "validation": {
            "overnight": overnight,
        },
        "tool_availability": {
            "docker": shutil.which("docker") is not None,
            "terraform": shutil.which("terraform") is not None,
            "kubectl": shutil.which("kubectl") is not None,
        },
    }

    report_path = output_dir / "review.md"
    evidence_path = output_dir / "evidence.json"
    evidence_payload = json.dumps(evidence, indent=2, ensure_ascii=True) + "\n"
    report_payload = render_report(evidence)
    atomic_write(evidence_path, evidence_payload)
    atomic_write(report_path, report_payload)
    return report_path, evidence_path, evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate local Git and validation evidence for human morning review."
    )
    parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="local comparison ref; the command never fetches it",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="new directory beneath .sandbox/review-packages",
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=24.0,
        help="maximum age for attributed overnight evidence",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.stale_hours <= 0:
        print("--stale-hours must be greater than zero", file=sys.stderr)
        return 2
    try:
        report_path, evidence_path, _ = generate_package(
            root=ROOT,
            base_ref=args.base_ref,
            requested_output=args.output_dir,
            now=utc_now(),
            stale_hours=args.stale_hours,
        )
    except (OSError, ReviewError) as exc:
        print(f"Morning review package failed: {exc}", file=sys.stderr)
        return 1

    print(f"Review package: {report_path.relative_to(ROOT)}")
    print(f"Evidence: {evidence_path.relative_to(ROOT)}")
    print("Decision: PENDING — human acceptance required")
    return 0


if __name__ == "__main__":
    sys.exit(main())
