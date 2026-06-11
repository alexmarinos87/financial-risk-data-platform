from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_TRACKED_PREFIXES = (
    ".benchmarks/",
    ".demo/",
    ".sandbox/",
    ".venv/",
    "data/",
)

FORBIDDEN_TRACKED_NAMES = {
    ".env",
    ".env.local",
    ".env.production",
    ".netrc",
    "credentials",
    "id_rsa",
    "id_ed25519",
}

SECRET_PATTERNS = (
    ("AWS access key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("GitHub token", re.compile(r"\bgh[opsu]_[A-Za-z0-9_]{30,}\b")),
    ("private key", re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA |)?PRIVATE KEY-----")),
    ("Slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
)

REQUIRED_FALSE_FLAGS = (
    "create_rds_postgres",
    "create_aurora_postgres",
    "create_documentdb_cluster",
)

KUBERNETES_CONFIG_COPIES = (
    ("config/storage.yaml", "deploy/kubernetes/base/config/storage.yaml"),
    ("config/risk_thresholds.yaml", "deploy/kubernetes/base/config/risk_thresholds.yaml"),
    ("config/symbols.yaml", "deploy/kubernetes/base/config/symbols.yaml"),
)


def run_git(args: list[str]) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return [line for line in result.stdout.splitlines() if line]


def tracked_and_untracked_files() -> list[Path]:
    tracked = run_git(["ls-files"])
    untracked = run_git(["ls-files", "--others", "--exclude-standard"])
    return [ROOT / path for path in [*tracked, *untracked]]


def is_text_file(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    return True


def relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def check_tracked_paths(failures: list[str]) -> None:
    for path in run_git(["ls-files"]):
        name = Path(path).name
        if path.startswith(FORBIDDEN_TRACKED_PREFIXES):
            failures.append(f"Generated or local-only path is tracked: {path}")
        if name in FORBIDDEN_TRACKED_NAMES:
            failures.append(f"Sensitive local file is tracked: {path}")
        if path.endswith(".tfvars") and not path.endswith(".tfvars.example"):
            failures.append(f"Terraform variable file is tracked: {path}")


def check_secret_patterns(files: list[Path], failures: list[str]) -> None:
    for path in files:
        if not is_text_file(path):
            continue
        text = path.read_text(encoding="utf-8")
        for label, pattern in SECRET_PATTERNS:
            if pattern.search(text):
                failures.append(f"{label} pattern found in {relative(path)}")


def check_deploy_workflow(failures: list[str]) -> None:
    deploy_workflow = ROOT / ".github" / "workflows" / "deploy.yml"
    if not deploy_workflow.exists():
        return

    text = deploy_workflow.read_text(encoding="utf-8")
    if "workflow_dispatch:" not in text:
        failures.append(".github/workflows/deploy.yml is not manual-only")
    if "confirm_deploy:" not in text:
        failures.append(".github/workflows/deploy.yml must require typed deploy confirmation")
    if "ALLOW_CLOUD_DEPLOY" not in text:
        failures.append(".github/workflows/deploy.yml must require ALLOW_CLOUD_DEPLOY")
    if "github.ref_name" not in text or "main" not in text:
        failures.append(".github/workflows/deploy.yml must restrict production deploys to main")
    if "risk-pipeline-rendered.yaml" not in text:
        failures.append(".github/workflows/deploy.yml must apply the rendered deployment manifest")
    if "kubectl set image" in text and "--local" not in text:
        failures.append(".github/workflows/deploy.yml must not mutate live images after apply")

    on_block = text.split("jobs:", maxsplit=1)[0]
    if re.search(r"(?m)^\s+push:\s*$", on_block) or re.search(
        r"(?m)^\s+pull_request:\s*$", on_block
    ):
        failures.append(".github/workflows/deploy.yml must not run on push or pull_request")


def check_codeowners(failures: list[str]) -> None:
    codeowners = ROOT / ".github" / "CODEOWNERS"
    if not codeowners.exists():
        failures.append(".github/CODEOWNERS is required for sensitive path ownership")
        return

    text = codeowners.read_text(encoding="utf-8")
    for required_path in ("/.github/", "/deploy/", "/infra/", "/sql/"):
        if required_path not in text:
            failures.append(f".github/CODEOWNERS must cover {required_path}")


def check_local_compose_bindings(failures: list[str]) -> None:
    compose = ROOT / "docker-compose.yml"
    if not compose.exists():
        return

    text = compose.read_text(encoding="utf-8")
    for unsafe_binding in ('"5433:5432"', "'5433:5432'", '"27018:27017"', "'27018:27017'"):
        if unsafe_binding in text:
            failures.append("docker-compose.yml must bind database ports to 127.0.0.1")


def check_kubernetes_defaults(failures: list[str]) -> None:
    service_account = ROOT / "deploy" / "kubernetes" / "base" / "service-account.yaml"
    network_policy = ROOT / "deploy" / "kubernetes" / "base" / "network-policy.yaml"

    if service_account.exists():
        text = service_account.read_text(encoding="utf-8")
        if "automountServiceAccountToken: false" not in text:
            failures.append("base Kubernetes service account must not automount tokens")

    if network_policy.exists():
        text = network_policy.read_text(encoding="utf-8")
        if "Egress" not in text:
            failures.append("base Kubernetes network policy must include default-deny egress")


def check_kubernetes_config_copies(failures: list[str]) -> None:
    for source_path, deploy_path in KUBERNETES_CONFIG_COPIES:
        source = ROOT / source_path
        deploy_copy = ROOT / deploy_path
        if not source.exists():
            failures.append(f"Missing source config file: {source_path}")
            continue
        if not deploy_copy.exists():
            failures.append(f"Missing Kubernetes config copy: {deploy_path}")
            continue
        if source.read_text(encoding="utf-8") != deploy_copy.read_text(encoding="utf-8"):
            failures.append(f"Kubernetes config copy is out of sync: {deploy_path}")


def check_terraform_flags(failures: list[str]) -> None:
    variables = ROOT / "infra" / "terraform" / "variables.tf"
    if not variables.exists():
        return

    text = variables.read_text(encoding="utf-8")
    for flag in REQUIRED_FALSE_FLAGS:
        pattern = re.compile(
            rf'variable\s+"{re.escape(flag)}"\s+\{{(?P<body>.*?)\n\}}',
            re.DOTALL,
        )
        match = pattern.search(text)
        if not match:
            failures.append(f"Missing Terraform creation flag: {flag}")
            continue
        if not re.search(r"(?m)^\s*default\s*=\s*false\s*$", match.group("body")):
            failures.append(f"Terraform creation flag must default false: {flag}")


def main() -> int:
    failures: list[str] = []
    files = tracked_and_untracked_files()

    check_tracked_paths(failures)
    check_secret_patterns(files, failures)
    check_deploy_workflow(failures)
    check_codeowners(failures)
    check_local_compose_bindings(failures)
    check_kubernetes_defaults(failures)
    check_kubernetes_config_copies(failures)
    check_terraform_flags(failures)

    if failures:
        print("Security check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Security check passed:")
    print("- no generated or local-only paths are tracked")
    print("- no obvious secret patterns were found")
    print("- deploy workflow remains manual-only")
    print("- sensitive paths have CODEOWNERS coverage")
    print("- local database ports are bound to localhost")
    print("- base Kubernetes defaults avoid token mounting and deny egress")
    print("- Kubernetes deploy config copies match repo config")
    print("- optional managed database creation flags default to false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
