"""Validate retained notification records offline; never record or execute them."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, NoReturn

from src.common.bounded_json import MAX_JSON_BYTES, load_bounded_json_object
from src.common.exceptions import ValidationError

MODEL_VERSION = "notification-evidence-validation-v1"
RECORD_KINDS = ("readiness", "receiver", "transition")
MAX_REPORT_BYTES = 4096
SAFE_RECORD_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,511}")
SHA256 = re.compile(r"[0-9a-f]{64}")
Validator = Callable[[Mapping[str, Any]], dict[str, Any]]
Canonicalizer = Callable[[Mapping[str, Any]], bytes]


def _contract(kind: str) -> tuple[Validator, Canonicalizer]:
    # Explicit selection only. Import semantic contracts, never recorder CLIs.
    if kind == "readiness":
        from src.warehouse.notification_execution_readiness_history_contract import (
            canonical_notification_execution_readiness_record_bytes,
            validate_notification_execution_readiness_record,
        )

        return (validate_notification_execution_readiness_record,
                canonical_notification_execution_readiness_record_bytes)
    if kind == "receiver":
        from src.warehouse.controlled_receiver_rehearsal_contract import (
            canonical_controlled_receiver_rehearsal_bytes,
            validate_controlled_receiver_rehearsal_record,
        )

        return (validate_controlled_receiver_rehearsal_record,
                canonical_controlled_receiver_rehearsal_bytes)
    if kind == "transition":
        from src.warehouse.notification_destination_transition_rehearsal_contract import (
            canonical_transition_rehearsal_record_bytes,
            validate_notification_destination_transition_rehearsal_record,
        )

        return (validate_notification_destination_transition_rehearsal_record,
                canonical_transition_rehearsal_record_bytes)
    raise ValidationError("unsupported notification evidence kind")


def _base_report(status: str) -> dict[str, Any]:
    return {
        "model_version": MODEL_VERSION, "status": status,
        "validation_scope": "retained_record_only",
        "database_access_performed": False, "external_request_performed": False,
        "source_authenticated": False, "current_state_verified": False,
        "runtime_permission_granted": False,
    }


def validate_notification_evidence_file(
    *, record_kind: str, path: Path, expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Check an existing record contract and optionally its canonical digest.

    A valid retained block/rejection record is a successful validation result.
    No current-state observation, source authentication or execution permission
    follows from this result. Source files and parent directories stay trusted.
    """
    if not isinstance(record_kind, str) or record_kind not in RECORD_KINDS:
        raise ValidationError("unsupported notification evidence kind")
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str) or not SHA256.fullmatch(expected_sha256)
    ):
        raise ValidationError("expected evidence digest must be lowercase SHA-256")
    value = load_bounded_json_object(path)
    validator, canonicalizer = _contract(record_kind)
    try:
        validated = validator(value)
        canonical = canonicalizer(validated)
    except Exception:
        # Existing validators may mention unknown field names. Do not expose
        # caller-controlled values through this inspection interface.
        raise ValidationError("notification evidence failed semantic validation") from None
    if not isinstance(canonical, bytes) or not 1 <= len(canonical) <= MAX_JSON_BYTES:
        raise ValidationError("canonical notification evidence exceeds its size contract")
    record_id = validated.get("record_id")
    if not isinstance(record_id, str) or not SAFE_RECORD_ID.fullmatch(record_id):
        raise ValidationError("validated notification record identity is invalid")
    digest = hashlib.sha256(canonical).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValidationError("notification evidence digest does not match")
    return {
        **_base_report("valid"), "record_kind": record_kind, "record_id": record_id,
        "document_sha256": digest, "canonical_bytes": len(canonical),
        "expected_digest_checked": expected_sha256 is not None,
    }


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(2, "Invalid evidence validation arguments; use --help.\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        description="Validate retained notification evidence without recording or execution.",
        allow_abbrev=False,
    )
    parser.add_argument("--kind", choices=RECORD_KINDS, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--expected-sha256")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        report = validate_notification_evidence_file(
            record_kind=args.kind, path=args.input, expected_sha256=args.expected_sha256,
        )
        code = 0
    except ValidationError:
        report = {**_base_report("rejected"), "error_code": "invalid_evidence"}
        code = 1
    except Exception:
        report = {**_base_report("error"), "error_code": "validation_failed"}
        code = 1
    try:
        rendered = json.dumps(report, sort_keys=True, allow_nan=False)
        if len(rendered.encode("utf-8")) + 1 > MAX_REPORT_BYTES:
            raise ValidationError("validation report exceeds its size contract")
        print(rendered)
    except (OSError, TypeError, ValueError, ValidationError):
        return 1
    return code


if __name__ == "__main__":
    raise SystemExit(main())
