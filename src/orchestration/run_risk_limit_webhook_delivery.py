from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..common.exceptions import StorageError, ValidationError
from ..delivery.risk_limit_webhook import (
    WebhookDeliveryConfig,
    deliver_risk_limit_notifications,
    load_webhook_delivery_config,
)

DEFAULT_POSTGRES_DSN = (
    "postgresql://risk_user:risk_password@localhost:5433/risk_platform"
)

NotificationReader = Callable[..., list[dict[str, Any]]]
ConfigLoader = Callable[[Path, str], WebhookDeliveryConfig]


def collect_pending_notifications(
    *,
    dsn: str,
    limit: int,
) -> list[dict[str, Any]]:
    if type(limit) is not int or limit <= 0:
        raise ValidationError("notification limit must be a positive integer")
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL notification delivery requires psycopg. Run `make setup`."
        ) from exc

    query = """
        SELECT to_jsonb(pending) AS notification
        FROM risk_platform.pending_portfolio_risk_limit_notifications pending
        ORDER BY notification_id
        LIMIT %s
    """
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()
    except Exception:
        raise StorageError(
            "Unable to read pending risk-limit notifications from PostgreSQL"
        ) from None

    notifications: list[dict[str, Any]] = []
    for row in rows:
        value = row[0]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                raise StorageError(
                    "Pending notification payload from PostgreSQL is invalid"
                ) from None
        if not isinstance(value, Mapping):
            raise StorageError(
                "Pending notification payload from PostgreSQL is invalid"
            )
        notifications.append(dict(value))
    return notifications


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Explicitly deliver current pending risk-limit notification intent "
            "through a disabled-by-default webhook adapter."
        )
    )
    parser.add_argument("--adapter-id", required=True)
    parser.add_argument(
        "--delivery-config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "WAREHOUSE_POSTGRES_DSN",
            DEFAULT_POSTGRES_DSN,
        ),
    )
    parser.add_argument(
        "--attempts-dir",
        type=Path,
        default=Path(".delivery/attempts"),
    )
    parser.add_argument("--enable-external-delivery", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _environment_value(
    environment: Mapping[str, str],
    name: str,
    *,
    required: bool,
) -> str | None:
    value = environment.get(name)
    if value is None or not value.strip():
        if required:
            raise ValidationError(
                f"required delivery environment variable '{name}' is not set"
            )
        return None
    return value.strip()


def run_risk_limit_webhook_delivery(
    *,
    adapter_id: str,
    delivery_config_path: Path,
    dsn: str,
    attempts_dir: Path,
    enable_external_delivery: bool = False,
    environment: Mapping[str, str] | None = None,
    config_loader: ConfigLoader | None = None,
    notification_reader: NotificationReader | None = None,
    deliverer: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected_config_loader = config_loader or load_webhook_delivery_config
    try:
        config = selected_config_loader(delivery_config_path, adapter_id)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("delivery configuration is invalid") from None

    base = {
        "run_id": str(uuid4()),
        "adapter_id": config.adapter_id,
        "adapter_enabled": config.enabled,
        "explicit_enable_flag": enable_external_delivery,
        "endpoint_environment_variable": config.endpoint_env,
        "authorization_environment_variable": config.authorization_env,
        "secrets_recorded": False,
    }
    if not enable_external_delivery:
        return {
            **base,
            "delivery": {
                "performed": False,
                "reason": "explicit_enable_flag_required",
            },
        }
    if not config.enabled:
        return {
            **base,
            "delivery": {
                "performed": False,
                "reason": "adapter_disabled_in_configuration",
            },
        }

    selected_environment = environment if environment is not None else os.environ
    endpoint = _environment_value(
        selected_environment,
        config.endpoint_env,
        required=True,
    )
    authorization = (
        _environment_value(
            selected_environment,
            config.authorization_env,
            required=True,
        )
        if config.authorization_env is not None
        else None
    )
    if endpoint is None:
        raise ValidationError("webhook endpoint is missing")

    selected_reader = notification_reader or collect_pending_notifications
    notifications = selected_reader(
        dsn=dsn,
        limit=config.max_notifications_per_run,
    )
    if not notifications:
        return {
            **base,
            "delivery": {
                "performed": False,
                "reason": "nothing_pending",
                "notifications_selected": 0,
            },
        }

    selected_deliverer = deliverer or deliver_risk_limit_notifications
    result = selected_deliverer(
        notifications,
        config=config,
        attempts_dir=attempts_dir,
        endpoint=endpoint,
        authorization=authorization,
    )
    if not isinstance(result, dict):
        raise StorageError("webhook deliverer returned invalid evidence")
    return {
        **base,
        "delivery": result,
    }


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("Unable to write webhook delivery summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_risk_limit_webhook_delivery(
            adapter_id=args.adapter_id,
            delivery_config_path=args.delivery_config,
            dsn=args.dsn,
            attempts_dir=args.attempts_dir,
            enable_external_delivery=args.enable_external_delivery,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Webhook delivery failed: explicit configuration, environment, or "
            "notification input was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Webhook delivery failed: pending-notification read or attempt "
            "persistence failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Webhook delivery failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
