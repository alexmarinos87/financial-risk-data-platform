from pathlib import Path


def test_destination_transition_plan_is_delivery_free_and_authority_bound() -> None:
    source = Path(
        "src/orchestration/notification_destination_transition_plan.py"
    ).read_text(encoding="utf-8")
    docs = Path(
        "docs/notification-destination-transition-plan.md"
    ).read_text(encoding="utf-8")
    normalized_docs = " ".join(docs.split()).casefold()

    for operation in ("rotate", "disable", "rollback"):
        assert f'"{operation}"' in source
        assert operation in normalized_docs

    for required in (
        "current_authority_accepted_by_target",
        "target_authority_required",
        "validate_target_destination_authority",
        "transition may change only endpoint identity and activation evidence",
        "rollback requires prior_plan_id",
        "endpoint_value_recorded",
        "external_request_performed",
    ):
        assert required in source

    for required in (
        "primary arc42 block: `orchestration`",
        "current authority cannot authorise the target",
        "fresh rollback review changes the target fingerprint",
        "has no `--execute` option",
        "performs no dns lookup",
        "terraform apply",
    ):
        assert required in normalized_docs

    for forbidden in (
        "requests.post(",
        "urllib.request",
        "httpx.",
        "socket.",
    ):
        assert forbidden not in source.casefold()
