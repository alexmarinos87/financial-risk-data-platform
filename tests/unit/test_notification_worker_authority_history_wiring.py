from pathlib import Path


def test_worker_authority_database_proof_is_invoked_by_standard_ci_target() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")
    recipe = makefile.split("postgres-contract-check:\n", 1)[1].split("\nlocal-db-up:", 1)[0]
    assert "-m src.warehouse.notification_worker_authority_postgres_contract_check" in recipe
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "28_notification_worker_authority_schema.sql:ro" in compose
    schema = Path("sql/notification_worker_authority_schema.sql").read_text(encoding="utf-8")
    assert "statement_timestamp() >= expires_at" in schema
    assert "BEFORE TRUNCATE" in schema
