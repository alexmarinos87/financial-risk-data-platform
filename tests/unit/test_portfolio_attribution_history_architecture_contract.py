from pathlib import Path


def test_architecture_documents_rolling_attribution_history() -> None:
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")
    attribution = Path("docs/portfolio-attribution.md").read_text(
        encoding="utf-8"
    )
    makefile = Path("Makefile").read_text(encoding="utf-8")

    for required in (
        "rolling historical attribution",
        "portfolio-attribution-history-demo",
        "MAX_HISTORY_SNAPSHOTS = 2_500",
        "one snapshot per eligible rolling end date",
        "start date is supplied",
    ):
        assert required.lower() in (
            architecture + attribution + makefile
        ).lower()

    for stale in (
        "Attribution is a latest-window snapshot only",
        "historical snapshots for every event date are not calculated",
        "does not yet calculate historical attribution snapshots",
        "This path stores one latest-window snapshot",
    ):
        assert stale not in architecture
        assert stale not in attribution
