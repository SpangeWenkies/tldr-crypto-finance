from tldr_crypto_finance.ingestion.gmail_ingest import _build_query
from tldr_crypto_finance.ingestion.imap_ingest import _matches_sender_filters


def test_build_query_adds_checkpoint_and_sender_filters() -> None:
    query = _build_query(
        "label:newsletters",
        ["risk@example.com", "macro@example.com"],
        "1700000000000",
    )
    assert "label:newsletters" in query
    assert "after:" in query
    assert "from:risk@example.com" in query


def test_matches_sender_filters_is_case_insensitive() -> None:
    assert (
        _matches_sender_filters("Risk Signals <ALERTS@example.com>", ["alerts@example.com"])
        is True
    )
    assert (
        _matches_sender_filters("Risk Signals <ALERTS@example.com>", ["macro@example.com"])
        is False
    )
