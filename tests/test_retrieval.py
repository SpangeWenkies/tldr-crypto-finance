from __future__ import annotations

from pathlib import Path

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.db.duckdb import connect
from tldr_crypto_finance.ingestion.eml_ingest import ingest_eml_directory
from tldr_crypto_finance.pipelines.enrich import label_articles, parse_issues
from tldr_crypto_finance.retrieval.briefs import risk_brief
from tldr_crypto_finance.retrieval.query import query_articles
from tldr_crypto_finance.retrieval.semantic import (
    build_article_embeddings,
    search_similar_articles,
)


def _sql_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "tldr_crypto_finance" / "db"


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        database_path=tmp_path / "test.duckdb",
        raw_data_dir=tmp_path / "raw",
        curated_data_dir=tmp_path / "curated",
        config_dir=Path(__file__).resolve().parents[1] / "configs",
        entity_extraction_backend="heuristic",
    )


def test_retrieval_end_to_end(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fixture_dir = Path(__file__).resolve().parent / "fixtures"

    with connect(settings) as connection:
        ingest_eml_directory(connection, fixture_dir, sql_dir=_sql_dir())
        parse_issues(connection, settings, sql_dir=_sql_dir())
        label_articles(connection, settings, sql_dir=_sql_dir(), mode="rules")
        build_article_embeddings(
            connection,
            backend="hash",
            model_name=settings.embeddings_model_name,
        )

        crypto_articles = query_articles(connection, topic="crypto_markets", limit=5)
        assert any(
            "stablecoin" in str(article["clean_summary_text"]).lower()
            for article in crypto_articles
        )

        similar = search_similar_articles(
            connection,
            "stablecoin liquidity reserve backing",
            limit=3,
        )
        assert similar
        assert "stablecoin" in str(similar[0]["clean_summary_text"]).lower()

        brief = risk_brief(connection, "stablecoin", topic="crypto_markets", limit=3)
        assert "Risk Brief: stablecoin" in brief
