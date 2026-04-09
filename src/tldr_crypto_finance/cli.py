"""Typer command line interface for the local-first pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from tldr_crypto_finance.analytics.export_parquet import export_curated_parquet
from tldr_crypto_finance.analytics.metrics import collect_metrics
from tldr_crypto_finance.config import get_settings
from tldr_crypto_finance.db.duckdb import connect, connect_read_only, init_database
from tldr_crypto_finance.ingestion.eml_ingest import ingest_eml_directory
from tldr_crypto_finance.ingestion.gmail_ingest import sync_gmail
from tldr_crypto_finance.ingestion.imap_ingest import sync_imap
from tldr_crypto_finance.ingestion.mbox_ingest import ingest_mbox_file
from tldr_crypto_finance.logging_utils import configure_logging
from tldr_crypto_finance.pipelines.backfill import run_backfill
from tldr_crypto_finance.pipelines.enrich import (
    label_articles as label_articles_pipeline,
)
from tldr_crypto_finance.pipelines.enrich import parse_issues as parse_issues_pipeline
from tldr_crypto_finance.pipelines.full_refresh import run_full_refresh
from tldr_crypto_finance.pipelines.sync import run_sync
from tldr_crypto_finance.retrieval.briefs import risk_brief
from tldr_crypto_finance.retrieval.query import context_bundle, query_articles
from tldr_crypto_finance.retrieval.semantic import (
    build_article_embeddings,
    search_similar_articles,
)

app = typer.Typer(help="Local-first finance and newsletter intelligence pipeline.")


@app.callback()
def main() -> None:
    """Initialize process logging before executing commands."""

    configure_logging(get_settings().log_level)


@app.command("init-db")
def init_db() -> None:
    """Create the DuckDB schema and curated views."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        init_database(connection, sql_dir)
    typer.echo(f"Initialized database at {settings.database_path}")


@app.command()
def metrics() -> None:
    """Print core operational metrics as formatted JSON."""

    settings = get_settings()
    with connect_read_only(settings) as connection:
        payload = collect_metrics(connection)
    typer.echo(json.dumps(payload, indent=2, default=str))


@app.command("export-parquet")
def export_parquet() -> None:
    """Export curated views and tables to local Parquet files."""

    settings = get_settings()
    with connect_read_only(settings) as connection:
        exported = export_curated_parquet(connection, settings.curated_data_dir)
    typer.echo(json.dumps([str(path) for path in exported], indent=2))


@app.command("ingest-eml")
def ingest_eml(path: Path) -> None:
    """Recursively ingest EML files from a directory."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = ingest_eml_directory(connection, path, sql_dir=sql_dir)
    typer.echo(json.dumps(stats, indent=2))


@app.command("ingest-mbox")
def ingest_mbox(path: Path) -> None:
    """Ingest a historical MBOX mailbox export."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = ingest_mbox_file(connection, path, sql_dir=sql_dir)
    typer.echo(json.dumps(stats, indent=2))


@app.command("parse-issues")
def parse_issues(
    force: bool = typer.Option(False, help="Reparse issues even if they already exist."),
    limit: int | None = typer.Option(None, help="Optional limit on raw messages to parse."),
) -> None:
    """Parse ingested raw messages into issues, sections, articles, and links."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = parse_issues_pipeline(
            connection,
            settings,
            sql_dir=sql_dir,
            force=force,
            limit=limit,
        )
    typer.echo(json.dumps(stats, indent=2))


@app.command("label-articles")
def label_articles(
    mode: str = typer.Option("rules", help="rules, hybrid, or zero-shot"),
    force: bool = typer.Option(False, help="Relabel articles that already have labels."),
    limit: int | None = typer.Option(None, help="Optional limit on articles to label."),
) -> None:
    """Label parsed article blocks and populate entity and review tables."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = label_articles_pipeline(
            connection,
            settings,
            sql_dir=sql_dir,
            mode=mode,
            force=force,
            limit=limit,
        )
    typer.echo(json.dumps(stats, indent=2))


@app.command("build-embeddings")
def build_embeddings(
    backend: str = typer.Option("hash", help="hash or sentence-transformer"),
    force: bool = typer.Option(False, help="Rebuild embeddings for existing articles."),
    limit: int | None = typer.Option(None, help="Optional limit on embedded articles."),
) -> None:
    """Build embeddings for analysis-ready articles."""

    settings = get_settings()
    with connect(settings) as connection:
        stats = build_article_embeddings(
            connection,
            backend=backend,
            model_name=settings.embeddings_model_name,
            force=force,
            limit=limit,
        )
    typer.echo(json.dumps(stats, indent=2))


@app.command("query-articles")
def query_articles_cmd(
    days: int | None = typer.Option(
        None,
        help="Only include recent articles from the last N days.",
    ),
    topic: str | None = typer.Option(None, help="Filter by topic."),
    sender: str | None = typer.Option(None, help="Filter by sender email fragment."),
    domain: str | None = typer.Option(None, help="Filter by primary domain."),
    asset_class: str | None = typer.Option(None, help="Filter by asset class."),
    risk_type: str | None = typer.Option(None, help="Filter by risk type."),
    limit: int = typer.Option(20, help="Maximum number of records to return."),
    output: str = typer.Option("json", help="json or markdown"),
) -> None:
    """Run structured SQL retrieval over the labeled article store."""

    settings = get_settings()
    with connect_read_only(settings) as connection:
        records = query_articles(
            connection,
            days=days,
            topic=topic,
            sender=sender,
            domain=domain,
            asset_class=asset_class,
            risk_type=risk_type,
            limit=limit,
        )
    typer.echo(context_bundle(records, output))


@app.command("search-similar")
def search_similar(
    query_text: str,
    topic: str | None = typer.Option(None, help="Optional topic filter before ranking."),
    days: int | None = typer.Option(None, help="Optional recent-day filter before ranking."),
    limit: int = typer.Option(10, help="Maximum number of records to return."),
    output: str = typer.Option("json", help="json or markdown"),
) -> None:
    """Search for similar articles using embeddings or lexical fallback."""

    settings = get_settings()
    with connect_read_only(settings) as connection:
        records = search_similar_articles(
            connection,
            query_text,
            backend="hash",
            model_name=settings.embeddings_model_name,
            days=days,
            topic=topic,
            limit=limit,
        )
    typer.echo(context_bundle(records, output))


@app.command("risk-brief")
def risk_brief_cmd(
    subject: str,
    days: int = typer.Option(14, help="Only include recent articles from the last N days."),
    topic: str | None = typer.Option(None, help="Optional topic filter."),
    limit: int = typer.Option(5, help="Maximum number of articles to summarize."),
) -> None:
    """Produce a short markdown brief for a risk or market topic."""

    settings = get_settings()
    with connect_read_only(settings) as connection:
        brief = risk_brief(connection, subject, days=days, topic=topic, limit=limit)
    typer.echo(brief)


@app.command("export-context")
def export_context(
    days: int | None = typer.Option(
        None,
        help="Only include recent articles from the last N days.",
    ),
    topic: str | None = typer.Option(None, help="Filter by topic."),
    limit: int = typer.Option(10, help="Maximum number of records to export."),
    output: str = typer.Option("json", help="json or markdown"),
    output_path: Path | None = typer.Option(
        None,
        help="Optional path to write the context bundle.",
    ),
) -> None:
    """Export a compact article context bundle for downstream agent use."""

    settings = get_settings()
    with connect_read_only(settings) as connection:
        records = query_articles(connection, days=days, topic=topic, limit=limit)
    bundle = context_bundle(records, output)
    if output_path is not None:
        output_path.write_text(bundle, encoding="utf-8")
    typer.echo(bundle)


@app.command("sync-gmail")
def sync_gmail_cmd(
    max_results: int = typer.Option(100, help="Maximum number of Gmail messages to request."),
) -> None:
    """Synchronize new messages from Gmail using the configured OAuth credentials."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = sync_gmail(connection, settings, sql_dir=sql_dir, max_results=max_results)
    typer.echo(json.dumps(stats, indent=2))


@app.command("sync-imap")
def sync_imap_cmd(
    folder: str | None = typer.Option(None, help="Mailbox folder to sync, defaults to env config."),
    max_messages: int = typer.Option(100, help="Maximum number of IMAP messages to fetch."),
) -> None:
    """Synchronize new messages from a generic IMAP mailbox."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = sync_imap(
            connection,
            settings,
            sql_dir=sql_dir,
            folder=folder,
            max_messages=max_messages,
        )
    typer.echo(json.dumps(stats, indent=2))


@app.command("run-sync")
def run_sync_cmd(
    gmail: bool = typer.Option(True, "--gmail/--no-gmail", help="Run Gmail sync."),
    imap: bool = typer.Option(False, "--imap/--no-imap", help="Run IMAP sync."),
) -> None:
    """Run live sync sources in a single command. Gmail runs by default."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = run_sync(connection, settings, sql_dir=sql_dir, gmail=gmail, imap=imap)
    typer.echo(json.dumps(stats, indent=2))


@app.command("run-backfill")
def run_backfill_cmd(
    path: Path,
    source: str = typer.Option(..., help="eml or mbox"),
    label_mode: str = typer.Option("rules", help="rules, hybrid, or zero-shot"),
    skip_embeddings: bool = typer.Option(False, help="Skip hash embedding generation."),
    skip_parquet: bool = typer.Option(False, help="Skip curated Parquet export."),
) -> None:
    """Run a historical backfill from local email files through the full pipeline."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = run_backfill(
            connection,
            settings,
            sql_dir=sql_dir,
            source_path=path,
            source_type=source,
            label_mode=label_mode,
            build_embeddings=not skip_embeddings,
            export_parquet=not skip_parquet,
        )
    typer.echo(json.dumps(stats, indent=2))


@app.command("full-refresh")
def full_refresh_cmd(
    label_mode: str = typer.Option("rules", help="rules, hybrid, or zero-shot"),
    force_parse: bool = typer.Option(False, help="Reparse existing issues."),
    force_label: bool = typer.Option(False, help="Relabel existing articles."),
    skip_embeddings: bool = typer.Option(False, help="Skip hash embedding generation."),
    skip_parquet: bool = typer.Option(False, help="Skip curated Parquet export."),
) -> None:
    """Rebuild downstream derived data from already ingested raw messages."""

    settings = get_settings()
    sql_dir = Path(__file__).resolve().parent / "db"
    with connect(settings) as connection:
        stats = run_full_refresh(
            connection,
            settings,
            sql_dir=sql_dir,
            label_mode=label_mode,
            force_parse=force_parse,
            force_label=force_label,
            build_embeddings=not skip_embeddings,
            export_parquet=not skip_parquet,
        )
    typer.echo(json.dumps(stats, indent=2))


if __name__ == "__main__":
    app()
