from contextlib import nullcontext
from pathlib import Path

import pytest
from typer.testing import CliRunner

import tldr_crypto_finance.cli as cli_module
from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.ingestion.gmail_ingest import _build_query
from tldr_crypto_finance.ingestion.imap_ingest import _matches_sender_filters
from tldr_crypto_finance.pipelines.sync import run_sync


def _test_settings(tmp_path: Path) -> Settings:
    """Build isolated settings for CLI and pipeline tests."""

    settings = Settings(
        database_path=tmp_path / "test.duckdb",
        raw_data_dir=tmp_path / "raw",
        curated_data_dir=tmp_path / "curated",
        config_dir=Path(__file__).resolve().parents[1] / "configs",
    )
    settings.ensure_directories()
    return settings


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


def test_run_sync_defaults_to_gmail(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = _test_settings(tmp_path)
    calls: list[str] = []

    def fake_sync_gmail(connection, configured_settings, *, sql_dir):
        calls.append("gmail")
        assert configured_settings is settings
        assert sql_dir == tmp_path
        return {"inserted": 1}

    monkeypatch.setattr("tldr_crypto_finance.pipelines.sync.sync_gmail", fake_sync_gmail)
    monkeypatch.setattr(
        "tldr_crypto_finance.pipelines.sync.sync_imap",
        lambda connection, configured_settings, *, sql_dir: {"inserted": 1},
    )

    results = run_sync(object(), settings, sql_dir=tmp_path)

    assert results == {"gmail": {"inserted": 1}}
    assert calls == ["gmail"]


def test_run_sync_requires_a_selected_source(tmp_path: Path) -> None:
    settings = _test_settings(tmp_path)

    with pytest.raises(RuntimeError, match="No sync sources selected"):
        run_sync(object(), settings, sql_dir=tmp_path, gmail=False, imap=False)


def test_run_sync_command_runs_gmail_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner = CliRunner()
    captured: dict[str, bool] = {}
    settings = _test_settings(tmp_path)

    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "configure_logging", lambda level: None)
    monkeypatch.setattr(cli_module, "connect", lambda configured_settings: nullcontext(object()))

    def fake_run_sync(connection, configured_settings, *, sql_dir, gmail, imap):
        captured["gmail"] = gmail
        captured["imap"] = imap
        assert configured_settings is settings
        return {"gmail": {"inserted": 2}}

    monkeypatch.setattr(cli_module, "run_sync", fake_run_sync)

    result = runner.invoke(cli_module.app, ["run-sync"])

    assert result.exit_code == 0
    assert captured == {"gmail": True, "imap": False}
    assert '"inserted": 2' in result.stdout
