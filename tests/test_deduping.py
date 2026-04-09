from __future__ import annotations

import mailbox
from email import policy
from email.parser import BytesParser
from pathlib import Path

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.db.duckdb import connect, init_database
from tldr_crypto_finance.ingestion.common import insert_raw_message, parse_message_record
from tldr_crypto_finance.ingestion.eml_ingest import ingest_eml_directory
from tldr_crypto_finance.ingestion.mbox_ingest import ingest_mbox_file


def _sql_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "tldr_crypto_finance" / "db"


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        database_path=tmp_path / "test.duckdb",
        raw_data_dir=tmp_path / "raw",
        curated_data_dir=tmp_path / "curated",
        config_dir=Path(__file__).resolve().parents[1] / "configs",
    )


def test_message_id_deduping(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "sample_newsletter_1.eml"
    with fixture_path.open("rb") as handle:
        message = BytesParser(policy=policy.default).parse(handle)

    with connect(settings) as connection:
        init_database(connection, _sql_dir())
        record_one = parse_message_record(
            message,
            source_system="eml",
            source_mailbox="fixtures",
            raw_path=str(fixture_path),
            run_id="run-1",
        )
        record_two = parse_message_record(
            message,
            source_system="eml",
            source_mailbox="fixtures",
            raw_path=str(fixture_path),
            run_id="run-2",
        )
        assert insert_raw_message(connection, record_one)[0] is True
        assert insert_raw_message(connection, record_two)[0] is False


def test_body_hash_fallback_deduping(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "sample_newsletter_2.eml"
    with fixture_path.open("rb") as handle:
        message = BytesParser(policy=policy.default).parse(handle)
    del message["Message-ID"]

    with connect(settings) as connection:
        init_database(connection, _sql_dir())
        record_one = parse_message_record(
            message,
            source_system="eml",
            source_mailbox="fixtures",
            raw_path="first",
            run_id="run-1",
        )
        record_two = parse_message_record(
            message,
            source_system="eml",
            source_mailbox="fixtures",
            raw_path="second",
            run_id="run-2",
        )
        assert insert_raw_message(connection, record_one)[0] is True
        assert insert_raw_message(connection, record_two)[0] is False


def test_ingest_eml_directory_handles_malformed_message(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source_dir = tmp_path / "eml"
    source_dir.mkdir()
    fixture_dir = Path(__file__).resolve().parent / "fixtures"
    for name in ["sample_newsletter_1.eml", "sample_newsletter_2.eml", "malformed_newsletter.eml"]:
        target = source_dir / name
        target.write_bytes((fixture_dir / name).read_bytes())

    with connect(settings) as connection:
        stats = ingest_eml_directory(connection, source_dir, sql_dir=_sql_dir())
        assert stats["files_seen"] == 3
        assert stats["inserted"] == 3
        assert stats["errors"] == 0


def test_ingest_mbox_file(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "sample_newsletter_1.eml"
    with fixture_path.open("rb") as handle:
        message = BytesParser(policy=policy.default).parse(handle)

    mbox_path = tmp_path / "sample.mbox"
    mbox = mailbox.mbox(mbox_path)
    mbox.add(message)
    mbox.flush()
    mbox.close()

    with connect(settings) as connection:
        stats = ingest_mbox_file(connection, mbox_path, sql_dir=_sql_dir())
        assert stats["messages_seen"] == 1
        assert stats["inserted"] == 1
        assert stats["duplicates"] == 0
