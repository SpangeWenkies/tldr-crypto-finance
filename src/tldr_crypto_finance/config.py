"""Runtime configuration helpers."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="TLDR_CRYPTO_FINANCE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_path: Path = Field(
        default_factory=lambda: _project_root() / "data" / "tldr_crypto_finance.duckdb"
    )
    raw_data_dir: Path = Field(default_factory=lambda: _project_root() / "data" / "raw")
    curated_data_dir: Path = Field(default_factory=lambda: _project_root() / "data" / "curated")
    config_dir: Path = Field(default_factory=lambda: _project_root() / "configs")
    log_level: str = "INFO"

    gmail_credentials_path: Path = Field(
        default_factory=lambda: _project_root() / "secrets" / "gmail_credentials.json"
    )
    gmail_token_path: Path = Field(
        default_factory=lambda: _project_root() / "secrets" / "gmail_token.json"
    )
    gmail_query_filter: str = "after:2024/05/20"

    imap_host: str = ""
    imap_port: int = 993
    imap_username: str = ""
    imap_password: str = ""
    imap_folder: str = "INBOX"

    default_sender_filters: Annotated[list[str], NoDecode] = Field(default_factory=list)
    zero_shot_model_name: str = "facebook/bart-large-mnli"
    embeddings_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    @field_validator("default_sender_filters", mode="before")
    @classmethod
    def _split_sender_filters(cls, value: Any) -> list[str]:
        """Normalize comma-delimited sender filters from the environment."""

        if value in (None, ""):
            return []
        if isinstance(value, list):
            return [item.strip() for item in value if item.strip()]
        return [item.strip() for item in str(value).split(",") if item.strip()]

    def ensure_directories(self) -> None:
        """Create core local directories used by the pipeline."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.curated_data_dir.mkdir(parents=True, exist_ok=True)

    def config_path(self, name: str) -> Path:
        """Return the absolute path to a YAML config file."""

        return self.config_dir / name


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance for the current process."""

    settings = Settings()
    settings.ensure_directories()
    return settings


def load_yaml_config(path: Path) -> dict[str, Any]:
    """Load a YAML file and return an empty dictionary when it is blank."""

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        msg = f"Expected mapping in config file: {path}"
        raise ValueError(msg)
    return payload
