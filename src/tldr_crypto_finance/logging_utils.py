"""Logging setup for CLI entrypoints."""

from __future__ import annotations

import logging


def configure_logging(level: str) -> None:
    """Configure process-wide logging with a compact, readable format."""

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
