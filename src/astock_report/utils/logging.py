"""Logging helpers for CLI and workflow diagnostics."""
from __future__ import annotations

import logging
from typing import Optional

from rich.logging import RichHandler

_LOGGER_CONFIGURED = False


def configure_logging(debug: bool = False, *, level: Optional[int] = None) -> None:
    """Configure process-wide logging with Rich handler."""
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return

    resolved_level = level or (logging.DEBUG if debug else logging.INFO)
    logging.basicConfig(
        level=resolved_level,
        format="%(message)s",
        datefmt="%H:%M:%S",
        handlers=[RichHandler()],
    )
    _LOGGER_CONFIGURED = True