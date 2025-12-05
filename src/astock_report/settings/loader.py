"""Settings helpers to centralize configuration access."""
from __future__ import annotations

from typing import Optional

from config import Config


def load_settings(debug_override: Optional[bool] = None) -> Config:
    """Return a Config instance, applying optional runtime overrides."""
    config = Config.from_env()
    if debug_override is not None:
        config.debug = debug_override
    return config