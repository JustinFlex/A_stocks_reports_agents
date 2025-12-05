"""Application-wide configuration defaults and helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Base directory for resolving relative paths.
BASE_DIR = Path(__file__).resolve().parent


def _to_bool(value: Optional[str], default: bool = False) -> bool:
    """Parse truthy environment values like '1' or 'true'."""
    if value is None:
        return default
    # Normalize non-string inputs (e.g., int defaults) before parsing.
    if not isinstance(value, str):
        value = str(value)
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _to_int(value: Optional[str]) -> Optional[int]:
    """Safely parse an integer env var, returning None on failure."""
    if value is None:
        return None
    try:
        parsed = int(str(value).strip())
        return parsed
    except (TypeError, ValueError):
        return None


@dataclass
class Config:
    """Runtime configuration loaded from environment variables."""

    debug: bool = False
    database_path: Path = BASE_DIR / "data" / "financials.db"
    sqlite_echo: bool = False
    tushare_api_key: Optional[str] = None
    poe_api_key: Optional[str] = None
    proxy_url: Optional[str] = None
    gemini_model: str = "gemini-2.5-pro"
    poe_web_search: Optional[bool] = None
    poe_thinking_budget: Optional[int] = None
    langgraph_checkpoint_dir: Path = BASE_DIR / "run" / "checkpoints"
    output_dir: Path = BASE_DIR / "reports"

    @classmethod
    def from_env(cls) -> "Config":
        """Build a configuration instance using environment overrides."""
        base = BASE_DIR
        db_path = Path(r'D:\TushareData\fina_indicator_complete_data.db')
        checkpoint_dir = Path(
            os.getenv("LANGGRAPH_CHECKPOINT_DIR", base / "run" / "checkpoints")
        )
        output_dir = Path(os.getenv("OUTPUT_DIR", base / "reports"))

        config = cls(
            debug=_to_bool(os.getenv("APP_DEBUG",1)),
            database_path=db_path,
            sqlite_echo=_to_bool(os.getenv("SQLITE_ECHO",1)),
            tushare_api_key=os.getenv("TUSHARE_API_KEY"),
            poe_api_key=os.getenv("POE_API_KEY"),
            proxy_url=os.getenv("PROXY_URL"),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-pro"),
            poe_web_search=_to_bool(os.getenv("POE_WEB_SEARCH"), default=False)
            if os.getenv("POE_WEB_SEARCH") is not None
            else None,
            poe_thinking_budget=_to_int(os.getenv("POE_THINKING_BUDGET")),
            langgraph_checkpoint_dir=checkpoint_dir,
            output_dir=output_dir,
        )
        config.ensure_directories()
        return config

    def ensure_directories(self) -> None:
        """Create directories needed for runtime artifacts."""
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.langgraph_checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)


active_config = Config.from_env()
