"""Simple smoke test for TuShare and Poe/Gemini wiring.

Run locally after installing dependencies and setting environment variables:

  python tools/api_smoke_test.py
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path
import sys
from typing import Optional

import httpx
import openai
import tushare as ts
from tushare.pro import client as ts_client

# Ensure repository root is on sys.path so config and package imports work when run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import Config  # noqa: E402


def configure_tushare_base_url() -> str:
    """Override TuShare base URL if provided (per TUSHARE_CONFIG guidance)."""
    base_url = os.getenv("TUSHARE_BASE_URL", "http://api.tushare.pro/dataapi")
    ts_client.DataApi._DataApi__http_url = base_url  # type: ignore[attr-defined]
    proxy = os.getenv("TUSHARE_PROXY")
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
    return base_url


def tushare_smoke(cfg: Config) -> None:
    if not cfg.tushare_api_key:
        raise ValueError("TUSHARE_API_KEY missing")
    base_url = configure_tushare_base_url()
    pro = ts.pro_api(cfg.tushare_api_key)
    # 股票数据：日线行情示例
    daily = pro.daily(ts_code="600000.SH", start_date="20240102", end_date="20240112")
    print("TUSHARE_BASE_URL:", base_url)
    print("TUSHARE_DAILY_SHAPE:", daily.shape)
    # 额外：简单财务数据
    income = pro.income(ts_code="600000.SH", start_date="20240101", end_date="20241231")
    print("TUSHARE_INCOME_SHAPE:", income.shape)


def poe_smoke(cfg: Config) -> None:
    if not cfg.poe_api_key:
        raise ValueError("POE_API_KEY missing")
    proxy_url: Optional[str] = cfg.proxy_url
    http_client = httpx.Client(
        proxy=proxy_url,
        timeout=httpx.Timeout(30.0, connect=10.0),
        verify=False,
    )
    client = openai.OpenAI(
        api_key=cfg.poe_api_key,
        base_url="https://api.poe.com/v1",
        http_client=http_client,
    )
    resp = client.chat.completions.create(
        model=os.getenv("GEMINI_MODEL", cfg.gemini_model) or "Gemini-2.5-Pro",
        messages=[{"role": "user", "content": "请只输出：OK"}],
        temperature=0.0,
        extra_body={
            "web_search": cfg.poe_web_search if cfg.poe_web_search is not None else True,
            "thinking_budget": cfg.poe_thinking_budget if cfg.poe_thinking_budget is not None else 512,
        },
    )
    print("GEMINI_SMOKE_OK:", True, "RESPONSE:", resp.choices[0].message.content)


def main() -> None:
    cfg = Config.from_env()
    print("TUSHARE_API_KEY_PRESENT:", bool(cfg.tushare_api_key))
    print("POE_API_KEY_PRESENT:", bool(cfg.poe_api_key))
    print("PROXY_URL_SET:", bool(cfg.proxy_url))

    try:
        tushare_smoke(cfg)
    except Exception as exc:  # noqa: BLE001
        print("TUSHARE_SMOKE_OK:", False, "ERROR:", repr(exc))

    try:
        poe_smoke(cfg)
    except Exception as exc:  # noqa: BLE001
        print("GEMINI_SMOKE_OK:", False, "ERROR:", repr(exc))


if __name__ == "__main__":
    main()
