"""Thin wrapper around TuShare SDK with project defaults."""
from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import tushare as ts
from tushare.pro import client as ts_client


class TuShareClient:
    """Encapsulate TuShare client initialization and helper queries."""

    def __init__(self, api_key: Optional[str], *, max_retries: int = 3, throttle_seconds: float = 0.2) -> None:
        token = api_key or self._read_token_from_disk()
        if not token:
            raise ValueError("TuShare API key is missing; set TUSHARE_API_KEY or provide .tushare_token.")
        self._configure_base_url()
        self._configure_proxy()
        self._pro = ts.pro_api(token)
        self._max_retries = max_retries
        self._throttle_seconds = throttle_seconds

    # ------------------
    # Public API helpers
    # ------------------
    def fetch_financials(self, ticker: str, since: Optional[date] = None) -> Dict[str, Any]:
        """Retrieve financial statements required by downstream calculators."""
        query_kwargs: Dict[str, Any] = {"ts_code": ticker}
        if since is not None:
            query_kwargs["start_date"] = since.strftime("%Y%m%d")
        income = self._call_with_retry(self._pro.income, **query_kwargs)
        balance = self._call_with_retry(self._pro.balancesheet, **query_kwargs)
        cashflow = self._call_with_retry(self._pro.cashflow, **query_kwargs)
        return {"income": income, "balance": balance, "cashflow": cashflow}

    def fetch_prices(
        self,
        ticker: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 120,
    ):
        """Fetch a recent price window for the ticker via TuShare daily API."""
        query_kwargs: Dict[str, Any] = {"ts_code": ticker, "limit": limit}
        if start_date is not None:
            query_kwargs["start_date"] = start_date.strftime("%Y%m%d")
        if end_date is not None:
            query_kwargs["end_date"] = end_date.strftime("%Y%m%d")
        return self._call_with_retry(self._pro.daily, **query_kwargs)

    def fetch_basic_info(self, ticker: str):
        """Fetch static company metadata such as name, list date, and industry."""
        return self._call_with_retry(
            self._pro.stock_basic,
            ts_code=ticker,
            fields="ts_code,name,area,industry,list_date,market,exchange",
        )

    def fetch_fina_indicators(self, ticker: str, *, start_date: Optional[date] = None, end_date: Optional[date] = None):
        """Fetch financial indicator snapshots from TuShare (VIP if available)."""
        query_kwargs: Dict[str, Any] = {"ts_code": ticker}
        if start_date is not None:
            query_kwargs["start_date"] = start_date.strftime("%Y%m%d")
        if end_date is not None:
            query_kwargs["end_date"] = end_date.strftime("%Y%m%d")
        return self._call_with_retry(self._pro.fina_indicator_vip, **query_kwargs)

    def fetch_top10_holders(self, ticker: str, *, end_date: Optional[date] = None):
        """Fetch the latest top 10 holders snapshot."""
        query_kwargs: Dict[str, Any] = {"ts_code": ticker}
        if end_date is not None:
            query_kwargs["end_date"] = end_date.strftime("%Y%m%d")
        return self._call_with_retry(self._pro.top10_holders, **query_kwargs)

    def probe(self) -> bool:
        """Lightweight connectivity probe using trade calendar."""
        try:
            _ = self._call_with_retry(self._pro.trade_cal, limit=1)
            return True
        except Exception:
            return False

    # -----------------
    # Internal helpers
    # -----------------
    def _call_with_retry(self, func, **kwargs):
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                result = func(**kwargs)
                return result
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                if attempt >= self._max_retries:
                    break
                time.sleep(self._throttle_seconds * attempt)
        if last_exc:
            raise last_exc
        raise RuntimeError("TuShare call failed without exception")

    @staticmethod
    def _configure_base_url() -> None:
        """Align TuShare base URL with local config (per TUSHARE_CONFIG)."""
        base_url = os.getenv("TUSHARE_BASE_URL", "http://api.tushare.pro/dataapi")
        # NOTE: TuShare client stores base URL as a private attribute.
        ts_client.DataApi._DataApi__http_url = base_url  # type: ignore[attr-defined]

    @staticmethod
    def _configure_proxy() -> None:
        """Apply proxy settings for TuShare if provided."""
        proxy = os.getenv("TUSHARE_PROXY") or os.getenv("PROXY_URL")
        if proxy:
            os.environ["http_proxy"] = proxy
            os.environ["https_proxy"] = proxy

    @staticmethod
    def _read_token_from_disk() -> Optional[str]:
        """Try to load TuShare token from repo root or home directory."""
        base_dir = Path(__file__).resolve().parent
        candidates = [
            Path.cwd() / ".tushare_token",
            base_dir.parent.parent.parent.parent / ".tushare_token",
            base_dir.parent.parent.parent.parent.parent / ".tushare_token",
            Path.home() / ".tushare_token",
        ]
        for path in candidates:
            if path.exists() and path.is_file():
                try:
                    token = path.read_text(encoding="utf-8").strip()
                    if token:
                        return token
                except Exception:
                    continue
        return None
