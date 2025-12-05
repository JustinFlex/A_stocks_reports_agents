#!/usr/bin/env python3
"""
Minimal Tushare API sanity check.
Reads token from env TUSHARE_API_KEY and fetches a small trade calendar slice.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
import argparse
import requests

import tushare as ts


def configure_proxies(disable: bool = False) -> None:
    """
    Proxy handling:
    - If disable or TUSHARE_NO_PROXY is set, clear proxies.
    - Else if TUSHARE_PROXY is set, force that as http/https proxy.
    - Otherwise keep existing environment proxies.
    """
    if disable or os.getenv("TUSHARE_NO_PROXY"):
        for key in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"]:
            os.environ.pop(key, None)
        return

    proxy = os.getenv("TUSHARE_PROXY")
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["http_proxy"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
        os.environ["https_proxy"] = proxy


def load_token() -> str | None:
    env_token = os.getenv("TUSHARE_API_KEY")
    if env_token:
        return env_token.strip()

    # Fallback to local token file (first line). Checked in repo root then home.
    for candidate in [Path(".tushare_token"), Path.home() / ".tushare_token"]:
        if candidate.is_file():
            content = candidate.read_text(encoding="utf-8").strip()
            if content:
                return content
    return None


def build_pro_client(token: str, base_url: str | None, disable_proxy: bool):
    """Create a Tushare client with optional base URL override and proxy disable."""
    configure_proxies(disable=disable_proxy)
    pro = ts.pro_api(token)
    if base_url:
        pro._DataApi__http_url = base_url.rstrip("/")  # type: ignore[attr-defined]
    return pro


def probe_endpoint(url: str, proxies: dict[str, str] | None, timeout: int = 8) -> str:
    """Lightweight POST probe to check network/TLS reachability."""
    payload = {"ping": "pong"}
    try:
        resp = requests.post(url, json=payload, timeout=timeout, proxies=proxies)
        return f"status={resp.status_code}, body[:100]={resp.text[:100]!r}"
    except Exception as exc:
        return f"probe failed: {exc}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal Tushare connectivity check")
    parser.add_argument("--start", default="20240101", help="start date YYYYMMDD")
    parser.add_argument("--end", default="20240110", help="end date YYYYMMDD")
    parser.add_argument("--base-url", default=None, help="Override Tushare base URL")
    parser.add_argument("--intranet", action="store_true", help="Disable proxies for内网直连")
    parser.add_argument("--timeout", type=int, default=10, help="request timeout seconds")
    parser.add_argument("--probe", action="store_true", help="only probe endpoint without API call")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = load_token()
    if not token:
        print(
            "No Tushare token found.\n"
            "Set env: export TUSHARE_API_KEY='your_token'\n"
            "or put your token in a file: .tushare_token (repo) or ~/.tushare_token"
        )
        return 1
    disable_proxy = bool(args.intranet)
    base_url = args.base_url
    if args.intranet and not base_url:
        base_url = "http://api.tushare.pro"

    # Show effective proxy/base config for debugging
    current_env_proxies = {k: v for k, v in os.environ.items() if k.lower() in {"http_proxy", "https_proxy"}}
    print(f"Using base_url={base_url or 'package default'} | disable_proxy={disable_proxy}")
    print(f"Env proxies before config: {current_env_proxies}")

    # Optional probe before API call
    if base_url:
        probe_proxies = None if disable_proxy else {
            k: os.environ.get(k)
            for k in ["http_proxy", "https_proxy"]
            if os.environ.get(k)
        }
        print("Probe:", probe_endpoint(base_url, probe_proxies, timeout=args.timeout))

    pro = build_pro_client(token, base_url=base_url, disable_proxy=disable_proxy)

    if args.probe:
        print("Probe-only mode, skip trade_cal.")
        return 0

    try:
        df = pro.trade_cal(
            exchange="",
            start_date=args.start,
            end_date=args.end,
            fields="exchange,cal_date,is_open,pretrade_date",
            timeout=args.timeout,
        )
    except Exception as exc:  # API returns errors for bad token/quota/network
        print(f"API call failed: {exc}")
        return 1

    if df.empty:
        print("API call succeeded but returned empty data (check token/quota/date range).")
        return 1

    print(f"Trade calendar sample rows: {len(df)}")
    print(df.head().to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
