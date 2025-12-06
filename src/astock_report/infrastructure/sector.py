"""Sector service to map tickers to Shenwan indices and peer percentiles."""
from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from astock_report.infrastructure.data_providers.tushare_client import TuShareClient
from astock_report.infrastructure.db.sqlite import SQLiteRepository


class SectorService:
    def __init__(self, repository: SQLiteRepository, tushare: Optional[TuShareClient]) -> None:
        self._repo = repository
        self._tushare = tushare

    def refresh_sw_classifications(self) -> None:
        if self._tushare is None:
            return
        try:
            df = self._tushare.fetch_index_classify(src="SW")
            if df is None or df.empty:
                return
            rows = df.to_dict(orient="records")
            self._repo.upsert_sw_classifications(rows)
        except Exception:
            return

    def refresh_sw_members(self, index_code: str, level: Optional[str] = None) -> None:
        """Refresh cached constituents for a Shenwan index code."""
        if self._tushare is None:
            return
        level = level or self._lookup_level(index_code)
        field = self._level_to_field(level)
        if field is None:
            return
        try:
            df = self._tushare.fetch_index_member_all(**{field: index_code, "is_new": "Y"})
            self._cache_member_all(df)
        except Exception:
            return

    def resolve_sw_index(self, ticker: Optional[str], industry_name: Optional[str] = None) -> Optional[Dict[str, str]]:
        """Resolve the preferred Shenwan index for a ticker."""
        if not ticker:
            return None
        memberships = self._repo.fetch_sw_memberships_for_ticker(ticker)
        selection = self._select_preferred_index(memberships)
        if selection:
            return selection
        # Load fresh membership from TuShare
        if self._tushare is not None:
            try:
                df = self._tushare.fetch_index_member_all(ts_code=ticker, is_new="Y")
                self._cache_member_all(df)
                memberships = self._repo.fetch_sw_memberships_for_ticker(ticker)
                selection = self._select_preferred_index(memberships)
                if selection:
                    return selection
            except Exception:
                pass
        # Fallback: match by industry name against classification names
        if industry_name:
            name = str(industry_name).lower()
            classes = self._get_classifications()
            for row in classes:
                idx_name = str(row.get("index_name", "")).lower()
                if name in idx_name or idx_name in name:
                    return {
                        "index_code": row.get("index_code"),
                        "index_name": row.get("index_name"),
                        "level": row.get("level"),
                        "member_count": None,
                    }
        return None

    def peer_percentiles(self, index_code: str, *, trade_date: Optional[date] = None) -> Dict[str, float]:
        """Compute peer percentile bands (20/50/80) for PE/PB/PS using constituents."""
        if self._tushare is None or not index_code:
            return {}
        try:
            self.refresh_sw_members(index_code)
            members = self._repo.fetch_sw_members(index_code)
            if not members:
                return {}
            ts_codes = [m["ts_code"] for m in members if m.get("ts_code")]
            if not ts_codes:
                return {}
            trade_date_str = self._format_trade_date(trade_date)

            fields = "ts_code,trade_date,pe_ttm,pb,ps_ttm"
            df = self._tushare.fetch_daily_basic(trade_date=trade_date_str, fields=fields)
            filtered = pd.DataFrame()
            if df is not None and not df.empty:
                filtered = df[df["ts_code"].isin(ts_codes)]
            if filtered.empty:
                # Fallback: per-ticker latest snapshot
                frames = []
                for code in ts_codes:
                    try:
                        snap = self._tushare.fetch_daily_basic(ts_code=code, limit=1, fields=fields)
                        if snap is not None and not snap.empty:
                            frames.append(snap.tail(1))
                    except Exception:
                        continue
                if frames:
                    filtered = pd.concat(frames, ignore_index=True)
            if filtered.empty:
                return {}

            # Drop extreme outliers to stabilize bands
            def winsor(series, lower=1, upper=99):
                vals = series.dropna().astype(float)
                if vals.empty:
                    return vals
                lower_val = np.percentile(vals, lower)
                upper_val = np.percentile(vals, upper)
                return vals.clip(lower_val, upper_val)

            filtered["pe_ttm"] = winsor(filtered["pe_ttm"])
            filtered["pb"] = winsor(filtered["pb"])
            if "ps_ttm" in filtered:
                filtered["ps_ttm"] = winsor(filtered["ps_ttm"])

            def pct(series, p):
                vals = series.dropna().astype(float)
                return float(np.percentile(vals, p)) if len(vals) else float("nan")

            def bundle(series):
                return {
                    "p20": pct(series, 20),
                    "p25": pct(series, 25),
                    "p50": pct(series, 50),
                    "p75": pct(series, 75),
                    "p80": pct(series, 80),
                }

            pe = bundle(filtered["pe_ttm"])
            pb = bundle(filtered["pb"])
            ps = bundle(filtered["ps_ttm"]) if "ps_ttm" in filtered else {"p20": float("nan"), "p25": float("nan"), "p50": float("nan"), "p75": float("nan"), "p80": float("nan")}
            return {
                "pe": pe,
                "pb": pb,
                "ps": ps,
                "sample_size": int(len(filtered)),
                "trade_date": str(filtered["trade_date"].iloc[0]) if "trade_date" in filtered else None,
            }
        except Exception:
            return {}

    # -----------------
    # Internal helpers
    # -----------------
    def _format_trade_date(self, trade_date: Optional[date]) -> Optional[str]:
        if trade_date is None:
            return None
        try:
            return trade_date.strftime("%Y%m%d")
        except Exception:
            try:
                # Handle string inputs like "2024-06-01" or "20240601"
                text = str(trade_date)
                return text.replace("-", "")
            except Exception:
                return None

    def _get_classifications(self) -> List[Dict]:
        cached = self._repo.fetch_sw_classification()
        if cached:
            return cached
        self.refresh_sw_classifications()
        return self._repo.fetch_sw_classification()

    def _level_to_field(self, level: Optional[str]) -> Optional[str]:
        if level is None:
            return None
        lvl = str(level).upper()
        if lvl.startswith("L1"):
            return "l1_code"
        if lvl.startswith("L2"):
            return "l2_code"
        if lvl.startswith("L3"):
            return "l3_code"
        return None

    def _lookup_level(self, index_code: str) -> Optional[str]:
        record = self._repo.fetch_sw_classification(index_code=index_code)
        if record:
            return record[0].get("level")
        self.refresh_sw_classifications()
        record = self._repo.fetch_sw_classification(index_code=index_code)
        if record:
            return record[0].get("level")
        return None

    def _cache_member_all(self, df) -> None:
        if df is None or df.empty:
            return
        class_rows: List[Dict[str, str]] = []
        members_by_index: Dict[str, List[Dict[str, str]]] = {}

        for _, row in df.iterrows():
            for level, code_key, name_key in [("L1", "l1_code", "l1_name"), ("L2", "l2_code", "l2_name"), ("L3", "l3_code", "l3_name")]:
                code = row.get(code_key)
                if not code:
                    continue
                class_rows.append(
                    {
                        "index_code": code,
                        "index_name": row.get(name_key),
                        "level": level,
                        "industry_code": None,
                    }
                )
                members_by_index.setdefault(code, []).append(
                    {
                        "ts_code": row.get("ts_code") or row.get("con_code"),
                        "name": row.get("name"),
                        "weight": row.get("weight"),
                        "con_date": row.get("in_date") or row.get("con_date"),
                    }
                )

        if class_rows:
            self._repo.upsert_sw_classifications(class_rows)
        for code, rows in members_by_index.items():
            self._repo.upsert_sw_members(code, rows)

    def _select_preferred_index(self, memberships: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
        if not memberships:
            return None
        counts: Dict[str, int] = {}
        for row in memberships:
            code = row.get("index_code")
            if not code:
                continue
            counts[code] = counts.get(code, 0) + 1

        if not counts:
            return None

        classifications = {c["index_code"]: c for c in self._get_classifications()}

        def rank(code: str) -> Tuple[int, int]:
            level = str(classifications.get(code, {}).get("level") or "").upper()
            order = {"L2": 0, "L1": 1, "L3": 2}
            return (order.get(level, 3), -counts.get(code, 0))

        best = sorted(counts.keys(), key=rank)[0]
        meta = classifications.get(best, {})
        return {
            "index_code": best,
            "index_name": meta.get("index_name"),
            "level": meta.get("level"),
            "member_count": counts.get(best),
        }
