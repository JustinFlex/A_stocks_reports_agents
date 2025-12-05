"""SQLite persistence layer for financial statements and market caches."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class SQLiteRepository:
    """Lightweight gateway for reading and writing financial data."""

    def __init__(self, database_uri: str, *, echo: bool = False) -> None:
        self._engine: Engine = create_engine(database_uri, echo=echo, future=True)
        self._ensure_schema()

    @property
    def engine(self) -> Engine:
        return self._engine

    # -----------------
    # Schema management
    # -----------------
    def _ensure_schema(self) -> None:
        """Create core tables if they do not already exist."""
        ddl = [
            # Unified statements table for IS/BS/CF
            """
            CREATE TABLE IF NOT EXISTS statements (
              ticker TEXT NOT NULL,
              report_type TEXT NOT NULL,
              report_date DATE NOT NULL,
              metric TEXT NOT NULL,
              value REAL,
              PRIMARY KEY (ticker, report_type, report_date, metric)
            );
            """,
            # Daily price cache
            """
            CREATE TABLE IF NOT EXISTS prices (
              ticker TEXT NOT NULL,
              trade_date DATE NOT NULL,
              open REAL,
              high REAL,
              low REAL,
              close REAL,
              vol REAL,
              amount REAL,
              PRIMARY KEY (ticker, trade_date)
            );
            """,
            """CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices(ticker);""",
            # Latest price anchor for offline valuation
            """
            CREATE TABLE IF NOT EXISTS price_anchors (
              ticker TEXT PRIMARY KEY,
              trade_date DATE,
              close REAL,
              market_cap REAL,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # Company static metadata
            """
            CREATE TABLE IF NOT EXISTS basic_info (
              ticker TEXT PRIMARY KEY,
              name TEXT,
              area TEXT,
              industry TEXT,
              list_date TEXT,
              market TEXT,
              exchange TEXT,
              updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # Top 10 holders snapshots
            """
            CREATE TABLE IF NOT EXISTS holders (
              ticker TEXT NOT NULL,
              end_date DATE NOT NULL,
              holder_name TEXT NOT NULL,
              hold_ratio REAL,
              hold_amount REAL,
              PRIMARY KEY (ticker, end_date, holder_name)
            );
            """,
            """CREATE INDEX IF NOT EXISTS idx_holders_ticker ON holders(ticker);""",
        ]
        with self._engine.begin() as conn:
            for statement in ddl:
                conn.execute(text(statement))

    # ---------------
    # Statements CRUD
    # ---------------
    def fetch_statements(self, ticker: str) -> Dict[str, List[Dict[str, Any]]]:
        """Load IS/BS/CF data from the unified statements table."""
        query = text(
            """
            SELECT report_type, report_date, metric, value
            FROM statements
            WHERE ticker = :ticker
            ORDER BY report_date DESC
            """
        )
        result: Dict[str, List[Dict[str, Any]]] = {"IS": [], "BS": [], "CF": []}
        with self._engine.connect() as conn:
            rows = conn.execute(query, {"ticker": ticker})
            for row in rows.mappings():
                bucket = result.setdefault(row["report_type"], [])
                bucket.append(dict(row))
        return result

    def upsert_statements(self, ticker: str, payload: Iterable[Dict[str, Any]]) -> int:
        """Persist normalized statement rows into SQLite using UPSERT."""
        rows = [
            {
                "ticker": ticker,
                "report_type": item.get("report_type"),
                "report_date": item.get("report_date"),
                "metric": item.get("metric"),
                "value": item.get("value"),
            }
            for item in payload
            if item.get("report_type") and item.get("report_date") and item.get("metric")
        ]
        if not rows:
            return 0

        stmt = text(
            """
            INSERT INTO statements (ticker, report_type, report_date, metric, value)
            VALUES (:ticker, :report_type, :report_date, :metric, :value)
            ON CONFLICT(ticker, report_type, report_date, metric) DO UPDATE SET
                value=excluded.value
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, rows)
        return len(rows)

    # -------------
    # Prices cache
    # -------------
    def fetch_prices(
        self,
        ticker: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve cached price window ordered by trade_date descending."""
        clauses = ["ticker = :ticker"]
        params: Dict[str, Any] = {"ticker": ticker}
        if start_date is not None:
            clauses.append("trade_date >= :start_date")
            params["start_date"] = start_date.isoformat()
        if end_date is not None:
            clauses.append("trade_date <= :end_date")
            params["end_date"] = end_date.isoformat()
        where = " AND ".join(clauses)
        limit_clause = ""
        if limit is not None:
            limit_clause = f" LIMIT {int(limit)}"

        query = text(
            f"""
            SELECT ticker, trade_date, open, high, low, close, vol, amount
            FROM prices
            WHERE {where}
            ORDER BY trade_date DESC
            {limit_clause}
            """
        )
        with self._engine.connect() as conn:
            rows = conn.execute(query, params)
            return [dict(row) for row in rows.mappings()]

    def upsert_prices(self, ticker: str, rows: Iterable[Dict[str, Any]]) -> int:
        """Cache TuShare daily data into the prices table."""
        payload = []
        for row in rows:
            trade_date = row.get("trade_date")
            if trade_date is None:
                continue
            payload.append(
                {
                    "ticker": ticker,
                    "trade_date": str(trade_date),
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "vol": row.get("vol"),
                    "amount": row.get("amount"),
                }
            )
        if not payload:
            return 0
        stmt = text(
            """
            INSERT INTO prices (ticker, trade_date, open, high, low, close, vol, amount)
            VALUES (:ticker, :trade_date, :open, :high, :low, :close, :vol, :amount)
            ON CONFLICT(ticker, trade_date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                vol=excluded.vol,
                amount=excluded.amount
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, payload)
        return len(payload)

    def upsert_price_anchor(self, ticker: str, trade_date: Optional[str], close: Optional[float], market_cap: Optional[float]) -> None:
        stmt = text(
            """
            INSERT INTO price_anchors (ticker, trade_date, close, market_cap, updated_at)
            VALUES (:ticker, :trade_date, :close, :market_cap, CURRENT_TIMESTAMP)
            ON CONFLICT(ticker) DO UPDATE SET
              trade_date=excluded.trade_date,
              close=excluded.close,
              market_cap=excluded.market_cap,
              updated_at=CURRENT_TIMESTAMP
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "close": close,
                    "market_cap": market_cap,
                },
            )

    def fetch_price_anchor(self, ticker: str) -> Optional[Dict[str, Any]]:
        query = text(
            """
            SELECT ticker, trade_date, close, market_cap
            FROM price_anchors
            WHERE ticker = :ticker
            LIMIT 1
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(query, {"ticker": ticker}).mappings().first()
            return dict(row) if row else None

    # ---------------------
    # Basic info & holders
    # ---------------------
    def upsert_basic_info(self, info: Dict[str, Any]) -> None:
        """Upsert static metadata for a ticker."""
        if not info.get("ts_code"):
            return
        stmt = text(
            """
            INSERT INTO basic_info (ticker, name, area, industry, list_date, market, exchange, updated_at)
            VALUES (:ts_code, :name, :area, :industry, :list_date, :market, :exchange, CURRENT_TIMESTAMP)
            ON CONFLICT(ticker) DO UPDATE SET
              name=excluded.name,
              area=excluded.area,
              industry=excluded.industry,
              list_date=excluded.list_date,
              market=excluded.market,
              exchange=excluded.exchange,
              updated_at=CURRENT_TIMESTAMP
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, info)

    def fetch_basic_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        query = text(
            """
            SELECT ticker, name, area, industry, list_date, market, exchange, updated_at
            FROM basic_info
            WHERE ticker = :ticker
            LIMIT 1
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(query, {"ticker": ticker}).mappings().first()
            return dict(row) if row else None

    def upsert_holders(self, ticker: str, holders: Iterable[Dict[str, Any]]) -> int:
        """Persist top 10 holder snapshots."""
        rows = []
        for h in holders:
            if not h.get("end_date") or not h.get("holder_name"):
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "end_date": h.get("end_date"),
                    "holder_name": h.get("holder_name"),
                    "hold_ratio": h.get("hold_ratio"),
                    "hold_amount": h.get("hold_amount"),
                }
            )
        if not rows:
            return 0
        stmt = text(
            """
            INSERT INTO holders (ticker, end_date, holder_name, hold_ratio, hold_amount)
            VALUES (:ticker, :end_date, :holder_name, :hold_ratio, :hold_amount)
            ON CONFLICT(ticker, end_date, holder_name) DO UPDATE SET
              hold_ratio=excluded.hold_ratio,
              hold_amount=excluded.hold_amount
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, rows)
        return len(rows)

    def fetch_holders(self, ticker: str) -> List[Dict[str, Any]]:
        query = text(
            """
            SELECT ticker, end_date, holder_name, hold_ratio, hold_amount
            FROM holders
            WHERE ticker = :ticker
            ORDER BY end_date DESC
            """
        )
        with self._engine.connect() as conn:
            rows = conn.execute(query, {"ticker": ticker}).mappings()
            return [dict(r) for r in rows]
