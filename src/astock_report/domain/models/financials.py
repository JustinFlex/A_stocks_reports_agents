"""Domain models describing the financial data exchanged between services."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


@dataclass
class FinancialStatement:
    """Represents a normalized financial statement for a single period."""

    ticker: str
    period: date
    statement_type: str
    metrics: Dict[str, float] = field(default_factory=dict)
    frequency: Optional[str] = None  # e.g., "annual" or "quarterly"
    update_flag: Optional[int] = None  # 1 for revised, 0 for initial when provided
    announced_date: Optional[date] = None


@dataclass
class FinancialDataset:
    """Container aggregating the three statements required by calculators."""

    ticker: str
    income_statements: List[FinancialStatement] = field(default_factory=list)
    balance_sheets: List[FinancialStatement] = field(default_factory=list)
    cash_flows: List[FinancialStatement] = field(default_factory=list)

    def is_complete(self) -> bool:
        """Quick validity check for downstream services."""
        return all([
            len(self.income_statements) > 0,
            len(self.balance_sheets) > 0,
            len(self.cash_flows) > 0,
        ])


@dataclass
class GrowthCurve:
    """Stores CAGR and trend information consumed by the writing agent."""

    metrics: Dict[str, float]
    commentary: Optional[str] = None


@dataclass
class RatioSummary:
    """Collection of derived financial ratios."""

    ratios: Dict[str, float]
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class ValuationBundle:
    """Hold valuation outputs so multiple methods can be compared."""

    intrinsic_value: Optional[float]
    valuation_methods: Dict[str, Dict[str, float]] = field(default_factory=dict)


@dataclass
class ReportDraft:
    """All ingredients required to assemble the final Markdown document."""

    company_intro: Optional[str]
    sections: Dict[str, str]
    markdown: Optional[str] = None
