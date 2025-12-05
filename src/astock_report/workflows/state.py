"""Workflow state definitions shared by LangGraph nodes."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from astock_report.domain.models.financials import (
    FinancialDataset,
    GrowthCurve,
    RatioSummary,
    ValuationBundle,
)


class ReportState(TypedDict, total=False):
    ticker: str
    company_name: Optional[str]
    report_date: str
    current_price: Optional[float]
    price_history: Optional[List[Dict[str, Any]]]

    financials: Optional[FinancialDataset]
    growth_curve: Optional[GrowthCurve]
    ratios: Optional[RatioSummary]
    valuation: Optional[ValuationBundle]

    company_intro: Optional[str]
    industry_analysis: Optional[str]
    growth_analysis: Optional[str]
    financial_analysis: Optional[str]
    valuation_analysis: Optional[str]
    risk_catalyst: Optional[str]
    core_viewpoints: Optional[str]
    review_report: Optional[str]
    markdown_report: Optional[str]
    html_report: Optional[str]
    news_digest: Optional[str]
    qa_report: Optional[Dict[str, Any]]
    rewrite_requests: Optional[List[Dict[str, Any]]]
    narrative_missing_sections: Optional[List[str]]
    stage_order: List[str]

    logs: List[str]
    errors: List[str]

    extras: Dict[str, Any]
