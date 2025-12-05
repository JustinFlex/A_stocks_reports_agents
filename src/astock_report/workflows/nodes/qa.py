"""LangGraph node to perform lightweight QA on the assembled report."""
from __future__ import annotations

from typing import Any, Dict, List

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

CHECKS = [
    ("financials", "财报数据已加载", True),
    ("price_history", "行情窗口可用", False),
    ("ratios", "财务比率已计算", True),
    ("valuation", "估值结果已生成", True),
    ("news_digest", "新闻摘要可用", True),
    ("qual_notes", "定性研究可用", True),
    ("company_intro", "叙事段落: 公司简介", True),
    ("industry_analysis", "叙事段落: 行业分析", True),
    ("growth_analysis", "叙事段落: 成长性", True),
    ("financial_analysis", "叙事段落: 财务分析", True),
    ("valuation_analysis", "叙事段落: 估值解读", True),
    ("risk_catalyst", "风险与催化剂", True),
    ("review_report", "复核报告可用", False),
    ("anomalies", "异常检测结果可用", False),
    ("markdown_report", "Markdown 报告已渲染", True),
    ("citations_news", "新闻段落包含来源格式", False),
    ("citations_qual", "定性要点包含来源格式", False),
]

NARRATIVE_KEYS = [
    "company_intro",
    "industry_analysis",
    "growth_analysis",
    "financial_analysis",
    "valuation_analysis",
    "core_viewpoints",
]


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    rewrite_requests: List[Dict[str, Any]] = state.setdefault("rewrite_requests", [])
    warnings: List[str] = state.setdefault("qa_warnings", [])
    rewrite_requests.clear()
    errors[:] = [e for e in errors if not e.startswith("QA: 估值结果为空或未计算")]

    logs.append("QAAgent -> verify mandatory sections before exit")

    checks: List[Dict[str, str]] = []
    missing_narratives: List[str] = []
    for key, label, critical in CHECKS:
        present = bool(state.get(key))
        if key in NARRATIVE_KEYS:
            present = _has_content(state.get(key))
            if not present:
                missing_narratives.append(key)
        # Heuristic citation checks
        if key == "citations_news":
            present = "(" in str(state.get("news_digest", ""))
        if key == "citations_qual":
            present = "来源" in str(state.get("qual_notes", ""))
        detail = "ok" if present else "missing"
        checks.append({"key": key, "label": label, "status": detail})
        if critical and not present:
            errors.append(f"QA: {label} 缺失")

    if missing_narratives:
        logs.append(f"QAAgent -> narrative sections missing: {missing_narratives}")
        rewrite_requests.append(
            {
                "stage": "narrative",
                "reason": "叙事段落缺失或为空",
                "missing_keys": missing_narratives,
                "suggested_action": "rerun_narrative_node",
            }
        )

    _validate_valuation(state, errors, warnings, rewrite_requests, logs)

    state["qa_report"] = {
        "passed": not any(item["status"] == "missing" for item in checks),
        "checks": checks,
        "missing_narratives": missing_narratives,
        "rewrite_requests": rewrite_requests,
        "warnings": warnings,
    }
    return state


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _validate_valuation(
    state: ReportState, errors: List[str], warnings: List[str], rewrite_requests: List[Dict[str, Any]], logs: List[str]
) -> None:
    valuation = state.get("valuation") or {}
    ratios = state.get("ratios") or {}
    current_price = state.get("current_price")
    if hasattr(valuation, "valuation_methods"):
        methods = getattr(valuation, "valuation_methods")
        intrinsic = getattr(valuation, "intrinsic_value", None)
    elif isinstance(valuation, dict):
        methods = valuation.get("valuation_methods")
        intrinsic = valuation.get("intrinsic_value")
    else:
        methods = None
        intrinsic = None
    ratio_values = ratios.ratios if hasattr(ratios, "ratios") else ratios.get("ratios", {})
    eps = ratio_values.get("eps")
    if not methods:
        msg = "QA: 估值结果为空或未计算"
        if msg not in errors:
            errors.append(msg)
        rewrite_requests.append(
            {
                "stage": "valuation",
                "reason": "估值结果为空或未计算",
                "suggested_action": "rerun_valuation_node",
            }
        )
        return
    # PE band requires valid EPS/price
    if "pe_band" in methods and (eps is None or eps != eps or eps <= 0 or current_price in (None, 0)):
        msg = "QA: pe_band 估值缺少有效 EPS 或价格，需重算"
        if msg not in errors:
            errors.append(msg)
        rewrite_requests.append(
            {
                "stage": "valuation",
                "reason": "pe_band 缺少 EPS/price",
                "suggested_action": "rerun_valuation_node",
            }
        )
    # Negative intrinsic vs positive price
    if intrinsic is not None and intrinsic == intrinsic and current_price is not None and current_price > 0 and intrinsic <= 0:
        msg = "Warning: 内在价值为负但现价为正（常见于高风险/重组预期场景），请人工复核估值假设"
        if msg not in warnings:
            warnings.append(msg)
        logs.append("QAAgent -> flagged negative intrinsic vs positive price (warning only)")
