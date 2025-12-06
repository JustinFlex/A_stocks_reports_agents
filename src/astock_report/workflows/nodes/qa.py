"""LangGraph node to perform lightweight QA on the assembled report."""
from __future__ import annotations

from typing import Any, Dict, List
import re

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.nodes import news as news_node
from astock_report.workflows.state import ReportState

CHECKS = [
    ("financials", "财报数据已加载", True),
    ("price_history", "行情窗口可用", False),
    ("ratios", "财务比率已计算", True),
    ("valuation", "估值结果已生成", True),
    ("news_digest", "新闻摘要可用", True),
    ("news_quality", "新闻摘要格式合规", True),
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

MIN_NARRATIVE_CHARS = 140
URL_RE = re.compile(r"https?://", re.IGNORECASE)
SOURCE_RE = re.compile(r"(来源|source)\s*[:：]", re.IGNORECASE)


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
    short_narratives: List[str] = []
    for key, label, critical in CHECKS:
        present = bool(state.get(key))
        if key in NARRATIVE_KEYS:
            present = _has_content(state.get(key))
            if not present:
                missing_narratives.append(key)
            elif _is_too_short(state.get(key)):
                short_narratives.append(key)
        # Heuristic citation checks
        if key == "citations_news":
            present = _has_citation(state.get("news_digest"))
        if key == "citations_qual":
            present = _has_citation(state.get("qual_notes"))
        if key == "news_quality":
            present = not _news_digest_invalid(state.get("news_digest"))
        detail = "ok" if present else "missing"
        severity = "critical" if critical else "warning"
        checks.append({"key": key, "label": label, "status": detail, "severity": severity})
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
    if short_narratives:
        warning_msg = f"叙事篇幅偏短(<{MIN_NARRATIVE_CHARS}字)：{short_narratives}"
        if warning_msg not in warnings:
            warnings.append(warning_msg)
        rewrite_requests.append(
            {
                "stage": "narrative",
                "reason": warning_msg,
                "missing_keys": short_narratives,
                "suggested_action": "rerun_narrative_node",
            }
        )

    _validate_valuation(state, errors, warnings, rewrite_requests, logs)

    if _news_digest_invalid(state.get("news_digest")):
        msg = "新闻摘要格式异常或含占位符，建议重跑新闻节点"
        if msg not in warnings:
            warnings.append(msg)
        rewrite_requests.append(
            {
                "stage": "news_fetch_mapreduce",
                "reason": msg,
                "suggested_action": "rerun_news_node",
            }
        )

    state["qa_report"] = {
        "passed": not any(item["status"] == "missing" for item in checks),
        "checks": checks,
        "missing_narratives": missing_narratives,
        "rewrite_requests": rewrite_requests,
        "warnings": warnings,
        "short_narratives": short_narratives,
    }
    return state


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _is_too_short(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return len(value.strip()) < MIN_NARRATIVE_CHARS


def _has_citation(value: Any) -> bool:
    if value is None:
        return False
    text = str(value)
    return bool(URL_RE.search(text) or SOURCE_RE.search(text))


def _news_digest_invalid(value: Any) -> bool:
    return not news_node.is_valid_news_digest(value or "")


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
    missing_methods = []
    for method_key, reason in [
        ("dcf", "DCF 未生成（缺少 FCF/净债务/股本）"),
        ("ev_ebitda", "EV/EBITDA 未生成（EBITDA<=0 或缺少净债务/股本）"),
    ]:
        if method_key not in methods and reason not in warnings:
            missing_methods.append(reason)
    warnings.extend(missing_methods)
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
