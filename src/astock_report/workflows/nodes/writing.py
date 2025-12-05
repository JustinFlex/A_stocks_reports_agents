"""LangGraph node responsible for final Markdown assembly."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
import base64

from astock_report.reports.renderer import ReportRenderer
from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

_TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "reports" / "templates"


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    logs.append("WritingAgent -> render Markdown output")

    renderer = ReportRenderer(template_dir=_TEMPLATE_DIR)
    anomalies_raw = state.get("anomalies") or {}

    def _limit_list(values, limit=8):
        if not values:
            return values
        values = list(values)
        if len(values) > limit:
            return values[:limit] + [f"...(+{len(values) - limit} more)"]
        return values

    anomalies_display = {k: _limit_list(v) for k, v in anomalies_raw.items()}
    ratios = state.get("ratios")
    growth_curve = state.get("growth_curve")
    qa_warnings = state.get("qa_warnings") or []
    news_digest = state.get("news_digest") or ""
    qual_notes = state.get("qual_notes") or ""
    review_report = state.get("review_report") or ""

    def _get_ratio(key: str) -> float:
        try:
            if hasattr(ratios, "ratios"):
                return float(ratios.ratios.get(key, float("nan")))
            return float(ratios.get("ratios", {}).get(key, float("nan")))
        except Exception:
            return float("nan")

    def _get_growth(key: str) -> float:
        try:
            if hasattr(growth_curve, "metrics"):
                return float(growth_curve.metrics.get(key, float("nan")))
            return float((growth_curve or {}).get("metrics", {}).get(key, float("nan")))
        except Exception:
            return float("nan")

    current_price = state.get("current_price")
    valuation = state.get("valuation") or {}
    intrinsic = None
    methods = {}
    if hasattr(valuation, "intrinsic_value"):
        intrinsic = getattr(valuation, "intrinsic_value", None)
        methods = getattr(valuation, "valuation_methods", {}) or {}
    elif isinstance(valuation, dict):
        intrinsic = valuation.get("intrinsic_value")
        methods = valuation.get("valuation_methods", {}) or {}

    quant_warnings = list(qa_warnings)
    # Clarify negative intrinsic vs positive price coexistence
    if intrinsic is not None and intrinsic == intrinsic and current_price and current_price > 0 and intrinsic < 0:
        msg = "DCF 内在价值为负但现价为正：模型可与市场价格共存，需结合其他估值（PB/EV/Sales）与假设复核。"
        if msg not in quant_warnings:
            quant_warnings.append(msg)
    # Leverage/liquidity clarification when ratios seem mild but coverage is weak
    debt_to_equity = _get_ratio("debt_to_equity")
    interest_cov = _get_ratio("interest_coverage")
    if debt_to_equity and debt_to_equity < 0.5 and (interest_cov != interest_cov or interest_cov < 1):
        msg = "杠杆率看似不高，但利息保障/现金流覆盖不足，需结合流动性表述审慎解读。"
        if msg not in quant_warnings:
            quant_warnings.append(msg)
    # Low-base effect note when YoY positive but margins still negative
    net_margin = _get_ratio("net_margin")
    net_income_yoy = _get_growth("net_income_yoy")
    if net_margin != net_margin:
        net_margin = None
    if net_income_yoy != net_income_yoy:
        net_income_yoy = None
    if net_margin is not None and net_margin < 0 and net_income_yoy is not None and net_income_yoy > 0:
        msg = "净利润同比转正/改善可能源于低基数，当前净利率仍为负，需警惕可持续性。"
        if msg not in quant_warnings:
            quant_warnings.append(msg)

    def _extract_urls(text: str) -> list[str]:
        if not text:
            return []
        return re.findall(r"https?://[^\s)\]]+", text)

    urls = []
    for segment in [news_digest, qual_notes, review_report]:
        urls.extend(_extract_urls(str(segment)))
    # Deduplicate while preserving order
    seen = set()
    deduped_urls: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            deduped_urls.append(u)
    footnotes = [{"index": idx + 1, "url": u} for idx, u in enumerate(deduped_urls)]
    footnote_tag = "".join(f"[^{item['index']}]" for item in footnotes) if footnotes else ""

    # Rating heuristic based on intrinsic vs price
    rating_text = "中性（数据不足）"
    if intrinsic is not None and intrinsic == intrinsic and current_price and current_price > 0:
        ratio = intrinsic / current_price
        if intrinsic <= 0:
            rating_text = "回避/减持（内在价值为负）"
        elif ratio >= 1.2:
            rating_text = "买入/增持（估值高于现价≥20%）"
        elif ratio >= 0.9:
            rating_text = "持有/中性（估值接近现价）"
        else:
            rating_text = "减持/谨慎（估值低于现价>10%）"

    def _inline_charts(charts: list[dict]) -> list[dict]:
        inlines = []
        for ch in charts or []:
            path = ch.get("path")
            if not path:
                continue
            try:
                data = Path(path).read_bytes()
                b64 = base64.b64encode(data).decode("ascii")
                data_url = f"data:image/png;base64,{b64}"
                inlines.append({"data_url": data_url, "caption": ch.get("caption")})
            except Exception:
                inlines.append(ch)
        return inlines

    render_context = {
        "ticker": state.get("ticker"),
        "company_name": state.get("company_name"),
        "report_date": state.get("report_date", datetime.utcnow().date().isoformat()),
        "report_timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "core_viewpoints": state.get("core_viewpoints"),
        "rating_text": rating_text,
        "rating_text": rating_text,
        "company_intro": state.get("company_intro"),
        "company_intro_refs": footnote_tag,
        "industry_analysis": state.get("industry_analysis"),
        "industry_analysis_refs": footnote_tag,
        "growth_analysis": state.get("growth_analysis"),
        "financial_analysis": state.get("financial_analysis"),
        "valuation_analysis": state.get("valuation_analysis"),
        "risk_catalyst": state.get("risk_catalyst"),
        "anomalies": anomalies_display,
        "charts": state.get("charts"),
        "charts_inline": _inline_charts(state.get("charts")),
        "qa_report": state.get("qa_report"),
        "review_report": state.get("review_report"),
        "current_price": current_price,
        "valuation": valuation,
        "qa_warnings": qa_warnings,
        "quant_warnings": quant_warnings,
        "footnotes": footnotes,
        "basic_info": state.get("basic_info") or {},
        "holders": state.get("holders") or [],
        "qual_notes": state.get("qual_notes"),
        "valuation_overrides": state.get("valuation_overrides") or {},
    }

    try:
        charts_inline = render_context.get("charts_inline")
        state["markdown_report"] = renderer.render(render_context)
        state["html_report"] = renderer.render_template("base_report.html.j2", render_context)
        state["quant_warnings"] = quant_warnings
        state["charts_inline"] = charts_inline
        state["rating_text"] = rating_text
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Markdown render failed: {exc}")
    return state
