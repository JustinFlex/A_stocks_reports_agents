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

    def _summarize_anomalies(values, limit=8):
        if not values:
            return values
        values = list(values)
        if len(values) <= limit:
            return values
        summary = []
        grouped: dict[str, list[str]] = {}
        for item in values:
            try:
                metric = str(item).split()[0]
            except Exception:  # pylint: disable=broad-except
                metric = "other"
            grouped.setdefault(metric, []).append(str(item))
        for metric, items in grouped.items():
            if len(items) <= 2:
                summary.extend(items)
                continue
            # Try to capture min/max percentage change for readability.
            deltas = []
            for it in items:
                match = re.search(r"changed\s+(-?\d+\.?\d*)%", it)
                if match:
                    try:
                        deltas.append(float(match.group(1)))
                    except Exception:  # pylint: disable=broad-except
                        continue
            if deltas:
                summary.append(
                    f"{metric}: {len(items)} 条，变动区间 {min(deltas):.1f}%~{max(deltas):.1f}%"
                )
            else:
                summary.append(f"{metric}: {len(items)} 条波动，详见明细")
        summary.append(f"...(共 {len(values)} 条，已汇总)")
        return summary

    anomalies_display = {k: _summarize_anomalies(v) for k, v in anomalies_raw.items()}
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
    for w in state.get("valuation_warnings") or []:
        if w not in quant_warnings:
            quant_warnings.append(w)
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

    def _strip_urls(text: str) -> str:
        return re.sub(r"https?://[^\s)\]]+", "", text or "").strip()

    def _clean_text_block(text: str) -> str:
        """Remove markdown noise, filler phrases, stray punctuation for readability."""
        if not text:
            return text
        # Drop common filler openings from LLM outputs.
        fillers = [
            "okay, these are the key points of the qualitative research generated based on the information you provided.",
            "ok, these are the key points of the qualitative research generated based on the information you provided.",
            "these are the key points",
            "learn more:",
            "learn more",
            "好的，这是基于您提供的信息生成的定性研究要点：",
            "好的，这是基于你提供的信息生成的定性研究要点：",
            "好的，这是基于您提供的信息生成的定性研究要点",
            "好的，这是基于你提供的信息生成的定性研究要点",
            "这是为您生成的",
            "这是为你生成的",
        ]
        stripped = text.strip()
        # Remove leading bullets/markers before checking fillers.
        leading_clean = re.sub(r"^[\-\*•#>\s]+", "", stripped)
        lowered = leading_clean.lower()
        for f in fillers:
            fl = f.lower()
            if lowered.startswith(fl):
                leading_clean = leading_clean[len(f):].lstrip()
                break
        text = leading_clean
        # Generic Chinese opener cleanup
        text = re.sub(r"^\s*好的[，,。]\s*", "", text)
        text = re.sub(r"^\s*(好的|ok|okay)[，,。]?\s*(这[里里]?|以下)?\s*(是|为)[^:：]*[:：]\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^[^\n]{0,40}定性研究要点[:：]\s*", "", text)
        # Drop trailing "Learn more" blocks and inline citation markers.
        text = re.sub(r"(?is)learn more:.*", "", text)
        text = re.sub(r"\[\[\d+\]\]", "", text)
        text = re.sub(r"\[\^\d+\]", "", text)
        text = re.sub(r"\b(Map|Reduce)\b[:：]?", "", text, flags=re.IGNORECASE)
        # Help news paragraphs break into readable chunks by dates.
        text = re.sub(r"\s*\[(20\d{2}-\d{2}-\d{2})\]\s*", r"\n\1 ", text)
        # Remove markdown emphasis/headings and bullets.
        text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
        lines = []
        for line in text.splitlines():
            line = re.sub(r"^[-*•#>\s]+", "", line.strip())
            if line:
                lines.append(line)
        separator = "； " if len(lines) > 1 else " "
        cleaned = separator.join(lines)
        # Remove empty parentheses/brackets left after URL stripping.
        cleaned = re.sub(r"\(\s*\)", "", cleaned)
        cleaned = re.sub(r"（\s*）", "", cleaned)
        # Collapse repeated spaces.
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        # Remove trailing "Learn more:"-style remnants.
        cleaned = re.sub(r"(learn more:?|了解更多[:：]?)\s*$", "", cleaned, flags=re.IGNORECASE).strip()
        return cleaned

    def _bulletize(text: str) -> str:
        if not text:
            return text
        parts = re.split(r"[；;]\s*|\n+", text)
        items = [p.strip() for p in parts if p and p.strip()]
        if len(items) <= 1:
            return text.strip()
        return "\n".join(f"- {item}" for item in items)

    def _build_footnotes(sections: dict[str, str]) -> tuple[list[dict], dict[str, list[int]], dict[str, str]]:
        """Collect URLs per section and return shared footnotes + section index map + cleaned text."""
        seen: dict[str, int] = {}
        footnotes: list[dict] = []
        section_refs: dict[str, list[int]] = {}
        cleaned: dict[str, str] = {}
        for name, text in sections.items():
            indices: list[int] = []
            for url in _extract_urls(str(text)):
                idx = seen.get(url)
                if idx is None:
                    idx = len(footnotes) + 1
                    seen[url] = idx
                    footnotes.append({"index": idx, "url": url})
                if idx not in indices:
                    indices.append(idx)
            if indices:
                section_refs[name] = indices
            cleaned[name] = _clean_text_block(_strip_urls(str(text)))
        return footnotes, section_refs, cleaned

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
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"Inline chart read failed for {path}: {exc}")
                continue
            b64 = base64.b64encode(data).decode("ascii")
            data_url = f"data:image/png;base64,{b64}"
            inlines.append({"data_url": data_url, "caption": ch.get("caption")})
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
        "industry_analysis": state.get("industry_analysis"),
        "growth_analysis": state.get("growth_analysis"),
        "financial_analysis": state.get("financial_analysis"),
        "valuation_analysis": state.get("valuation_analysis"),
        "risk_catalyst": state.get("risk_catalyst"),
        "anomalies": anomalies_display,
    }

    footnotes, section_refs, cleaned_texts = _build_footnotes(
        {
            "company_intro": render_context["company_intro"],
            "industry_analysis": render_context["industry_analysis"],
            "growth_analysis": render_context["growth_analysis"],
            "financial_analysis": render_context["financial_analysis"],
            "valuation_analysis": render_context["valuation_analysis"],
            "risk_catalyst": render_context["risk_catalyst"],
            "qa_report": render_context.get("qa_report"),
            "review_report": render_context.get("review_report"),
            "news_digest": news_digest,
            "qual_notes": qual_notes,
        }
    )

    render_context.update(
        {
            "charts": state.get("charts"),
            "charts_inline": _inline_charts(state.get("charts")),
            "qa_report": state.get("qa_report"),
            "review_report": cleaned_texts.get("review_report", review_report),
            "current_price": current_price,
        "valuation": valuation,
        "valuation_hints": state.get("valuation_hints") or {},
        "qa_warnings": qa_warnings,
        "quant_warnings": quant_warnings,
        "footnotes": footnotes,
        "section_refs": section_refs,
        "basic_info": state.get("basic_info") or {},
            "holders": state.get("holders") or [],
            "qual_notes": _bulletize(cleaned_texts.get("qual_notes", qual_notes)),
            "valuation_overrides": state.get("valuation_overrides") or {},
            "news_digest": _bulletize(cleaned_texts.get("news_digest", news_digest)),
            "company_intro": cleaned_texts.get("company_intro", render_context["company_intro"]),
            "industry_analysis": cleaned_texts.get("industry_analysis", render_context["industry_analysis"]),
            "growth_analysis": cleaned_texts.get("growth_analysis", render_context["growth_analysis"]),
            "financial_analysis": cleaned_texts.get("financial_analysis", render_context["financial_analysis"]),
            "valuation_analysis": cleaned_texts.get("valuation_analysis", render_context["valuation_analysis"]),
            "risk_catalyst": cleaned_texts.get("risk_catalyst", render_context["risk_catalyst"]),
        }
    )

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
