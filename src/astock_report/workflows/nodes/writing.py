"""LangGraph node responsible for final Markdown assembly."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from astock_report.reports.renderer import ReportRenderer
from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

_TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "reports" / "templates"


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    logs.append("WritingAgent -> render Markdown output")

    renderer = ReportRenderer(template_dir=_TEMPLATE_DIR)
    render_context = {
        "ticker": state.get("ticker"),
        "company_name": state.get("company_name"),
        "report_date": state.get("report_date", datetime.utcnow().date().isoformat()),
        "core_viewpoints": state.get("core_viewpoints"),
        "company_intro": state.get("company_intro"),
        "industry_analysis": state.get("industry_analysis"),
        "growth_analysis": state.get("growth_analysis"),
        "financial_analysis": state.get("financial_analysis"),
        "valuation_analysis": state.get("valuation_analysis"),
        "risk_catalyst": state.get("risk_catalyst"),
        "anomalies": state.get("anomalies"),
        "charts": state.get("charts"),
        "qa_report": state.get("qa_report"),
        "review_report": state.get("review_report"),
    }

    try:
        state["markdown_report"] = renderer.render(render_context)
        state["html_report"] = renderer.render_template("base_report.html.j2", render_context)
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Markdown render failed: {exc}")
    return state
