"""LangGraph node deriving financial ratios."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    dataset = state.get("financials")

    if dataset is None or not dataset.is_complete():
        errors.append("RatioCalcAgent skipped because financial dataset is incomplete.")
        return state

    logs.append("RatioCalcAgent -> derive KPI set")
    try:
        state["ratios"] = context.ratio_calculator.calculate(dataset)
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Ratio calculation failed: {exc}")
    return state