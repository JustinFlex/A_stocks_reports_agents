"""LangGraph node deriving growth trends from financial statements."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    dataset = state.get("financials")

    if dataset is None or not dataset.is_complete():
        errors.append("GrowthCurveAgent skipped because financial dataset is incomplete.")
        return state

    logs.append("GrowthCurveAgent -> compute CAGR metrics")
    try:
        state["growth_curve"] = context.growth_calculator.calculate(dataset)
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Growth calculation failed: {exc}")
    return state