"""LangGraph node for valuation modelling."""
from __future__ import annotations

from astock_report.domain.models.financials import RatioSummary
from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    dataset = state.get("financials")
    ratios = state.get("ratios")
    current_price = state.get("current_price")

    # Ensure valuation sees latest price even if not present in DB snapshot
    if dataset and dataset.balance_sheets and current_price is not None:
        latest_bs = dataset.balance_sheets[-1]
        latest_bs.metrics = dict(latest_bs.metrics)
        latest_bs.metrics["price"] = float(current_price)
        shares = latest_bs.metrics.get("shares_outstanding")
        if shares is not None:
            try:
                latest_bs.metrics["market_cap"] = float(current_price) * float(shares)
            except Exception:
                pass

    if dataset is None or ratios is None:
        errors.append("ValuationAgent skipped because prerequisites are missing.")
        return state

    logs.append("ValuationAgent -> execute DCF and peer comparisons")
    try:
        ratio_summary = ratios if isinstance(ratios, RatioSummary) else ratios
        overrides = state.get("valuation_overrides") or {}
        hints = state.get("valuation_hints") or {}
        bundle = context.valuation_engine.run(dataset, ratio_summary, overrides=overrides, hints=hints)
        state["valuation"] = bundle
        if getattr(bundle, "warnings", None):
            state["valuation_warnings"] = list(bundle.warnings)
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Valuation engine failed: {exc}")
    return state
