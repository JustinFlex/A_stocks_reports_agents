"""LangGraph node orchestrating growth + ratio calculations."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    dataset = state.get("financials")
    current_price = state.get("current_price")

    # Inject latest price into balance sheet metrics to improve PE/PB/EV calc
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

    if dataset is None or not dataset.is_complete():
        errors.append("QuantMetricsAgent skipped because financial dataset is incomplete.")
        return state

    logs.append("QuantMetricsAgent -> compute growth and ratios")
    try:
        state["growth_curve"] = context.growth_calculator.calculate(dataset)
        state["ratios"] = context.ratio_calculator.calculate(dataset)
        if context.anomaly_detector:
            state["anomalies"] = context.anomaly_detector.detect(dataset, state["ratios"])
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Quant metrics failed: {exc}")
    return state
