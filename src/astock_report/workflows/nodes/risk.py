"""LangGraph node producing risk and catalyst narratives via LLM."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

SYSTEM_PROMPT = "You are a CFA-level equity research analyst writing in Chinese."


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    if context.gemini is None:
        logs.append("RiskCatalystAgent -> skipped (Gemini client not configured)")
        state["risk_catalyst"] = "(未配置Gemini，待完善)"
        return state

    logs.append("RiskCatalystAgent -> request qualitative analysis from Gemini")
    qual_notes = state.get("qual_notes") or "(暂无定性摘要)"
    basic_info = state.get("basic_info") or {}
    holders = state.get("holders") or []
    anomalies = state.get("anomalies") or {}
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "请根据以下定量结果与公司背景，生成风险与催化剂段落，并显式引用来源（定量/新闻/定性/未知）：\n"
                f"Ticker: {state.get('ticker')}\n"
                f"Core ratios: {state.get('ratios')}\n"
                f"Valuation: {state.get('valuation')}\n"
                f"Price window (latest): {state.get('current_price')}\n"
                f"News digest: {state.get('news_digest')}\n"
                f"定性要点: {qual_notes}\n"
                f"公司基础信息: {basic_info}\n"
                f"主要股东: {holders[:5]}\n"
                f"数据异常提示: {anomalies}\n"
            ),
        },
    ]
    try:
        state["risk_catalyst"] = context.gemini.generate(
            messages,
            web_search=False,
            thinking_budget=context.config.poe_thinking_budget,
        )
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Risk catalyst generation failed: {exc}")
    return state
