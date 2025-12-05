"""Reviewer node to cross-check narratives against quantitative outputs."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

SYSTEM_PROMPT = "You are a meticulous equity research reviewer. Write in Chinese."


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    if context.gemini is None:
        logs.append("ReviewerAgent -> skipped (Gemini not configured)")
        return state

    logs.append("ReviewerAgent -> cross-check narratives vs data")
    anomalies = state.get("anomalies") or {}
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "请作为复核人，检查以下内容的一致性并输出：\n"
                "- 量化结果 (growth_curve, ratios, valuation, current_price)\n"
                "- 新闻摘要\n"
                "- 定性要点 (qual_notes)\n"
                "- 叙事段落 (company_intro / industry_analysis / growth_analysis / financial_analysis / valuation_analysis / risk_catalyst)\n"
                "- 数据异常提示 (anomalies)\n"
                "任务：\n"
                "1) 标记数字与文字是否矛盾；\n"
                "2) 新闻或定性引用是否缺来源；\n"
                "3) 若发现严重问题，列出需要重写的段落。\n"
                "输出：Markdown 列表，包含 [通过/警告/错误] + 原因。若全部通过，写“通过，无需修改”。\n"
                f"growth_curve: {state.get('growth_curve')}\n"
                f"ratios: {state.get('ratios')}\n"
                f"valuation: {state.get('valuation')}\n"
                f"current_price: {state.get('current_price')}\n"
                f"news_digest: {state.get('news_digest')}\n"
                f"qual_notes: {state.get('qual_notes')}\n"
                f"anomalies: {anomalies}\n"
                f"company_intro: {state.get('company_intro')}\n"
                f"industry_analysis: {state.get('industry_analysis')}\n"
                f"growth_analysis: {state.get('growth_analysis')}\n"
                f"financial_analysis: {state.get('financial_analysis')}\n"
                f"valuation_analysis: {state.get('valuation_analysis')}\n"
                f"risk_catalyst: {state.get('risk_catalyst')}\n"
            ),
        },
    ]

    try:
        state["review_report"] = context.gemini.generate(
            messages,
            web_search=False,
            thinking_budget=context.config.poe_thinking_budget,
        )
    except Exception as exc:  # pylint: disable=broad-except
        # One retry with slight backoff on transient errors
        try:
            state["review_report"] = context.gemini.generate(
                messages,
                web_search=False,
                thinking_budget=context.config.poe_thinking_budget,
            )
        except Exception:
            errors.append(f"Reviewer failed: {exc}")
    return state
