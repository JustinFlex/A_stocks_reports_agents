"""LangGraph node generating qualitative industry/peer/catalyst notes."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

SYSTEM_PROMPT = "You are a concise equity research assistant writing in Chinese."


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    if context.gemini is None:
        logs.append("QualResearchAgent -> skipped (Gemini client not configured)")
        state["qual_notes"] = "(未配置Gemini，跳过定性研究)"
        return state

    news_digest = state.get("news_digest") or "(无新闻摘要)"
    basic = state.get("basic_info") or {}

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "基于以下公司信息与新闻摘要，生成定性研究要点，并在缺失时明确写“未知/暂无”：\n"
                "1) 行业与竞争格局（含主要对手与差异化）\n"
                "2) 近期催化剂与政策/宏观影响\n"
                "3) 需要关注的经营风险或不确定性\n"
                "输出为简短 Markdown 列表，格式 `- 要点 (来源: 新闻/常识/未知)`；缺失时写“未知/暂无”并标注来源=未知。\n"
                f"公司信息: {basic}\n"
                f"新闻摘要: {news_digest}\n"
            ),
        },
    ]

    try:
        state["qual_notes"] = context.gemini.generate(
            messages,
            web_search=True,
            thinking_budget=context.config.poe_thinking_budget,
        )
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Qual research failed: {exc}")
    return state
