"""LangGraph node to summarize recent news via Poe (Gemini)."""
from __future__ import annotations

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

SYSTEM_PROMPT = "You are a precise financial news summarizer writing in Chinese. Cite sources." 


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    if context.gemini is None:
        logs.append("NewsSentimentAgent -> skipped (Gemini client not configured)")
        state["news_digest"] = "(未配置Gemini，跳过新闻摘要)"
        return state

    ticker = state.get("ticker")
    company_name = state.get("company_name") or ""
    logs.append("NewsSentimentAgent -> fetch and summarize latest public news (MapReduce)")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "检索最近 30 天与下述公司相关的新闻、公告、研报摘要，并返回：\n"
                "1) Map 阶段：列出 5-8 条事件，包含日期(ISO)、来源(域名或媒体名)、情绪(正/负/中性)、一句话影响点、URL；用 Markdown 列表，格式为 `- [日期][来源][情绪] 事件概述 (URL)`\n"
                "2) Reduce 阶段：合并为 3-5 个催化剂/风险，标注对应来源标签（可多源），缺信息时写“暂无可靠新闻”。格式 `- 主题：结论 (来源: A/B/...)`\n"
                "输出格式：先写 **Map** 段，再写 **Reduce** 段。\n"
                f"公司: {company_name} / {ticker}"
            ),
        },
    ]

    try:
        state["news_digest"] = context.gemini.generate(
            messages,
            web_search=True,
            thinking_budget=context.config.poe_thinking_budget or 2048,
        )
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"News summarization failed: {exc}")
    return state
