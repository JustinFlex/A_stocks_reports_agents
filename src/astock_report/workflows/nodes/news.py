"""LangGraph node to summarize recent news via Poe (Gemini)."""
from __future__ import annotations

import re

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

    overrides = state.get("llm_overrides") or {}
    resolved_web_search = overrides.get("news_web_search")
    if resolved_web_search is None:
        resolved_web_search = True if context.config.poe_web_search is None else context.config.poe_web_search
    resolved_budget = overrides.get("news_thinking_budget")
    if resolved_budget is None:
        resolved_budget = context.config.poe_thinking_budget or 2048

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "检索最近 30 天与下述公司相关的新闻、公告、研报摘要，并返回（直接输出结果，不要任何前言/客套/确认语）：\n"
                "1) Map 阶段：列出 5-8 条事件，包含日期(ISO)、来源(域名或媒体名)、情绪(正/负/中性)、一句话影响点、URL；用 Markdown 列表，格式为 `- [日期][来源][情绪] 事件概述 (URL)`\n"
                "2) Reduce 阶段：合并为 3-5 个催化剂/风险，标注对应来源标签（可多源），缺信息时写“暂无可靠新闻”。格式 `- 主题：结论 (来源: A/B/...)`\n"
                "输出格式：先写 **Map** 段，再写 **Reduce** 段。\n"
                f"公司: {company_name} / {ticker}"
            ),
        },
    ]

    raw_outputs = []
    for attempt in range(1, 3):
        try:
            raw = context.gemini.generate(
                messages,
                web_search=resolved_web_search,
                thinking_budget=resolved_budget,
            )
            raw_outputs.append(raw)
            cleaned = _normalize_news_digest(raw)
            if _valid_news_digest(cleaned):
                state["news_digest"] = cleaned
                break
            logs.append(f"NewsSentimentAgent -> output invalid format on attempt {attempt}, retrying")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"News summarization failed: {exc}")
            break
    else:
        if raw_outputs:
            state["news_digest"] = _normalize_news_digest(raw_outputs[-1])
            errors.append("News summarization returned invalid Map/Reduce format after retries.")

    state.setdefault("extras", {}).setdefault("news_raw", raw_outputs)
    return state


def _normalize_news_digest(text: str) -> str:
    """Strip Poe 'Thinking...' preambles and standardize Map/Reduce headers."""
    if not text:
        return ""
    cleaned = str(text).strip()
    # Drop upfront Thinking/citation boilerplate blocks
    cleaned = re.sub(r"(?is)^\s*\*?Thinking\.\.\.\*?.*?(?:\n{2,}|$)", "", cleaned)
    cleaned = re.sub(r"(?im)^>.*\n", "", cleaned)  # remove leading quoted meta lines
    cleaned = re.sub(r"(?is)^(ok|okay|好的|行的)[^\n]*\n?", "", cleaned)  # drop casual prefixes
    # Harmonize headers to **Map** / **Reduce**
    cleaned = re.sub(r"(?im)^\s*#{1,3}\s*Map\b.*", "**Map**", cleaned, count=1)
    cleaned = re.sub(r"(?im)^\s*Map\s*[:\-]?", "**Map**", cleaned, count=1)
    cleaned = re.sub(r"(?im)^\s*#{1,3}\s*Reduce\b.*", "**Reduce**", cleaned, count=1)
    cleaned = re.sub(r"(?im)^\s*Reduce\s*[:\-]?", "**Reduce**", cleaned, count=1)
    cleaned = re.sub(r"(?is)^.*?(\*\*Map\*\*)", r"**Map**", cleaned, count=1)  # drop any lead-in text before Map
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)  # collapse blank lines
    return cleaned.strip()


def _valid_news_digest(text: str) -> bool:
    normalized = _normalize_news_digest(text)
    if not normalized:
        return False
    if "Thinking" in normalized:
        return False
    has_map = bool(re.search(r"(?i)\*\*\s*map\s*\*\*", normalized))
    has_reduce = bool(re.search(r"(?i)\*\*\s*reduce\s*\*\*", normalized))
    return has_map and has_reduce


def is_valid_news_digest(text: str) -> bool:
    """Public wrapper for QA to reuse validation logic."""
    return _valid_news_digest(text)
