"""LangGraph node producing risk and catalyst narratives via LLM."""
from __future__ import annotations

import re

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState
from astock_report.workflows.nodes.llm_clean import clean_llm_output

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
                "请根据以下定量结果与公司背景，生成简洁的风险与催化剂要点（各3-5条，单条不超过2句），"
                "用 Markdown 列表，显式引用来源（定量/新闻/定性/未知）。"
                "仅返回列表，不要任何前缀、客套话或总结句：\n"
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
        raw = context.gemini.generate(
            messages,
            web_search=False,
            thinking_budget=context.config.poe_thinking_budget,
        )
        cleaned = clean_llm_output(raw)
        state["risk_catalyst"] = _normalize_bullets(cleaned)
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"Risk catalyst generation failed: {exc}")
    return state


def _normalize_bullets(text: str) -> str:
    """Keep only bullet lines; drop headings/prefaces and normalize markers."""
    if not text:
        return ""
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    # Find first bullet-like line
    start = 0
    for idx, ln in enumerate(lines):
        if ln.lstrip().startswith(("-", "*", "•")):
            start = idx
            break
    trimmed = lines[start:]
    normalized: list[str] = []
    for ln in trimmed:
        if ln.lstrip().startswith(("#", "**")):
            continue  # skip stray headings/bold banners
        # Convert *, • to -
        normalized_ln = re.sub(r"^\s*[\*\u2022]\s*", "- ", ln)
        normalized_ln = re.sub(r"\*\*(.*?)\*\*", r"\1", normalized_ln)
        normalized.append(normalized_ln)
    return "\n".join(normalized).strip()
