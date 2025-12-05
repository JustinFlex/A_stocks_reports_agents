"""LangGraph node to craft narrative sections from quantitative outputs."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

SYSTEM_PROMPT = (
    "You are a CFA-level equity research analyst. "
    "Write concise, investment-grade Chinese narratives grounded strictly on provided data."
)

OUTPUT_KEYS = [
    "company_intro",
    "industry_analysis",
    "growth_analysis",
    "financial_analysis",
    "valuation_analysis",
    "core_viewpoints",
]

SECTION_LABELS = {
    "company_intro": "公司简介",
    "industry_analysis": "行业分析",
    "growth_analysis": "成长性",
    "financial_analysis": "财务分析",
    "valuation_analysis": "估值解读",
    "core_viewpoints": "核心观点",
}

MAX_NARRATIVE_ATTEMPTS = 3


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])

    if context.gemini is None:
        logs.append("NarrativeAgent -> skipped (Gemini client not configured)")
        for key in OUTPUT_KEYS:
            state.setdefault(key, "(未配置Gemini，待完善)")
        return state

    ticker = state.get("ticker")
    company_name = state.get("company_name") or ""
    logs.append("NarrativeAgent -> generate structured sections via Gemini")

    price_preview = (state.get("price_history") or [])[:3]
    qual_notes = state.get("qual_notes") or "(暂无定性摘要)"
    basic_info = state.get("basic_info") or {}
    holders = state.get("holders") or []
    anomalies = state.get("anomalies") or {}

    base_prompt = (
        "根据以下量化输出与新闻摘要，生成 JSON，键包含：\n"
        "company_intro, industry_analysis, growth_analysis, financial_analysis, "
        "valuation_analysis, core_viewpoints。每段 120-200 字，保持事实严谨，勿虚构数据；"
        "行业分析必须包含主要竞争对手及对比要点；适当引用新闻摘要中的来源信息。"
        "请严格输出单个 JSON 对象，不要包含额外文本或代码块。\n"
        f"Ticker: {ticker}, 公司: {company_name}\n"
        f"价格窗口: {state.get('current_price')} / {price_preview}\n"
        f"成长性: {state.get('growth_curve')}\n"
        f"财务比率: {state.get('ratios')}\n"
        f"估值结果: {state.get('valuation')}\n"
        f"新闻摘要: {state.get('news_digest')}\n"
        f"定性要点: {qual_notes}\n"
        f"公司基础信息: {basic_info}\n"
        f"主要股东: {holders[:5]}\n"
        f"数据异常提示: {anomalies}\n"
    )

    parsed: Dict[str, str] = {}
    missing_sections: List[str] = OUTPUT_KEYS.copy()
    raw_outputs: List[str] = []

    for attempt in range(1, MAX_NARRATIVE_ATTEMPTS + 1):
        prompt_suffix = ""
        if attempt > 1:
            prompt_suffix = (
                "\n上次输出缺失字段或无法解析，请重新返回完整 JSON。"
                f"必填字段: {', '.join(OUTPUT_KEYS)}。"
                "仅返回 JSON，不要添加代码块标记或多余文字。"
            )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": base_prompt + prompt_suffix},
        ]

        try:
            raw = context.gemini.generate(
                messages,
                web_search=False,
                thinking_budget=context.config.poe_thinking_budget,
            )
            raw_outputs.append(raw)
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"Narrative generation failed (attempt {attempt}): {exc}")
            continue

        try:
            parsed_candidate = _parse_json_response(raw)
        except ValueError:
            logs.append("NarrativeAgent -> JSON parse failed; will retry")
            continue

        cleaned = _normalize_sections(parsed_candidate)
        missing_sections = [k for k in OUTPUT_KEYS if k not in cleaned or not cleaned[k]]
        parsed = cleaned
        if not missing_sections:
            break
        logs.append(
            f"NarrativeAgent -> missing sections after attempt {attempt}: {missing_sections}"
        )

    state.setdefault("extras", {}).setdefault("narrative_raw", raw_outputs)

    if missing_sections:
        errors.append(
            "Narrative sections incomplete after retries: " + ", ".join(missing_sections)
        )

    for key in OUTPUT_KEYS:
        value = parsed.get(key)
        state[key] = value or _fallback_for(key)
    state["narrative_missing_sections"] = missing_sections
    return state


def _parse_json_response(raw: str) -> Dict[str, Any]:
    """Parse Gemini output into JSON with lightweight cleanup."""

    candidates = [raw]
    fenced = _strip_code_fences(raw)
    if fenced != raw:
        candidates.append(fenced)
    braced = _extract_braced_block(raw)
    if braced not in candidates:
        candidates.append(braced)

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            if isinstance(parsed, dict):
                return parsed
        except Exception:  # pylint: disable=broad-except
            continue
    raise ValueError("Unable to parse narrative JSON response.")


def _strip_code_fences(text: str) -> str:
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        return match.group(1)
    return text.strip()


def _extract_braced_block(text: str) -> str:
    stripped = text.strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        return stripped[start : end + 1]
    return stripped


def _normalize_sections(payload: Dict[str, Any]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key in OUTPUT_KEYS:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (dict, list)):
            normalized[key] = json.dumps(value, ensure_ascii=False)
        else:
            normalized[key] = str(value).strip()
    return {k: v for k, v in normalized.items() if v}


def _fallback_for(key: str) -> str:
    label = SECTION_LABELS.get(key, key)
    return f"{label} 待补充（模型未返回有效内容）"
