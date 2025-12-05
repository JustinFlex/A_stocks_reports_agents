import pytest

from astock_report.workflows.nodes import news


def test_news_normalization_strips_thinking_and_standardizes_headers():
    raw = (
        "*Thinking...*\n\n"
        "> planning steps\n\n"
        "### Map\n"
        "- [2025-01-01][X][正] 事件概述 (http://example.com)\n\n"
        "### Reduce\n"
        "- 主题：结论 (来源: X)"
    )
    cleaned = news._normalize_news_digest(raw)
    assert "Thinking" not in cleaned
    assert cleaned.startswith("**Map**")
    assert "**Reduce**" in cleaned
    assert news.is_valid_news_digest(raw)


def test_news_validation_requires_reduce_section():
    raw = "**Map**\n- [2025-01-01][X][中性] 事件 (http://example.com)"
    assert not news.is_valid_news_digest(raw)
