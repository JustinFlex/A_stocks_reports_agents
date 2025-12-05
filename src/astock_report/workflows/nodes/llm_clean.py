"""Helpers to scrub Poe/Gemini planning chatter from model outputs."""
from __future__ import annotations

import re


def clean_llm_output(text: str) -> str:
    """Remove 'Thinking.../Planning' scaffolding and leading quotes."""
    if not text:
        return ""
    cleaned = str(text).strip()
    cleaned = re.sub(r"(?is)^\s*\*?(?:Thinking|Planning)[^.]*\*?.*?(?:\n{2,}|$)", "", cleaned)
    cleaned = re.sub(r"(?im)^>.*\n", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()
