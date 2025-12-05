"""LLM gateway for Gemini access via the OpenAI-compatible Poe API."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx
from openai import OpenAI


class GeminiClient:
    """Minimal Gemini client hiding transport plumbing from workflow nodes."""

    def __init__(
        self,
        api_key: str,
        model: str,
        *,
        proxy_url: Optional[str] = None,
        timeout: float = 60.0,
        default_web_search: Optional[bool] = None,
        default_thinking_budget: Optional[int] = None,
    ) -> None:
        if not api_key:
            raise ValueError("POE_API_KEY is required to contact Gemini endpoints.")

        http_client_kwargs: Dict[str, Any] = {
            "timeout": httpx.Timeout(timeout, connect=10.0),
            "verify": True,
        }
        if proxy_url:
            http_client_kwargs["proxy"] = proxy_url
            http_client_kwargs["verify"] = False

        self._http_client = httpx.Client(**http_client_kwargs)
        self._client = OpenAI(
            api_key=api_key,
            base_url="https://api.poe.com/v1",
            http_client=self._http_client,
        )
        self._model = model
        self._default_web_search = default_web_search
        self._default_thinking_budget = default_thinking_budget

    def generate(
        self,
        messages: List[Dict[str, str]],
        *,
        temperature: float = 0.2,
        web_search: Optional[bool] = None,
        thinking_budget: Optional[int] = None,
    ) -> str:
        """Fire a chat completion request and return the assistant message content."""
        resolved_web_search = (
            self._default_web_search if web_search is None else web_search
        )
        resolved_budget = (
            self._default_thinking_budget if thinking_budget is None else thinking_budget
        )

        extra_body: Dict[str, Any] = {}
        if resolved_web_search is not None:
            extra_body["web_search"] = bool(resolved_web_search)
        if resolved_budget is not None:
            extra_body["thinking_budget"] = resolved_budget

        response = self._client.chat.completions.create(
            model=self._model,
            temperature=temperature,
            messages=messages,
            extra_body=extra_body or None,
        )
        if not response.choices:
            raise RuntimeError("Gemini returned no choices.")
        return response.choices[0].message.content or ""

    def close(self) -> None:
        """Release the underlying HTTP session."""
        self._http_client.close()
