"""LangGraph workflow assembly for the end-to-end report pipeline."""
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from config import Config
from astock_report.domain.services.calculations import (
    AnomalyDetector,
    GrowthCalculator,
    RatioCalculator,
    ValuationEngine,
)
from astock_report.infrastructure.data_providers.tushare_client import TuShareClient
from astock_report.infrastructure.db.sqlite import SQLiteRepository
from astock_report.infrastructure.llm.gemini_client import GeminiClient
from astock_report.infrastructure.sector import SectorService
from astock_report.workflows import context as context_module
from astock_report.workflows.blueprint import StageSpec, build_default_stages
from astock_report.workflows.nodes import (
    narrative,
    news,
    qa,
    qual_research,
    reviewer,
    valuation,
    writing,
)
from astock_report.workflows.state import ReportState


class ReportWorkflow:
    """Compose LangGraph nodes into a runnable workflow."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._context = self._build_context()
        self._stages: List[StageSpec] = build_default_stages()
        self._graph = self._build_graph()

    def _build_context(self) -> context_module.WorkflowContext:
        repository = SQLiteRepository(
            database_uri=f"sqlite:///{self._config.database_path}",
            echo=self._config.sqlite_echo,
        )
        tushare_client: Optional[TuShareClient]
        try:
            tushare_client = TuShareClient(self._config.tushare_api_key)
        except ValueError:
            tushare_client = None

        gemini_client: Optional[GeminiClient]
        try:
            gemini_client = GeminiClient(
                api_key=self._config.poe_api_key or "",
                model=self._config.gemini_model,
                proxy_url=self._config.proxy_url,
                default_web_search=self._config.poe_web_search,
                default_thinking_budget=self._config.poe_thinking_budget,
            )
        except ValueError:
            gemini_client = None

        return context_module.WorkflowContext(
            config=self._config,
            repository=repository,
            tushare=tushare_client,
            sector_service=SectorService(repository=repository, tushare=tushare_client),
            growth_calculator=GrowthCalculator(),
            ratio_calculator=RatioCalculator(),
            valuation_engine=ValuationEngine(),
            anomaly_detector=AnomalyDetector(),
            gemini=gemini_client,
        )

    def _build_graph(self):
        builder = StateGraph(dict)

        if not self._stages:
            raise RuntimeError("Workflow blueprint is empty; cannot build LangGraph.")

        for stage in self._stages:
            builder.add_node(stage.key, self._wrap(stage.handler))

        # Serialize execution in declared stage order to avoid concurrent state writes.
        builder.set_entry_point(self._stages[0].key)
        for current, nxt in zip(self._stages, self._stages[1:]):
            builder.add_edge(current.key, nxt.key)
        builder.add_edge(self._stages[-1].key, END)

        return builder.compile(checkpointer=None)

    def _wrap(self, func: Callable[[ReportState, context_module.WorkflowContext], ReportState]):
        def wrapper(state: Dict[str, Any]) -> Dict[str, Any]:
            return func(state, self._context)

        return wrapper

    def run(
        self,
        ticker: str,
        company_name: Optional[str] = None,
        *,
        valuation_overrides: Optional[Dict[str, float]] = None,
        llm_overrides: Optional[Dict[str, Any]] = None,
    ) -> ReportState:
        """Execute the workflow for a single ticker."""
        initial_state: ReportState = {
            "ticker": ticker,
            "company_name": company_name,
            "report_date": datetime.utcnow().date().isoformat(),
            "logs": [],
            "errors": [],
            "extras": {},
            "stage_order": [stage.key for stage in self._stages],
        }
        if valuation_overrides:
            initial_state["valuation_overrides"] = valuation_overrides
        if llm_overrides:
            initial_state["llm_overrides"] = llm_overrides
        result: ReportState = self._graph.invoke(initial_state)
        result = self._apply_rerun_hooks(result)
        return result  # type: ignore[return-value]

    def _apply_rerun_hooks(self, state: ReportState) -> ReportState:
        """Apply post-run rewrites (e.g., narrative rerun) and re-render report with QA/review."""

        logs = state.setdefault("logs", [])
        rewrite_requests = state.get("rewrite_requests") or []
        wants_narrative_rerun = any(
            req.get("suggested_action") == "rerun_narrative_node" for req in rewrite_requests
        )
        wants_valuation_rerun = any(
            req.get("suggested_action") == "rerun_valuation_node" for req in rewrite_requests
        )
        wants_news_rerun = any(
            req.get("suggested_action") == "rerun_news_node" for req in rewrite_requests
        )

        if wants_news_rerun:
            logs.append("PostRun -> rerunning news/qual/narrative/reviewer/writing/qa due to news rewrite request")
            state = news.run(state, self._context)
            state = qual_research.run(state, self._context)
            state = narrative.run(state, self._context)
            state = reviewer.run(state, self._context)
            state = writing.run(state, self._context)
            state = qa.run(state, self._context)

        if wants_valuation_rerun:
            logs.append("PostRun -> rerunning valuation/narrative/reviewer/writing/qa due to valuation rewrite request")
            state = valuation.run(state, self._context)
            state = narrative.run(state, self._context)
            state = reviewer.run(state, self._context)
            state = writing.run(state, self._context)
            state = qa.run(state, self._context)

        if wants_narrative_rerun and not wants_valuation_rerun:
            logs.append("PostRun -> rerunning narrative/reviewer/writing/qa due to rewrite request")
            state = narrative.run(state, self._context)
            state = reviewer.run(state, self._context)
            state = writing.run(state, self._context)
            state = qa.run(state, self._context)

        if state.get("qa_report") or state.get("review_report"):
            logs.append("PostRun -> re-rendering Markdown to include QA/Review summaries")
            state = writing.run(state, self._context)

        return state

    def persist_state(self, state: ReportState, path: Path) -> None:
        """Serialize the workflow state to disk for debugging or auditing."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state, default=_json_serializer, indent=2, ensure_ascii=False)
        path.write_text(payload, encoding="utf-8")

    def persist_markdown(self, markdown: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown, encoding="utf-8")

    def persist_html(self, html: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html, encoding="utf-8")

    def describe_stages(self) -> List[str]:
        """Return human-readable workflow stage descriptions."""
        return [f"{stage.key}: {stage.description}" for stage in self._stages]

    def __del__(self) -> None:  # pragma: no cover
        try:
            self._context.close()
        except Exception:  # pylint: disable=broad-except
            pass


def _json_serializer(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "tolist"):
        return value.tolist()
    return str(value)
