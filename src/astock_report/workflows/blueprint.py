"""Workflow blueprint describing agent stages and their handlers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, TYPE_CHECKING

from astock_report.workflows.nodes import (
    data_load,
    narrative,
    news,
    price_enrich,
    qa,
    qual_research,
    quant_metrics,
    reviewer,
    risk,
    valuation,
    writing,
    chart_builder,
)

if TYPE_CHECKING:
    from astock_report.workflows.context import WorkflowContext
    from astock_report.workflows.state import ReportState


@dataclass
class StageSpec:
    """Single LangGraph stage definition."""

    key: str
    description: str
    handler: Callable[["ReportState", "WorkflowContext"], "ReportState"]
    depends_on: List[str] = field(default_factory=list)


def build_default_stages() -> List[StageSpec]:
    """Return the ordered stages for the report workflow."""
    return [
        StageSpec(
            key="ingest_financials",
            description="Load statements from SQLite; fallback to TuShare if cache missing.",
            handler=data_load.run,
        ),
        StageSpec(
            key="enrich_market",
            description="Pull recent price/volume window to anchor valuation and narratives.",
            handler=price_enrich.run,
            depends_on=["ingest_financials"],
        ),
        StageSpec(
            key="quant_metrics",
            description="Compute growth and ratios needed for valuation (pandas-based).",
            handler=quant_metrics.run,
            depends_on=["enrich_market"],
        ),
        StageSpec(
            key="news_fetch_mapreduce",
            description="Use Poe (Gemini) with web_search to summarize latest news and catalysts.",
            handler=news.run,
            depends_on=["ingest_financials"],
        ),
        StageSpec(
            key="qual_research",
            description="Industry/peer/catalyst qualitative summary informed by news/basic info.",
            handler=qual_research.run,
            depends_on=["news_fetch_mapreduce"],
        ),
        StageSpec(
            key="valuation",
            description="Run DCF and relative valuation models to produce fair value bands.",
            handler=valuation.run,
            depends_on=["quant_metrics", "qual_research"],
        ),
        StageSpec(
            key="narrative",
            description="LLM-generate company intro, industry view, growth/financial narratives.",
            handler=narrative.run,
            depends_on=["valuation"],
        ),
        StageSpec(
            key="risk",
            description="LLM-generate risk and catalyst section grounded on metrics and news.",
            handler=risk.run,
            depends_on=["valuation"],
        ),
        StageSpec(
            key="reviewer",
            description="LLM reviewer to cross-check consistency and flag issues.",
            handler=reviewer.run,
            depends_on=["narrative", "risk"],
        ),
        StageSpec(
            key="chart_builder",
            description="Generate price and revenue/net income charts for embedding.",
            handler=chart_builder.run,
            depends_on=["valuation"],
        ),
        StageSpec(
            key="writing",
            description="Render the final Markdown report with all upstream outputs.",
            handler=writing.run,
            depends_on=["narrative", "risk", "reviewer", "chart_builder"],
        ),
        StageSpec(
            key="qa",
            description="Lightweight QA to flag missing sections before handing off.",
            handler=qa.run,
            depends_on=["writing", "reviewer"],
        ),
    ]
