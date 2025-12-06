"""Workflow dependency container."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from config import Config
from astock_report.domain.services.calculations import (
    AnomalyDetector,
    GrowthCalculator,
    RatioCalculator,
    ValuationEngine,
)
from astock_report.infrastructure.sector import SectorService
from astock_report.infrastructure.data_providers.tushare_client import TuShareClient
from astock_report.infrastructure.db.sqlite import SQLiteRepository
from astock_report.infrastructure.llm.gemini_client import GeminiClient


@dataclass
class WorkflowContext:
    """Holds heavy-weight dependencies shared by LangGraph nodes."""

    config: Config
    repository: SQLiteRepository
    tushare: Optional[TuShareClient]
    sector_service: SectorService
    growth_calculator: GrowthCalculator
    ratio_calculator: RatioCalculator
    valuation_engine: ValuationEngine
    anomaly_detector: AnomalyDetector
    gemini: Optional[GeminiClient]

    def close(self) -> None:
        """Release any dependencies that need explicit cleanup."""
        if self.gemini is not None:
            self.gemini.close()
