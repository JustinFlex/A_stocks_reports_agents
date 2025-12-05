"""Basic smoke tests for the scaffold."""
from __future__ import annotations

from config import Config
from astock_report.workflows.graph import ReportWorkflow


def test_config_defaults(tmp_path):
    cfg = Config.from_env()
    assert cfg.database_path.exists() or cfg.database_path.parent.exists()


def test_workflow_stages():
    cfg = Config.from_env()
    workflow = ReportWorkflow(cfg)
    stages = workflow.describe_stages()
    assert len(stages) >= 8  # topology expanded with parallel branches
    assert stages[0].startswith("ingest_financials")
