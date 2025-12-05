# Architecture Overview

## Pattern
The system follows a layered, hexagon-inspired architecture that separates domain calculations from infrastructure and presentation concerns. LangGraph orchestrates Agent nodes defined in src/astock_report/workflows/nodes while the CLI entrypoint lives in src/astock_report/cli.

## Layers
- **Domain**: pure financial models and calculation services.
- **Infrastructure**: gateways for SQLite, TuShare, and Gemini.
- **Workflow**: LangGraph topology plus dependency container.
- **Presentation**: Typer CLI and Markdown rendering.

## Dependency Rules
- Domain is dependency-free and unaware of infrastructure.
- Infrastructure may depend on external SDKs but never on CLI.
- Workflow stitches dependencies together and exposes a facade to the CLI.
- Presentation interacts with workflow only, keeping commands thin.

## Current Workflow Topology
并行数据/信息流，末端复核与图表：`ingest_financials → enrich_market` ║ `news_fetch_mapreduce → qual_research` → `quant_metrics` + `qual_research` → `valuation` → `narrative_writer` → `risk` → `reviewer` → `chart_builder` → `writing` → `qa`。写作层现生成 Markdown+HTML，HTML 模板包含打印友好主题并可作为 PDF 源。
