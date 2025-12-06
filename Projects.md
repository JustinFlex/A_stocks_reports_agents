# Projects

> 这是一份“总览导航”，帮你在 1 分钟内搞清楚：这个项目做什么、核心思路是什么、下一步该看哪里。详细策划请看 `ProjectPlan.md`。

## 这个项目在做什么？
- 目标：一键生成 A 股上市公司中文深度研究报告（Markdown，可选 JSON 状态）。
- 形态：本地运行的 CLI 工具 + LangGraph 工作流。
- 数据：SQLite 本地缓存为主，缺数据时通过 TuShare 拉取财报/行情（所有股票数据接口必须严格按 `TushareAPI/TUSHARE_CONFIG.md` 配置调用）。新闻、舆情、非结构化文本与推理一律走 Poe。
- 模型：通过 Poe 接入 Gemini 生成行业、风险、估值解读与整篇报告；可按场景选择 `web_search` 与 `thinking_budget`（默认可由 `POE_WEB_SEARCH`/`POE_THINKING_BUDGET` 提供，或在调用时覆盖）。

- 分层结构：Domain（计算） / Infrastructure（TuShare、SQLite、Gemini） / Workflow（LangGraph 节点） / Presentation（Typer CLI + Markdown 模板）。
- 工作流路径：`ingest_financials → enrich_market` ║ `news_fetch_mapreduce` → `quant_metrics` + `qual_research` → `valuation` → `narrative_writer` → `risk` → `reviewer` → `chart_builder` → `writing` → `qa`（数据流与信息流并行，末端复核 + 图表）。
- 代码入口：`src/astock_report/app/main.py`（CLI）、`src/astock_report/workflows/graph.py`（LangGraph 组装）、`src/astock_report/domain/services/calculations.py`（核心算子）。

- 只想跑一下看效果：按 `README.md` 安装依赖和配置环境变量，然后运行  
  `python -m astock_report.app.main generate 600000.SH --name "SPD Bank" --pdf --pdf-source html`  
  或批量：`python -m astock_report.app.main batch 600000.SH 000001.SZ`，报告默认输出到 `reports/`（Markdown+HTML，若有 pandoc 可同时导出 PDF）。
- 想改数据/指标：先看 `src/astock_report/domain/services/calculations.py` 和 `infrastructure/db/sqlite.py`，再对照 `TushareAPI/TUSHARE_CONFIG.md` 以及本地数据库结构。
- 想改 LLM 推理/搜索策略：调整 `infrastructure/llm/gemini_client.py` 默认参数或在节点调用时传入 `web_search`、`thinking_budget`。
- 想改报告结构/文案：调整 `src/astock_report/reports/templates/base_report.md.j2` 和相关 LangGraph 节点（`workflows/nodes/`）。

## 文档怎么读？
- 只想快速理解项目：看 `README.md` + 本文件（`Projects.md`）。
- 想了解完整产品/技术方案、KPI、里程碑：看 `ProjectPlan.md`。
- 想看分层和依赖规则：看 `docs/architecture.md`。
- 想了解 TuShare 配置、代理和本地接口文档：看 `TushareAPI/README.md` 与 `TushareAPI/TUSHARE_CONFIG.md`。

## 现在做到哪了？
- 已建成：CLI（batch/PDF）、LangGraph 全链（行情/财报/新闻/定性/估值/复核/图表/QA）、TuShare/Gemini 适配、基础计算与单测、CI（ruff/mypy/pytest）。
- 最新进展：Shenwan 行业映射与成分缓存（TuShare `index_classify/index_member_all`）上线，估值使用 sector beta→CAPM WACC 与同业 PE/PB/PS 分位作默认假设；模板透明展示行业基准与分位，HTML/Markdown 估值情景与行业分位均表格化。新闻/定性输出 bullet 化，前缀/空括号/“Learn more” 垃圾清洗完成，脚注统一集中在文末；异常提示汇总成区间；图表扩展（Margin Bridge）。Markdown 头部加入 title/日期元数据以消除 pandoc 警告。
- 当前状态：财报去重+频次标记/TTM 完成；估值/比率护栏生效；叙事 JSON 清洗+重试；QA/复核回写报告；图表年度序列平滑；PB/EV/Sales 自适应；核心观点附评级。
- 下一步重点（详情见 `TODO.md` / `ProjectPlan.md`）：
  1) 估值多模型加权/区间展示、情景表（±WACC/g、±multiples），完善增长假设护栏。
  2) 集成测试与 TuShare/Poe smoke，开发者指南与 TuShare 索引脚本；字体/样式统一。
  3) QA 对引用/估值一致性做硬校验，弱数据自动降权；完善 pandoc 语言包或提示。
