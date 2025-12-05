# TODO（按里程碑推进）

## Milestone 1 — 数据基建与拓扑落地
- [x] 定稿 SQLite schema：报表（IS/BS/CF）、行情窗口、股东、基础信息；实现 SQLAlchemy Core upsert。
- [x] TuShare 客户端：封装 `stock_basic`、`fina_indicator_vip`、`top10_holders`、`daily`，加入重试/速率控制/探活；缓存优先。
- [x] 行情窗口缓存：`enrich_market` 写回 SQLite（可选），保留统计字段（min/max/vol）。
- [x] 工作流拓扑：并行数据流（ingest_financials → enrich_market → quant_metrics）与信息流（news_fetch_mapreduce → qual_research）在 valuation 汇合，末端 reviewer + qa。
  - 后续增强：
    - [ ] 补充 TuShare 字段别名覆盖（负债/现金流等），并为标准化编写更多夹具。
    - [x] 财报频次清洗：标记季报/年报，去重取最新公告版，派生年度口径快照（优先 12 月）并保留季度数据以便 TTM/异常检测。
    - [x] 将最新价格/市值持久化到缓存表，离线运行也能得到准确估值。
    - [ ] 增加持股/基本面信息在估值和模板中的呈现。

## Milestone 2 — 量化与估值引擎
- [x] `quant_metrics`：pandas 实现 CAGR/YoY/趋势、25+ 比率、异常检测（>30% 波动标记）；单元测试夹具。
- [x] 估值引擎：完成 DCF、PE/EV Band，暴露假设（WACC、g、multiple 分位）给模板；缺失/除零保护。
- [x] 提取 price anchor（现价/波动）供估值与叙事调用，并将最新价/市值持久化以便离线估值。
  - 后续增强：
    - [x] YoY/CAGR 口径修正：基于年度或 TTM，同期比（季度→季度、年度→年度），过滤重复披露，补充单元测试。
    - [ ] 调整估值假设（WACC/g/multiple 分位）入模板和 CLI 参数，暴露敏感性。
    - [x] 估值引擎重构：使用年度/TTM FCF/EPS/EBITDA，处理负值/极端输入（例如 pe_band 需有效 EPS 基准），将数值计算与 LLM 叙事拆分并增加场景测试。
    - [x] 将异常提示、定性要点与估值结果在模板中联动（量化/估值提示区分，补充低基数/杠杆覆盖说明）。

## Milestone 3 — 信息流、叙事与复核
- [x] `news_fetch_mapreduce`：Map（逐条摘要含日期/来源/情绪/链接）→ Reduce（3-5 条催化剂）；强制 `web_search=True`、记录 `thinking_budget`，输出含来源格式。
- [x] `qual_research`：行业/同业/政策/催化剂总结，引用来源；缺失时写“未知/暂无”。
- [x] `narrative_writer`：Poe 无搜索，输出严格 JSON（company/industry/growth/financial/valuation/viewpoints）；JSON 校验与重试。
- [x] `reviewer`：Poe 复核一致性/合规（数字与结论匹配、引用来源）；加入一次重试并记录 review_report。
- [x] `writing`：Jinja 模板插入量化结果、防覆盖；接收 anomalies/qual/news/review。
- [x] `qa`：缺失/错误检查表（financials/price/news/qual/sections/report/anomalies/review），包含引用格式粗检。
  - 后续增强：
    - [ ] 为 Reviewer/QA 增加引用正则检查与严重性分级；必要时触发重写。
    - [ ] QA 对估值/比率一致性做硬校验（如 pe_band 需有效 PE/EPS 基准、负估值且现价为正时标红并触发 rerun/rewrite；负内在价值可降级为警告）。
    - [ ] 为 news/qual 增加 WebSearch/ThinkingBudget 参数化入口。
    - [ ] 叙事完整性与篇幅平衡：对 company/industry/growth/financial/valuation/qual_notes/risk_catalyst/anomalies 设置最小字数或占比检查，缺失或过短时自动触发重写或补齐，并在 QA 报告中标注原因。
    - [x] 将 review/QA 结果回写到报告末尾或单独附件（Markdown/HTML 均已附尾部）。
    - [x] 修复 Narrative JSON 解析/落盘：保证 company_intro/industry/growth/financial/valuation 段落填充成功，必要时做 JSON 清洗与重试，避免报告段落为 None。
    - [x] 在 QA 中增加叙事缺失的明确提示与可选重试钩子。
    - [x] 风险与催化剂输出：移除前缀/标题杂质，统一为简短 bullets。

## Milestone 4 — 图表、体验与工程化
- [x] `chart_builder`：matplotlib 生成价格、营收/净利趋势图，写入 state 并在模板引用。
- [x] CLI 体验：进度提示、batch、PDF 导出（pandoc 可选）。
- [x] 引入 `ruff`、`mypy`，配置 CI（pytest + lint/type check），提供 `.env.example`。
- [ ] 集成测试：固定 SQLite/CSV 示例驱动 workflow，验证节点输出稳定；记录黄金值。
- [x] 提供 `.env.example`，记录必需 env（TuShare/Poe/代理/思考预算）。
- [ ] Poe/TuShare smoke：更新 `tools/api_smoke_test.py` 覆盖新增参数和新闻调用。
- [ ] 更新开发者指南：并行拓扑、节点职责、Poe 参数（web_search/thinking_budget）、TuShare 配置。
- [ ] 为 `TushareAPI/` 增加索引/检索脚本（快速查字段/接口）。
- [ ] 图表字体与国际化：确保标题/图例英文，避免中文字体缺失警告。
- [x] 提供 HTML 报告及 PDF 源选择（md/html），并在 HTML 模板中加入打印友好主题。
  - 后续增强：
    - [x] 图表频次清洗：年度序列一条/年，季度单独聚合或 TTM，避免同年多点“锯齿”；补回归测试。

## Milestone 5 — 估值多方法与稳健性
- [x] 增补估值方法：已加入 PB/EV/Sales 动态分位（亏损时收敛低倍），NAV/同业分位待定。
- [ ] 估值结果汇总：对多模型结果做加权/区间展示，并在模板中明确假设来源与适用场景。
- [ ] 极端场景护栏：对亏损/高杠杆公司给出保底估值逻辑（如情景重组、资产清算），并降级异常输出为警告而非阻断。
- [ ] DCF 假设来源化：将 g/WACC/gt 参数外化（配置/CLI/模板），引入行业基准/宏观长增速作为默认来源，避免硬编码。
