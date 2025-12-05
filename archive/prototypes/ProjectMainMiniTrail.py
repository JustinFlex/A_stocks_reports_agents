"""
A股上市公司深度报告自动生成系统 - LangGraph工作流 (Enhanced with Gemini API)
Author: Assistant
Date: 2025-09
"""

import os
import json
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, TypedDict, Optional
from enum import Enum
import warnings
import sys
warnings.filterwarnings('ignore')

# LangGraph imports
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import ToolNode

# OpenAI imports for Poe API
import openai
import httpx

# Tushare imports
import tushare as ts

# ===========================
# 配置部分
# ===========================

class Config:
    """系统配置"""
    # 数据库路径
    DB_PATH = r'D:\TushareData\fina_indicator_complete_data.db'
    
    # API配置
    POE_API_KEY = os.getenv("POE_API_KEY")
    TUSHARE_API_KEY = os.getenv("TUSHARE_API_KEY")
    
    # 代理配置
    PROXY_URL = "http://127.0.0.1:10808"
    
    # 模型配置
    MODEL_NAME = "Gemini-2.5-Flash-Lite"
    
    # 报告输出路径
    OUTPUT_DIR = "./reports"

# ===========================
# 状态定义
# ===========================

class WorkflowState(TypedDict):
    """工作流状态定义"""
    # 基础信息
    ticker: str
    company_name: Optional[str]
    report_date: str
    current_price: Optional[float]
    
    # 数据载体
    financials: Optional[pd.DataFrame]
    growth_curve: Optional[Dict]
    ratios: Optional[Dict]
    valuation: Optional[Dict]
    
    # 文本内容
    company_intro: Optional[str]
    industry_analysis: Optional[str]
    growth_analysis: Optional[str]
    financial_analysis: Optional[str]
    valuation_analysis: Optional[str]
    risk_catalyst: Optional[str]
    core_viewpoints: Optional[str]
    
    # 最终报告
    markdown_report: Optional[str]
    
    # 错误和日志
    errors: List[str]
    logs: List[str]

# ===========================
# API客户端初始化
# ===========================

class APIClients:
    """API客户端管理"""
    
    @staticmethod
    def init_poe_client():
        """初始化Poe API客户端"""
        http_client = httpx.Client(
            proxies={
                "http://": Config.PROXY_URL,
                "https://": Config.PROXY_URL
            },
            timeout=httpx.Timeout(60.0, connect=10.0),
            verify=False
        )
        
        return openai.OpenAI(
            api_key=Config.POE_API_KEY,
            base_url="https://api.poe.com/v1",
            http_client=http_client
        )
    
    @staticmethod
    def init_tushare_api():
        """初始化Tushare API"""
        if not Config.TUSHARE_API_KEY:
            raise ValueError("请设置环境变量 TUSHARE_API_KEY")
        return ts.pro_api(Config.TUSHARE_API_KEY)

# ===========================
# Agent 节点定义
# ===========================

class DataLoadAgent:
    """数据加载Agent - 从SQLite读取财务数据"""
    
    def __init__(self):
        self.db_path = Config.DB_PATH
        self.tushare_api = APIClients.init_tushare_api()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """执行数据加载"""
        try:
            ticker = state['ticker']
            state['logs'].append(f"开始加载 {ticker} 的财务数据...")
            
            # 从SQLite读取数据
            conn = sqlite3.connect(self.db_path)
            
            # 查询最近12个季度的财务数据
            query = """
                SELECT * FROM fina_indicator_complete 
                WHERE ts_code = ?
                ORDER BY end_date DESC
                LIMIT 12
            """
            
            df = pd.read_sql(query, conn, params=[ticker])
            conn.close()
            
            if df.empty:
                # 如果本地没有，尝试从Tushare获取
                state['logs'].append(f"本地无数据，从Tushare获取...")
                df = self._fetch_from_tushare(ticker)
            
            # 获取当前股价（简化版，实际应从行情接口获取）
            state['current_price'] = self._get_current_price(ticker)
            
            state['financials'] = df
            state['logs'].append(f"成功加载 {len(df)} 条财务数据")
            
        except Exception as e:
            state['errors'].append(f"DataLoadAgent错误: {str(e)}")
            state['logs'].append(f"数据加载失败: {str(e)}")
        
        return state
    
    def _fetch_from_tushare(self, ticker: str) -> pd.DataFrame:
        """从Tushare获取数据"""
        try:
            df = self.tushare_api.fina_indicator(
                ts_code=ticker,
                start_date='20200101'
            )
            return df
        except Exception as e:
            print(f"Tushare获取失败: {e}")
            return pd.DataFrame()
    
    def _get_current_price(self, ticker: str) -> float:
        """获取当前股价（简化版）"""
        try:
            # 这里应该调用实时行情接口
            # 暂时返回模拟值
            return 10.0
        except:
            return 10.0

class CompanyIntroAgent:
    """公司简介Agent - 调用Gemini生成公司介绍"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """生成公司简介"""
        try:
            state['logs'].append("开始生成公司简介...")
            
            prompt = f"""
你是一位专业的证券分析师，请为以下公司生成详细的公司简介：

公司代码：{state['ticker']}
公司名称：{state['company_name']}

请包含以下内容：
1. 公司基本信息（成立时间、上市时间、注册地等）
2. 主营业务描述（核心产品/服务、业务结构）
3. 行业地位（市场份额、竞争优势）
4. 发展历程中的重要事件
5. 管理团队核心成员

要求：
- 内容准确、客观、专业
- 字数控制在300-500字
- 使用结构化的段落，不要使用列表
"""
            
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位专业的A股证券分析师，擅长撰写公司研究报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5000
            )
            
            state['company_intro'] = response.choices[0].message.content
            state['logs'].append("公司简介生成完成")
            
        except Exception as e:
            state['errors'].append(f"CompanyIntroAgent错误: {str(e)}")
            state['company_intro'] = f"{state['company_name']}是一家上市公司，股票代码{state['ticker']}。"
        
        return state

class IndustryAnalysisAgent:
    """行业分析Agent - 调用Gemini进行行业分析"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """生成行业与竞争格局分析"""
        try:
            state['logs'].append("开始生成行业分析...")
            
            prompt = f"""
你是一位专业的行业研究员，请为以下公司进行行业与竞争格局分析：

公司：{state['company_name']}（{state['ticker']}）

请提供以下分析内容：

1. 行业概况
   - 行业规模与增长率（提供具体数据）
   - 行业发展阶段（成长期/成熟期/衰退期）
   - 技术发展趋势
   - 上下游产业链分析

2. 政策环境
   - 主要政策法规
   - 政策导向与影响
   - 未来政策预期

3. 竞争格局
   - 主要竞争对手（列举3-5家）
   - 市场份额分布
   - 竞争优势分析
   - 进入壁垒评估

4. 发展趋势
   - 行业未来3-5年发展预测
   - 主要机会与挑战

要求：
- 结合最新的行业数据和信息
- 分析深入、逻辑清晰
- 字数控制在600-800字
- 使用专业术语，但确保易于理解
"""
            
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位资深的行业研究专家，精通A股各行业的发展状况。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5500
            )
            
            state['industry_analysis'] = response.choices[0].message.content
            state['logs'].append("行业分析生成完成")
            
        except Exception as e:
            state['errors'].append(f"IndustryAnalysisAgent错误: {str(e)}")
            state['industry_analysis'] = "行业分析暂时无法生成。"
        
        return state

class GrowthCurveAgent:
    """增长曲线分析Agent - 计算后调用Gemini分析"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """计算增长曲线并分析"""
        try:
            df = state['financials']
            if df is None or df.empty:
                state['errors'].append("GrowthCurveAgent: 无财务数据")
                return state
            
            state['logs'].append("开始计算增长曲线...")
            
            # 计算关键增长指标
            growth_metrics = self._calculate_growth_metrics(df)
            state['growth_curve'] = growth_metrics
            
            # 调用Gemini进行分析
            analysis = self._analyze_growth_with_gemini(growth_metrics, state)
            state['growth_analysis'] = analysis
            
            state['logs'].append("增长曲线分析完成")
            
        except Exception as e:
            state['errors'].append(f"GrowthCurveAgent错误: {str(e)}")
            state['logs'].append(f"增长曲线分析失败: {str(e)}")
        
        return state
    
    def _calculate_growth_metrics(self, df: pd.DataFrame) -> Dict:
        """计算增长指标"""
        growth_metrics = {
            'revenue_growth': [],
            'profit_growth': [],
            'eps_growth': [],
            'periods': [],
            'revenue_values': [],
            'profit_values': []
        }
        
        # 提取数据
        for _, row in df.head(8).iterrows():
            growth_metrics['periods'].append(row['end_date'])
            growth_metrics['revenue_growth'].append(row.get('or_yoy', 0))
            growth_metrics['profit_growth'].append(row.get('netprofit_yoy', 0))
            growth_metrics['eps_growth'].append(row.get('basic_eps_yoy', 0))
            growth_metrics['revenue_values'].append(row.get('revenue', 0))
            growth_metrics['profit_values'].append(row.get('n_income', 0))
        
        # 计算复合增长率
        if len(growth_metrics['revenue_growth']) >= 4:
            growth_metrics['revenue_cagr'] = self._calculate_cagr(growth_metrics['revenue_growth'][:4])
            growth_metrics['profit_cagr'] = self._calculate_cagr(growth_metrics['profit_growth'][:4])
        
        return growth_metrics
    
    def _calculate_cagr(self, growth_rates: List[float]) -> float:
        """计算复合年增长率"""
        try:
            n = len(growth_rates)
            if n == 0:
                return 0
            
            total_growth = 1
            for rate in growth_rates:
                if rate and rate != 0:
                    total_growth *= (1 + rate/100)
            
            cagr = (total_growth ** (1/n) - 1) * 100
            return round(cagr, 2)
        except:
            return 0
    
    def _analyze_growth_with_gemini(self, metrics: Dict, state: WorkflowState) -> str:
        """使用Gemini分析增长数据"""
        try:
            # 准备数据摘要
            data_summary = f"""
公司：{state['company_name']}（{state['ticker']}）

增长数据：
1. 最近4个季度营收同比增长率：{metrics['revenue_growth'][:4]}
2. 最近4个季度净利润同比增长率：{metrics['profit_growth'][:4]}
3. 营收复合增长率(CAGR)：{metrics.get('revenue_cagr', 'N/A')}%
4. 净利润复合增长率：{metrics.get('profit_cagr', 'N/A')}%
5. 报告期：{metrics['periods'][:4]}
"""
            
            prompt = f"""
基于以下增长数据，请进行深入的增长分析：

{data_summary}

请分析：
1. 增长趋势评估
   - 增长的稳定性和可持续性
   - 增长率的变化趋势（加速/减速）
   - 与行业平均水平的对比

2. 增长驱动因素
   - 内生增长动力（产品创新、市场扩张等）
   - 外部因素（行业景气度、政策支持等）
   - 主要增长来源分析

3. 增长质量评估
   - 营收与利润增长的匹配度
   - 增长的健康度评分
   - 潜在的增长风险

4. 未来增长预期
   - 短期（1年）增长预测
   - 中长期（3-5年）增长展望

要求：
- 结合公司实际情况和行业特点
- 提供具体的分析依据
- 字数500-700字
"""
            
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位专业的财务分析师，擅长企业成长性分析。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5200
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"增长分析生成失败：{str(e)}"

class RatioCalcAgent:
    """财务比率计算与分析Agent"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """计算财务比率并分析"""
        try:
            df = state['financials']
            if df is None or df.empty:
                state['errors'].append("RatioCalcAgent: 无财务数据")
                return state
            
            state['logs'].append("开始计算财务比率...")
            
            # 计算财务比率
            ratios = self._calculate_ratios(df)
            state['ratios'] = ratios
            
            # 调用Gemini进行财务分析
            analysis = self._analyze_financials_with_gemini(ratios, state)
            state['financial_analysis'] = analysis
            
            state['logs'].append("财务分析完成")
            
        except Exception as e:
            state['errors'].append(f"RatioCalcAgent错误: {str(e)}")
            state['logs'].append(f"财务分析失败: {str(e)}")
        
        return state
    
    def _calculate_ratios(self, df: pd.DataFrame) -> Dict:
        """计算财务比率"""
        latest = df.iloc[0]
        
        ratios = {
            'profitability': {
                'roe': latest.get('roe', 0),
                'roa': latest.get('roa', 0),
                'roic': latest.get('roic', 0),
                'gross_margin': latest.get('grossprofit_margin', 0),
                'net_margin': latest.get('netprofit_margin', 0),
                'ebitda_margin': latest.get('ebitda', 0) / latest.get('revenue', 1) * 100 if latest.get('revenue', 0) > 0 else 0
            },
            'efficiency': {
                'asset_turnover': latest.get('assets_turn', 0),
                'inventory_days': latest.get('invturn_days', 0),
                'receivable_days': latest.get('arturn_days', 0),
                'payable_days': latest.get('turn_days', 0)
            },
            'liquidity': {
                'current_ratio': latest.get('current_ratio', 0),
                'quick_ratio': latest.get('quick_ratio', 0),
                'cash_ratio': latest.get('cash_ratio', 0),
                'ocf_to_current_liab': latest.get('ocf_to_shortdebt', 0)
            },
            'leverage': {
                'debt_to_assets': latest.get('debt_to_assets', 0),
                'debt_to_equity': latest.get('debt_to_eqt', 0),
                'interest_coverage': latest.get('ebit_to_interest', 0),
                'equity_multiplier': latest.get('em', 0)
            },
            'cash_flow': {
                'ocf_to_revenue': latest.get('ocf_to_or', 0),
                'ocf_to_profit': latest.get('ocf_to_netprofit', 0),
                'fcf': latest.get('fcff', 0),
                'cash_to_revenue': latest.get('cash_to_liqdebt', 0)
            }
        }
        
        # 计算趋势
        ratios['trends'] = self._calculate_trends(df)
        
        return ratios
    
    def _calculate_trends(self, df: pd.DataFrame) -> Dict:
        """计算关键指标趋势"""
        trends = {}
        
        # ROE趋势
        if 'roe' in df.columns:
            trends['roe_trend'] = df['roe'].head(4).tolist()
        
        # 毛利率趋势
        if 'grossprofit_margin' in df.columns:
            trends['margin_trend'] = df['grossprofit_margin'].head(4).tolist()
        
        # 资产负债率趋势
        if 'debt_to_assets' in df.columns:
            trends['debt_trend'] = df['debt_to_assets'].head(4).tolist()
        
        return trends
    def _safe_format(self, value, format_str=".2f", default=0):
        """安全格式化数值，处理None值"""
        if value is None:
            return f"{default:{format_str}}"
        try:
            return f"{value:{format_str}}"
        except (ValueError, TypeError):
            return f"{default:{format_str}}"

    def _analyze_financials_with_gemini(self, ratios: Dict, state: WorkflowState) -> str:
        """使用Gemini分析财务数据"""
        try:
            # 准备财务数据摘要
            data_summary = f"""
    公司：{state['company_name']}（{state['ticker']}）
    
    财务指标：
    【盈利能力】
    - ROE: {self._safe_format(ratios['profitability']['roe'])}%
    - ROA: {self._safe_format(ratios['profitability']['roa'])}%
    - 毛利率: {self._safe_format(ratios['profitability']['gross_margin'])}%
    - 净利率: {self._safe_format(ratios['profitability']['net_margin'])}%
    
    【运营效率】
    - 总资产周转率: {self._safe_format(ratios['efficiency']['asset_turnover'])}
    - 存货周转天数: {self._safe_format(ratios['efficiency']['inventory_days'], '.1f')}天
    - 应收账款周转天数: {self._safe_format(ratios['efficiency']['receivable_days'], '.1f')}天
    
    【财务健康】
    - 流动比率: {self._safe_format(ratios['liquidity']['current_ratio'])}
    - 速动比率: {self._safe_format(ratios['liquidity']['quick_ratio'])}
    - 资产负债率: {self._safe_format(ratios['leverage']['debt_to_assets'])}%
    - 利息保障倍数: {self._safe_format(ratios['leverage']['interest_coverage'])}
    
    【现金流】
    - 经营现金流/营收: {self._safe_format(ratios['cash_flow']['ocf_to_revenue'])}%
    - 自由现金流: {self._safe_format(ratios['cash_flow']['fcf'])}万元
    
    【趋势数据】
    - ROE趋势(近4季): {ratios['trends'].get('roe_trend', [])}
    - 毛利率趋势(近4季): {ratios['trends'].get('margin_trend', [])}
    """
            
            prompt = f"""
基于以下财务数据，请进行深入的财务分析：

{data_summary}

请提供以下分析：

1. 盈利能力分析
   - ROE杜邦分析（净利率×资产周转率×权益乘数）
   - 盈利能力在行业中的水平
   - 盈利稳定性和可持续性

2. 运营效率评估
   - 营运资本管理效率
   - 资产利用效率
   - 与同行业对比

3. 财务风险评估
   - 偿债能力分析
   - 财务杠杆合理性
   - 流动性风险

4. 现金流质量
   - 经营现金流与净利润的匹配度
   - 自由现金流创造能力
   - 现金流的稳定性

5. 财务优势与问题
   - 核心财务优势（2-3点）
   - 需要关注的财务问题（2-3点）

要求：
- 结合行业特点进行分析
- 指出异常指标并分析原因
- 字数600-800字
"""
            
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位资深的财务分析专家，精通财务报表分析。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"财务分析生成失败：{str(e)}"

class ValuationAgent:
    """估值模型Agent - 计算估值并分析"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """执行估值计算和分析"""
        try:
            df = state['financials']
            if df is None or df.empty:
                state['errors'].append("ValuationAgent: 无财务数据")
                return state
            
            state['logs'].append("开始估值计算...")
            
            # 计算估值
            valuation = self._calculate_valuation(df, state)
            state['valuation'] = valuation
            
            # 调用Gemini进行估值分析
            analysis = self._analyze_valuation_with_gemini(valuation, state)
            state['valuation_analysis'] = analysis
            
            state['logs'].append("估值分析完成")
            
        except Exception as e:
            state['errors'].append(f"ValuationAgent错误: {str(e)}")
            state['logs'].append(f"估值分析失败: {str(e)}")
        
        return state
    
    def _calculate_valuation(self, df: pd.DataFrame, state: WorkflowState) -> Dict:
        """计算估值指标"""
        latest = df.iloc[0]
        current_price = state.get('current_price', 10.0)
        
        valuation = {
            'current_price': current_price,
            'current_pe': 0,
            'forward_pe': 0,
            'pb_ratio': 0,
            'ps_ratio': 0,
            'peg_ratio': 0,
            'ev_ebitda': 0,
            'dcf_value': 0,
            'target_price': 0,
            'upside_potential': 0
        }
        
        # 计算市盈率
        eps = latest.get('eps', 0)
        if eps > 0:
            valuation['current_pe'] = current_price / eps
        
        # 计算市净率
        bps = latest.get('bps', 0)
        if bps > 0:
            valuation['pb_ratio'] = current_price / bps
        
        # 计算市销率（简化）
        revenue_per_share = latest.get('revenue', 0) / 1000000  # 需要获取总股本
        if revenue_per_share > 0:
            valuation['ps_ratio'] = current_price / revenue_per_share
        
        # PEG（需要增长率）
        growth_rate = state.get('growth_curve', {}).get('profit_cagr', 0)
        if valuation['current_pe'] > 0 and growth_rate > 0:
            valuation['peg_ratio'] = valuation['current_pe'] / growth_rate
        
        # DCF估值
        valuation['dcf_value'] = self._calculate_dcf(df)
        
        # 目标价（使用多种方法的平均值）
        target_prices = []
        
        # PE估值法
        if eps > 0:
            industry_pe = 15  # 应该从行业数据获取
            target_prices.append(eps * industry_pe)
        
        # PB估值法
        if bps > 0:
            industry_pb = 1.5  # 应该从行业数据获取
            target_prices.append(bps * industry_pb)
        
        # DCF法
        if valuation['dcf_value'] > 0:
            target_prices.append(valuation['dcf_value'])
        
        if target_prices:
            valuation['target_price'] = np.mean(target_prices)
            valuation['upside_potential'] = (valuation['target_price'] - current_price) / current_price * 100
        
        return valuation
    
    def _calculate_dcf(self, df: pd.DataFrame) -> float:
        """DCF估值计算"""
        try:
            # 获取自由现金流
            fcf_list = df['fcff'].head(4).tolist() if 'fcff' in df.columns else [0]
            fcf_list = [f for f in fcf_list if f and f > 0]
            
            if not fcf_list:
                return 0
            
            avg_fcf = np.mean(fcf_list)
            
            # 估值参数（应该根据公司特点调整）
            growth_rate = 0.08  # 增长率
            discount_rate = 0.10  # 折现率
            terminal_growth = 0.03  # 永续增长率
            
            # 5年预测期
            pv_fcf = 0
            for i in range(1, 6):
                fcf = avg_fcf * (1 + growth_rate) ** i
                pv_fcf += fcf / (1 + discount_rate) ** i
            
            # 终值
            terminal_fcf = avg_fcf * (1 + growth_rate) ** 5 * (1 + terminal_growth)
            terminal_value = terminal_fcf / (discount_rate - terminal_growth)
            pv_terminal = terminal_value / (1 + discount_rate) ** 5
            
            # 企业价值
            enterprise_value = pv_fcf + pv_terminal
            
            # 每股价值（需要实际股本数据）
            shares_outstanding = 1000000  # 应该从数据中获取
            equity_value_per_share = enterprise_value / shares_outstanding
            
            return round(equity_value_per_share, 2)
            
        except Exception as e:
            print(f"DCF计算错误: {e}")
            return 0
    
    def _analyze_valuation_with_gemini(self, valuation: Dict, state: WorkflowState) -> str:
        """使用Gemini分析估值"""
        try:
            # 准备估值数据
            data_summary = f"""
公司：{state['company_name']}（{state['ticker']}）

估值数据：
- 当前股价：{valuation['current_price']:.2f}元
- 市盈率(PE)：{valuation['current_pe']:.2f}
- 市净率(PB)：{valuation['pb_ratio']:.2f}
- 市销率(PS)：{valuation['ps_ratio']:.2f}
- PEG比率：{valuation['peg_ratio']:.2f}
- DCF估值：{valuation['dcf_value']:.2f}元
- 目标价：{valuation['target_price']:.2f}元
- 上涨空间：{valuation['upside_potential']:.2f}%
"""
            
            prompt = f"""
基于以下估值数据，请进行专业的估值分析：

{data_summary}

请提供以下分析：

1. 估值水平评估
   - 绝对估值水平（高估/合理/低估）
   - 与历史估值水平对比
   - 与行业平均估值对比

2. 估值方法分析
   - PE估值法的适用性和结论
   - PB估值法的适用性和结论
   - DCF估值的关键假设和敏感性

3. 估值驱动因素
   - 影响估值的核心因素
   - 估值提升的催化剂
   - 估值下行的风险

4. 投资建议
   - 目标价合理性分析
   - 投资评级建议（买入/增持/中性/减持/卖出）
   - 建议的投资时间窗口

5. 风险提示
   - 估值假设的主要风险
   - 可能导致估值调整的因素

要求：
- 结合公司基本面和行业特点
- 提供明确的投资建议
- 字数500-700字
"""
            
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位专业的股票估值分析师，精通各种估值方法。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5200
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"估值分析生成失败：{str(e)}"

class RiskCatalystAgent:
    """风险与催化剂分析Agent"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """生成风险与催化剂分析"""
        try:
            state['logs'].append("开始生成风险与催化剂分析...")
            
            # 收集关键信息
            key_info = self._collect_key_info(state)
            
            # 构建prompt
            prompt = self._build_prompt(key_info, state)
            
            # 调用LLM
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位专业的风险管理专家，擅长识别投资风险和机会。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5500
            )
            
            state['risk_catalyst'] = response.choices[0].message.content
            state['logs'].append("风险与催化剂分析完成")
            
        except Exception as e:
            state['errors'].append(f"RiskCatalystAgent错误: {str(e)}")
            state['risk_catalyst'] = "风险与催化剂分析生成失败"
        
        return state
    
    def _collect_key_info(self, state: WorkflowState) -> Dict:
        """收集关键信息用于风险分析"""
        info = {
            'roe': 0,
            'debt_ratio': 0,
            'current_ratio': 0,
            'revenue_growth': 0,
            'pe_ratio': 0,
            'upside': 0
        }
        
        if state.get('ratios'):
            info['roe'] = state['ratios']['profitability'].get('roe', 0)
            info['debt_ratio'] = state['ratios']['leverage'].get('debt_to_assets', 0)
            info['current_ratio'] = state['ratios']['liquidity'].get('current_ratio', 0)
        
        if state.get('growth_curve'):
            info['revenue_growth'] = state['growth_curve'].get('revenue_cagr', 0)
        
        if state.get('valuation'):
            info['pe_ratio'] = state['valuation'].get('current_pe', 0)
            info['upside'] = state['valuation'].get('upside_potential', 0)
        
        return info
    def _safe_format(self, value, format_str=".2f", default=0):
        """安全格式化数值，处理None值"""
        if value is None:
            return f"{default:{format_str}}"
        try:
            return f"{value:{format_str}}"
        except (ValueError, TypeError):
            return f"{default:{format_str}}"
    
    def _build_prompt(self, info: Dict, state: WorkflowState) -> str:
        """构建风险分析提示词"""
        return f"""
请为{state['company_name']}（{state['ticker']}）生成详细的风险与催化剂分析。

公司关键指标：
- ROE: {self._safe_format(info['roe'])}%
- 资产负债率: {self._safe_format(info['debt_ratio'])}%
- 流动比率: {self._safe_format(info['current_ratio'])}
- 营收增长率: {self._safe_format(info['revenue_growth'])}%
- PE估值: {self._safe_format(info['pe_ratio'])}
- 潜在上涨空间: {self._safe_format(info['upside'])}%

请分析以下内容：

一、主要风险因素（请结合公司实际情况）
1. 行业风险
   - 行业周期性风险
   - 技术变革风险
   - 竞争加剧风险

2. 公司特定风险
   - 经营风险
   - 财务风险
   - 管理风险

3. 外部风险
   - 政策监管风险
   - 宏观经济风险
   - 国际环境风险

二、潜在催化剂
1. 短期催化剂（6个月内）
   - 业绩超预期
   - 新产品/新业务
   - 政策利好

2. 中长期催化剂（6个月-2年）
   - 行业整合机会
   - 市场份额提升
   - 转型升级成功

三、风险应对建议
- 风险监控指标
- 风险缓释措施
- 投资策略建议

要求：
- 每个风险和催化剂都要具体、可量化
- 结合公司和行业最新动态
- 提供可操作的建议
- 字数600-800字
"""

class WritingAgent:
    """报告撰写Agent - 整合所有分析生成最终报告"""
    
    def __init__(self):
        self.llm_client = APIClients.init_poe_client()
    
    def __call__(self, state: WorkflowState) -> WorkflowState:
        """生成完整的Markdown报告"""
        try:
            state['logs'].append("开始生成研究报告...")
            
            # 生成核心观点（最后生成，基于所有分析）
            core_viewpoints = self._generate_core_viewpoints(state)
            state['core_viewpoints'] = core_viewpoints
            
            # 组装完整报告
            markdown_report = self._assemble_report(state)
            
            state['markdown_report'] = markdown_report
            state['logs'].append("研究报告生成完成")
            
        except Exception as e:
            state['errors'].append(f"WritingAgent错误: {str(e)}")
            state['logs'].append(f"报告生成失败: {str(e)}")
            
            # 生成简化报告
            state['markdown_report'] = self._generate_simple_report(state)
        
        return state
    
    def _generate_core_viewpoints(self, state: WorkflowState) -> str:
        """基于所有分析生成核心观点"""
        try:
            # 收集所有分析内容
            all_analysis = f"""
行业分析要点：
{state.get('industry_analysis', '暂无')[:500]}

增长分析要点：
{state.get('growth_analysis', '暂无')[:500]}

财务分析要点：
{state.get('financial_analysis', '暂无')[:500]}

估值分析要点：
{state.get('valuation_analysis', '暂无')[:500]}

风险与催化剂要点：
{state.get('risk_catalyst', '暂无')[:500]}
"""
            
            prompt = f"""
基于以下所有分析内容，请为{state['company_name']}（{state['ticker']}）生成3-5个核心投资观点：

{all_analysis}

要求：
1. 每个观点要简洁有力（1-2句话）
2. 要有明确的投资逻辑
3. 包含关键数据支撑
4. 体现投资价值和风险
5. 给出明确的投资建议

格式示例：
• 核心观点1：[观点描述]，[数据支撑]，[投资含义]
• 核心观点2：...

最后给出投资评级和目标价。
"""
            
            response = self.llm_client.chat.completions.create(
                model=Config.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "你是一位资深的投资策略分析师，擅长提炼投资要点。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=5800
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"核心观点生成失败：{str(e)}"
    
    def _assemble_report(self, state: WorkflowState) -> str:
        """组装完整报告"""
        company_name = state.get('company_name', state['ticker'])
        ticker = state['ticker']
        report_date = state['report_date']
        
        # 获取估值数据
        valuation = state.get('valuation', {})
        target_price = valuation.get('target_price', 0)
        upside = valuation.get('upside_potential', 0)
        
        report = f"""
    # {company_name}（{ticker}）深度研究报告
    
    **报告日期：** {report_date}  
    **目标价：** {target_price:.2f}元  
    **潜在上涨空间：** {upside:.2f}%  
    
    ---
    
    ## 核心观点
    
    {state.get('core_viewpoints', '核心观点生成中...')}
    
    ---
    
    ## 1. 公司简介
    
    {state.get('company_intro', '公司简介生成中...')}
    
    ---
    
    ## 2. 行业与竞争格局
    
    {state.get('industry_analysis', '行业分析生成中...')}
    
    ---
    
    ## 3. 成长性分析
    
    {state.get('growth_analysis', '增长分析生成中...')}
    
    ---
    
    ## 4. 财务分析
    
    {state.get('financial_analysis', '财务分析生成中...')}
    
    ---
    
    ## 5. 估值分析
    
    {state.get('valuation_analysis', '估值分析生成中...')}
    
    ---
    
    ## 6. 风险与催化剂
    
    {state.get('risk_catalyst', '风险分析生成中...')}
    
    ---
    
    ## 附录：关键财务数据
    
    ### 增长指标
    """
        
        # 修改增长数据表格生成部分
        if state.get('growth_curve'):
            growth = state['growth_curve']
            revenue_growth = growth.get('revenue_growth', [])
            profit_growth = growth.get('profit_growth', [])
            
            # 安全获取数据，避免索引越界和None值
            def safe_get_growth(data_list, index):
                try:
                    if len(data_list) > index:
                        value = data_list[index]
                        return self._safe_format(value) if value is not None else "N/A"
                    return "N/A"
                except:
                    return "N/A"
            
            report += f"""
    | 指标 | Q1 | Q2 | Q3 | Q4 | CAGR |
    |------|----|----|----|----|------|
    | 营收增长 | {safe_get_growth(revenue_growth, 0)}% | {safe_get_growth(revenue_growth, 1)}% | {safe_get_growth(revenue_growth, 2)}% | {safe_get_growth(revenue_growth, 3)}% | {self._safe_format(growth.get('revenue_cagr', 0))}% |
    | 利润增长 | {safe_get_growth(profit_growth, 0)}% | {safe_get_growth(profit_growth, 1)}% | {safe_get_growth(profit_growth, 2)}% | {safe_get_growth(profit_growth, 3)}% | {self._safe_format(growth.get('profit_cagr', 0))}% |
    """
        
        # 添加财务比率表格
        report += """
    ### 核心财务比率
    """
        if state.get('ratios'):
            ratios = state['ratios']
            prof = ratios.get('profitability', {})
            liq = ratios.get('liquidity', {})
            lev = ratios.get('leverage', {})
            
            report += f"""
    | 类别 | 指标 | 数值 |
    |------|------|------|
    | 盈利能力 | ROE | {self._safe_format(prof.get('roe', 0))}% |
    | | ROA | {self._safe_format(prof.get('roa', 0))}% |
    | | 毛利率 | {self._safe_format(prof.get('gross_margin', 0))}% |
    | | 净利率 | {self._safe_format(prof.get('net_margin', 0))}% |
    | 流动性 | 流动比率 | {self._safe_format(liq.get('current_ratio', 0))} |
    | | 速动比率 | {self._safe_format(liq.get('quick_ratio', 0))} |
    | 杠杆 | 资产负债率 | {self._safe_format(lev.get('debt_to_assets', 0))}% |
    | | 利息保障倍数 | {self._safe_format(lev.get('interest_coverage', 0))} |
    """
        
        # 添加页脚
        report += f"""
    
    ---
    
    **免责声明：** 本报告基于公开数据和模型分析生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。
    
    **数据来源：** Tushare金融数据  
    **分析模型：** LangGraph + Gemini AI  
    **生成时间：** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
        
        return report
    def _safe_format(self, value):
        """安全格式化数值，处理None和各种异常值"""
        try:
            if value is None:
                return "N/A"
            if isinstance(value, str):
                try:
                    value = float(value)
                except:
                    return "N/A"
            if isinstance(value, (int, float)):
                if np.isnan(value) or np.isinf(value):
                    return "N/A"
                return f"{float(value):.2f}"
            return "N/A"
        except Exception as e:
            return "N/A"
    
    def _generate_simple_report(self, state: WorkflowState) -> str:
        """生成简化报告（错误时使用）"""
        return f"""
# {state.get('company_name', state['ticker'])}（{state['ticker']}）研究报告

**报告日期：** {state['report_date']}

## 报告生成过程中遇到错误

### 错误信息：
{chr(10).join(state.get('errors', ['未知错误']))}

### 执行日志：
{chr(10).join(state.get('logs', ['无日志']))}

---

**说明：** 由于数据获取或分析过程中出现问题，无法生成完整报告。请检查数据源和API配置。
"""

# ===========================
# 工作流构建
# ===========================

class ReportWorkflow:
    """报告生成工作流"""
    
    def __init__(self):
        self.workflow = None
        self.agents = {}
        self._initialize_agents()
        self._build_workflow()
    
    def _initialize_agents(self):
        """初始化所有Agent"""
        self.agents = {
            'data_load': DataLoadAgent(),
            'company_intro': CompanyIntroAgent(),
            'industry_analysis': IndustryAnalysisAgent(),
            'growth_curve': GrowthCurveAgent(),
            'ratio_calc': RatioCalcAgent(),
            'valuation': ValuationAgent(),
            'risk_catalyst': RiskCatalystAgent(),
            'writing': WritingAgent()
        }
    
    def _build_workflow(self):
        """构建LangGraph工作流"""
        # 创建状态图
        workflow = StateGraph(WorkflowState)
        
        # 添加节点
        for name, agent in self.agents.items():
            workflow.add_node(name, agent)
        
        # 设置入口点
        workflow.set_entry_point("data_load")
        
        # 添加边（定义执行顺序）
        workflow.add_edge("data_load", "company_intro")
        workflow.add_edge("company_intro", "industry_analysis")
        workflow.add_edge("industry_analysis", "growth_curve")
        workflow.add_edge("growth_curve", "ratio_calc")
        workflow.add_edge("ratio_calc", "valuation")
        workflow.add_edge("valuation", "risk_catalyst")
        workflow.add_edge("risk_catalyst", "writing")
        workflow.add_edge("writing", END)
        
        # 编译工作流
        self.workflow = workflow.compile()
    
    def generate_report(self, ticker: str, company_name: Optional[str] = None) -> Dict:
        """生成研究报告"""
        # 初始化状态
        initial_state = WorkflowState(
            ticker=ticker,
            company_name=company_name or ticker,
            report_date=datetime.now().strftime('%Y-%m-%d'),
            current_price=None,
            financials=None,
            growth_curve=None,
            ratios=None,
            valuation=None,
            company_intro=None,
            industry_analysis=None,
            growth_analysis=None,
            financial_analysis=None,
            valuation_analysis=None,
            risk_catalyst=None,
            core_viewpoints=None,
            markdown_report=None,
            errors=[],
            logs=[]
        )
        
        # 执行工作流
        print(f"开始生成 {ticker} 的研究报告...")
        print("=" * 50)
        
        try:
            # 运行工作流
            final_state = self.workflow.invoke(initial_state)
            
            # 保存报告
            if final_state.get('markdown_report'):
                self._save_report(final_state)
            
            return final_state
            
        except Exception as e:
            print(f"工作流执行失败: {e}")
            initial_state['errors'].append(f"工作流执行失败: {str(e)}")
            return initial_state
    
    def _save_report(self, state: WorkflowState):
        """保存报告到文件"""
        try:
            # 创建输出目录
            os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
            
            # 生成文件名
            filename = f"{state['ticker']}_{state['report_date']}.md"
            filepath = os.path.join(Config.OUTPUT_DIR, filename)
            
            # 写入文件
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(state['markdown_report'])
            
            print(f"报告已保存至: {filepath}")
            
        except Exception as e:
            print(f"报告保存失败: {e}")

# ===========================
# 辅助函数
# ===========================

def is_jupyter():
    """检测是否在Jupyter环境中运行"""
    try:
        get_ipython()
        return True
    except NameError:
        return False

def generate_report_for(ticker: str, company_name: Optional[str] = None) -> Dict:
    """
    生成指定股票的研究报告（适用于Jupyter环境）
    
    参数:
        ticker: 股票代码，如 '600000.SH'
        company_name: 公司名称，如 '浦发银行'
    
    返回:
        包含报告和执行信息的字典
    """
    workflow = ReportWorkflow()
    result = workflow.generate_report(ticker, company_name)
    
    # 打印简要结果
    print(f"\n{'='*50}")
    print(f"报告生成{'成功' if result.get('markdown_report') else '失败'}")
    print(f"{'='*50}")
    
    if result['logs']:
        print("\n执行日志:")
        for log in result['logs'][-5:]:  # 只显示最后5条日志
            print(f"  - {log}")
    
    if result['errors']:
        print("\n错误信息:")
        for error in result['errors']:
            print(f"  ❌ {error}")
    
    return result

def test_workflow():
    """测试工作流"""
    print("=" * 70)
    print("测试A股研究报告生成系统")
    print("=" * 70)
    
    # 测试股票
    test_ticker = "600000.SH"
    test_name = "浦发银行"
    
    print(f"\n测试股票: {test_name}（{test_ticker}）")
    print("-" * 70)
    
    # 生成报告
    result = generate_report_for(test_ticker, test_name)
    
    # 输出详细结果
    print(f"\n测试结果汇总:")
    print(f"- 股票代码: {result['ticker']}")
    print(f"- 公司名称: {result['company_name']}")
    print(f"- 报告日期: {result['report_date']}")
    print(f"- 财务数据: {'✓ 已加载' if result.get('financials') is not None else '✗ 加载失败'}")
    print(f"- 公司简介: {'✓ 已生成' if result.get('company_intro') else '✗ 生成失败'}")
    print(f"- 行业分析: {'✓ 已生成' if result.get('industry_analysis') else '✗ 生成失败'}")
    print(f"- 增长分析: {'✓ 已生成' if result.get('growth_analysis') else '✗ 生成失败'}")
    print(f"- 财务分析: {'✓ 已生成' if result.get('financial_analysis') else '✗ 生成失败'}")
    print(f"- 估值分析: {'✓ 已生成' if result.get('valuation_analysis') else '✗ 生成失败'}")
    print(f"- 风险分析: {'✓ 已生成' if result.get('risk_catalyst') else '✗ 生成失败'}")
    print(f"- 核心观点: {'✓ 已生成' if result.get('core_viewpoints') else '✗ 生成失败'}")
    print(f"- 完整报告: {'✓ 已生成' if result.get('markdown_report') else '✗ 生成失败'}")
    
    print("\n" + "=" * 70)
    
    return result

# ===========================
# 主程序入口
# ===========================

def main():
    """主程序入口"""
    import argparse
    
    # 检测是否在Jupyter环境中
    if is_jupyter():
        print("检测到Jupyter环境，请使用以下函数：")
        print("  - test_workflow() : 运行测试")
        print("  - generate_report_for('股票代码', '公司名称') : 生成报告")
        return
    
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='A股上市公司深度报告自动生成系统')
    parser.add_argument('ticker', nargs='?', help='股票代码，如 600000.SH')
    parser.add_argument('--name', type=str, help='公司名称', default=None)
    parser.add_argument('--test', action='store_true', help='运行测试')
    
    args = parser.parse_args()
    
    if args.test or not args.ticker:
        # 运行测试
        test_workflow()
    else:
        # 生成指定股票的报告
        workflow = ReportWorkflow()
        result = workflow.generate_report(args.ticker, args.name)
        
        # 打印执行结果
        print("\n执行完成！")
        if result['errors']:
            print("遇到以下错误:")
            for error in result['errors']:
                print(f"  ❌ {error}")

if __name__ == "__main__":
    # 检测运行环境
    if is_jupyter():
        # 在Jupyter中自动显示使用说明
        print("=" * 70)
        print("A股研究报告自动生成系统 - Jupyter环境已就绪")
        print("=" * 70)
        print("\n使用方法:")
        print("  1. 运行测试: test_workflow()")
        print("  2. 生成报告: generate_report_for('600000.SH', '浦发银行')")
        print("\n支持的股票代码格式: '600000.SH', '000001.SZ' 等")
    else:
        # 命令行环境
        main()