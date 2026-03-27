"""
ETF 分析协调器
整合数据获取、技术分析、LLM分析，生成完整分析结果
"""

from __future__ import annotations

import asyncio
from typing import List, Optional

from data_provider.akshare_provider import TushareProvider
from data_provider.etf_registry import get_etf_info
from src.config import config
from src.logger import logger
from src.models import ETFAnalysisResult
from src.technical import TechnicalAnalyzer
from src.llm_analyzer import LLMAnalyzer


class ETFAnalyzer:
    """ETF 分析协调器"""

    def __init__(self):
        self.data_provider = AkShareProvider()
        self.tech_analyzer = TechnicalAnalyzer()
        self.llm_analyzer = LLMAnalyzer()

    async def analyze_single(self, code: str) -> ETFAnalysisResult:
        """分析单只ETF"""
        code = code.strip()  # 清理空白字符
        registry_info = get_etf_info(code)

        result = ETFAnalysisResult(
            code=code,
            name=registry_info["name"],
            index=registry_info["index"],
            etf_type=registry_info["type"],
        )

        logger.info(f"开始分析 {code} {result.name}...")

        # 1. 获取实时行情
        try:
            quote = await self.data_provider.get_realtime_quote(code)
            if quote:
                result.name = quote.get("name") or result.name
                result.price = quote.get("price", 0)
                result.open = quote.get("open", 0)
                result.high = quote.get("high", 0)
                result.low = quote.get("low", 0)
                result.prev_close = quote.get("prev_close", 0)
                result.change_pct = quote.get("change_pct", 0)
                result.change_amt = quote.get("change_amt", 0)
                result.volume = quote.get("volume", 0)
                result.turnover = quote.get("turnover", 0)
                result.turnover_rate = quote.get("turnover_rate", 0)
                result.amplitude = quote.get("amplitude", 0)
            else:
                logger.warning(f"{code} 未获取到实时行情，跳过")
                result.error = "无法获取实时行情"
                return result
        except Exception as e:
            logger.error(f"{code} 行情获取异常: {e}")
            result.error = str(e)
            return result

        # 2. 获取历史数据并计算技术指标
        try:
            hist_df = await self.data_provider.get_history(code, days=180)
            if hist_df is not None and not hist_df.empty:
                tech = self.tech_analyzer.analyze(hist_df)
                result.ma5 = tech.ma5
                result.ma10 = tech.ma10
                result.ma20 = tech.ma20
                result.ma60 = tech.ma60
                result.ma120 = tech.ma120
                result.ma_trend = tech.ma_trend
                result.rsi14 = tech.rsi14
                result.rsi_signal = tech.rsi_signal
                result.macd = tech.macd
                result.macd_signal_line = tech.macd_signal
                result.macd_hist = tech.macd_hist
                result.macd_cross = tech.macd_cross
                result.macd_hist_trend = tech.macd_hist_trend
                result.bb_upper = tech.bb_upper
                result.bb_middle = tech.bb_middle
                result.bb_lower = tech.bb_lower
                result.bb_position = tech.bb_position
                result.kdj_k = tech.kdj_k
                result.kdj_d = tech.kdj_d
                result.kdj_j = tech.kdj_j
                result.kdj_cross = tech.kdj_cross
                result.volume_ratio = tech.volume_ratio
                result.volume_trend = tech.volume_trend
                result.adx = tech.adx
                result.adx_trend = tech.adx_trend
                result.obv = tech.obv
                result.obv_trend = tech.obv_trend
                result.overall_signal = tech.overall_signal
                result.signal_score = tech.signal_score
                result.signals = tech.signals
        except Exception as e:
            logger.warning(f"{code} 技术分析失败: {e}")

        # 3. 获取净值和折溢价（可选）
        try:
            nav_data = await self.data_provider.get_etf_nav(code)
            if nav_data:
                result.nav = nav_data.get("nav")
                result.nav_date = nav_data.get("nav_date")
                if result.nav and result.price:
                    result.premium_discount = (result.price / result.nav - 1) * 100
        except Exception as e:
            logger.debug(f"{code} 净值获取失败: {e}")

        # 4. LLM 智能分析（只有拿到行情数据才调用）
        try:
            if self.llm_analyzer.is_available() and result.price > 0:
                market_overview = await self.data_provider.get_market_overview()
                etf_data_for_llm = result.to_dict()
                etf_data_for_llm["market_overview"] = market_overview
                etf_data_for_llm["registry_info"] = registry_info
                result.llm_analysis = await self.llm_analyzer.analyze(etf_data_for_llm)
            else:
                if result.price <= 0:
                    logger.info(f"{code} 行情数据为空，跳过LLM分析")
                else:
                    logger.info(f"{code} LLM未配置，跳过AI分析")
        except Exception as e:
            logger.warning(f"{code} LLM分析失败: {e}")

        logger.info(f"✅ {code} {result.name} 分析完成，信号: {result.overall_signal} ({result.signal_score:+.1f})")
        return result

    async def analyze_batch(
        self, codes: List[str], concurrency: int = 3
    ) -> List[ETFAnalysisResult]:
        """批量分析ETF，控制并发数量避免触发限流"""
        results = []
        semaphore = asyncio.Semaphore(concurrency)

        async def analyze_with_limit(code: str) -> ETFAnalysisResult:
            async with semaphore:
                result = await self.analyze_single(code)
                # 加入小延时，避免频繁请求
                await asyncio.sleep(0.5)
                return result

        tasks = [analyze_with_limit(code) for code in codes]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # 按信号评分排序
        results.sort(key=lambda r: r.signal_score, reverse=True)
        return results
