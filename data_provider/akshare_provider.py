"""
AkShare 数据提供者
获取国内ETF的实时行情、历史K线、净值、折溢价等数据
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.logger import logger


def _with_retry(func, *args, retries=3, delay=5, **kwargs):
    """带重试的封装，适用于易超时的网络请求"""
    last_err = None
    for i in range(retries):
        try:
            result = func(*args, **kwargs)
            if result is not None:
                return result
        except Exception as e:
            last_err = e
            logger.warning(f"第 {i+1}/{retries} 次获取失败: {e}")
        if i < retries - 1:
            time.sleep(delay * (i + 1))
    logger.error(f"重试 {retries} 次后仍失败: {last_err}")
    return None


class AkShareProvider:
    """基于 AkShare 的 ETF 数据提供者"""

    def __init__(self):
        self._ak = None  # 懒加载

    def _get_ak(self):
        if self._ak is None:
            import akshare as ak
            ak.http_cache = False
            self._ak = ak
        return self._ak

    async def get_realtime_quote(self, code: str) -> Optional[dict]:
        """获取ETF实时行情，多接口兜底"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(
                self._fetch_realtime_quote, code, retries=3, delay=8
            ),
        )

    def _fetch_realtime_quote(self, code: str) -> Optional[dict]:
        ak = self._get_ak()

        # 策略1：基金 ETF 实时行情（东方财富）
        try:
            df = ak.fund_etf_spot_em()
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                name = str(r.get("名称", ""))
                if name and name not in ("", "nan"):
                    logger.info(f"[{code}] 通过 fund_etf_spot_em 获取到: {name}")
                    return self._row_to_quote(r, code)
        except Exception as e:
            logger.warning(f"[{code}] fund_etf_spot_em 失败: {e}")

        # 策略2：A股全票行情（东方财富）
        try:
            df = ak.stock_zh_a_spot_em()
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                name = str(r.get("名称", ""))
                if name and name not in ("nan",):
                    logger.info(f"[{code}] 通过 stock_zh_a_spot_em 获取到: {name}")
                    return self._row_to_quote(r, code)
        except Exception as e:
            logger.warning(f"[{code}] stock_zh_a_spot_em 失败: {e}")

        logger.warning(f"未找到ETF {code} 的实时行情（已尝试2种接口）")
        return None

    def _row_to_quote(self, r, code: str) -> dict:
        return {
            "code": code,
            "name": str(r.get("名称", "")),
            "price": float(r.get("最新价", 0)),
            "open": float(r.get("今开", 0)),
            "high": float(r.get("最高", 0)),
            "low": float(r.get("最低", 0)),
            "prev_close": float(r.get("昨收", 0)),
            "volume": float(r.get("成交量", 0)),
            "turnover": float(r.get("成交额", 0)),
            "change_pct": float(r.get("涨跌幅", 0)),
            "change_amt": float(r.get("涨跌额", 0)),
            "amplitude": float(r.get("振幅", 0)),
            "turnover_rate": float(r.get("换手率", 0)),
            "pe_ratio": r.get("市盈率-动态", None),
            "timestamp": datetime.now().isoformat(),
        }

    async def get_history(
        self, code: str, period: str = "daily", days: int = 120,
    ) -> Optional[pd.DataFrame]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(
                self._fetch_history, code, period, days, retries=3, delay=5
            ),
        )

    def _fetch_history(
        self, code: str, period: str, days: int
    ) -> Optional[pd.DataFrame]:
        ak = self._get_ak()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")

        col_map = {
            "日期": "date", "开盘": "open", "最高": "high", "最低": "low",
            "收盘": "close", "成交量": "volume", "成交额": "turnover",
            "振幅": "amplitude", "涨跌幅": "change_pct", "涨跌额": "change_amt",
            "换手率": "turnover_rate",
        }

        # 策略1：ETF 历史行情
        try:
            df = ak.fund_etf_hist_em(
                symbol=code, period=period,
                start_date=start_date, end_date=end_date, adjust="hfq",
            )
            if df is not None and not df.empty:
                df = df.rename(columns=col_map)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").tail(days).reset_index(drop=True)
                for col in ["open", "high", "low", "close", "volume", "turnover"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.info(f"[{code}] 历史数据获取成功，共 {len(df)} 条")
                return df
        except Exception as e:
            logger.warning(f"[{code}] fund_etf_hist_em 失败: {e}")

        # 策略2：A股历史K线（备用）
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period=period,
                start_date=start_date, end_date=end_date, adjust="hfq",
            )
            if df is not None and not df.empty:
                df = df.rename(columns=col_map)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").tail(days).reset_index(drop=True)
                for col in ["open", "high", "low", "close", "volume", "turnover"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.info(f"[{code}] 股票历史接口获取成功，共 {len(df)} 条")
                return df
        except Exception as e:
            logger.warning(f"[{code}] stock_zh_a_hist 失败: {e}")

        logger.warning(f"未获取到 {code} 历史数据")
        return None

    async def get_etf_nav(self, code: str) -> Optional[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_etf_nav, code)

    def _fetch_etf_nav(self, code: str) -> Optional[dict]:
        ak = self._get_ak()
        try:
            df = ak.fund_open_fund_info_em(fund=code, indicator="单位净值走势")
            if df is None or df.empty:
                return None
            latest = df.iloc[-1]
            nav = float(latest.get("单位净值", 0))
            return {"nav": nav, "nav_date": str(latest.get("净值日期", ""))}
        except Exception as e:
            logger.debug(f"获取 {code} 净值失败: {e}")
            return None

    async def get_market_overview(self) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(self._fetch_market_overview, retries=2, delay=3),
        )

    def _fetch_market_overview(self) -> dict:
        ak = self._get_ak()
        indices = {
            "000001": "上证指数",
            "399001": "深证成指",
            "399006": "创业板指",
            "000688": "科创50",
        }
        result = {}
        try:
            df = ak.stock_zh_index_spot_em()
            for code, name in indices.items():
                row = df[df["代码"] == code]
                if not row.empty:
                    r = row.iloc[0]
                    result[name] = {
                        "price": float(r.get("最新价", 0)),
                        "change_pct": float(r.get("涨跌幅", 0)),
                    }
        except Exception as e:
            logger.warning(f"获取大盘数据失败: {e}")
        return result
