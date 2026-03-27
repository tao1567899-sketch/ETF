"""
AkShare 数据提供者
获取国内ETF的实时行情、历史K线、净值、折溢价等数据
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.logger import logger


class AkShareProvider:
    """基于 AkShare 的 ETF 数据提供者"""

    def __init__(self):
        self._ak = None  # 懒加载

    def _get_ak(self):
        if self._ak is None:
            import akshare as ak
            self._ak = ak
        return self._ak

    async def get_realtime_quote(self, code: str) -> Optional[dict]:
        """获取ETF实时行情"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_realtime_quote, code)

    def _fetch_realtime_quote(self, code: str) -> Optional[dict]:
        ak = self._get_ak()
        try:
            # 判断交易所前缀
            market = "sh" if code.startswith(("5", "11")) else "sz"
            symbol = f"{market}{code}"

            df = ak.stock_zh_a_spot_em()
            row = df[df["代码"] == code]
            if row.empty:
                # 备用：直接查ETF行情
                df2 = ak.fund_etf_spot_em()
                row = df2[df2["代码"] == code]

            if row.empty:
                logger.warning(f"未找到ETF {code} 的实时行情")
                return None

            r = row.iloc[0]
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
        except Exception as e:
            logger.error(f"获取 {code} 实时行情失败: {e}")
            return None

    async def get_history(
        self,
        code: str,
        period: str = "daily",
        days: int = 120,
    ) -> Optional[pd.DataFrame]:
        """获取ETF历史K线数据"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._fetch_history, code, period, days
        )

    def _fetch_history(
        self, code: str, period: str, days: int
    ) -> Optional[pd.DataFrame]:
        ak = self._get_ak()
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=days + 30)).strftime(
                "%Y%m%d"
            )

            # 使用 ETF 专用接口
            df = ak.fund_etf_hist_em(
                symbol=code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="hfq",  # 后复权
            )

            if df is None or df.empty:
                # 备用：股票历史接口
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="hfq",
                )

            if df is None or df.empty:
                logger.warning(f"未获取到 {code} 历史数据")
                return None

            # 标准化列名
            col_map = {
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "成交额": "turnover",
                "振幅": "amplitude",
                "涨跌幅": "change_pct",
                "涨跌额": "change_amt",
                "换手率": "turnover_rate",
            }
            df = df.rename(columns=col_map)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").tail(days)
            df = df.reset_index(drop=True)

            # 确保数值列
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception as e:
            logger.error(f"获取 {code} 历史数据失败: {e}")
            return None

    async def get_etf_nav(self, code: str) -> Optional[dict]:
        """获取ETF净值（NAV）和折溢价数据"""
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
            return {
                "nav": nav,
                "nav_date": str(latest.get("净值日期", "")),
            }
        except Exception as e:
            logger.debug(f"获取 {code} 净值失败（非开放式基金）: {e}")
            return None

    async def get_market_overview(self) -> dict:
        """获取大盘概览（主要宽基指数）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_market_overview)

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
