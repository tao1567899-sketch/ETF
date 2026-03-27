"""
TuShare 数据提供者
获取国内ETF的实时行情、历史K线、净值等数据
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.logger import logger


def _with_retry(func, *args, retries=3, delay=5, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            result = func(*args, **kwargs)
            if result is not None:
                return result
        except Exception as e:
            last_err = e
            logger.warning(f"第 {i+1}/{retries} 次失败: {e}")
        if i < retries - 1:
            time.sleep(delay * (i + 1))
    logger.error(f"重试 {retries} 次后仍失败: {last_err}")
    return None


class AkShareProvider:
    def __init__(self):
        self._ts = None

    def _get_ts(self):
        if self._ts is None:
            import tushare as ts
            token = os.getenv("TUSHARE_TOKEN", "")
            if not token:
                raise RuntimeError("TUSHARE_TOKEN 未配置，请在 GitHub Secrets 中添加 TUSHARE_TOKEN")
            proxy = os.getenv("TUSHARE_PROXY", "").rstrip("/")
            if proxy:
                for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                    os.environ[k] = proxy
                logger.info(f"使用代理: {proxy}")
            ts.set_token(token)
            self._ts = ts
            logger.info("TuShare 初始化成功")
        return self._ts

    def _code(self, code: str) -> str:
        if code.startswith(("5", "11", "13")):
            return f"{code}.SH"
        return f"{code}.SZ"

    async def get_realtime_quote(self, code: str) -> Optional[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(self._fetch_realtime, code, retries=3, delay=5),
        )

    def _fetch_realtime(self, code: str) -> Optional[dict]:
        ts = self._get_ts()
        ts_code = self._code(code)
        today = datetime.now().strftime("%Y%m%d")

        try:
            df = ts.daily(ts_code=ts_code, start_date=today, end_date=today)
            if df is None or df.empty:
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                df = ts.daily(ts_code=ts_code, start_date=yesterday, end_date=yesterday)

            if df is None or df.empty:
                logger.warning(f"[{code}] 未找到实时行情")
                return None

            r = df.iloc[0]
            close = float(r.get("close", 0))
            pre_close = float(r.get("pre_close", close))
            change = float(r.get("change", 0)) if r.get("change") is not None else (close - pre_close)
            pct = float(r.get("pct_chg", 0)) if r.get("pct_chg") is not None else 0.0

            return {
                "code": code,
                "name": str(r.get("name", f"ETF-{code}")),
                "price": close,
                "open": float(r.get("open", close)),
                "high": float(r.get("high", close)),
                "low": float(r.get("low", close)),
                "prev_close": pre_close,
                "volume": float(r.get("vol", 0) or 0) * 100,
                "turnover": float(r.get("amount", 0) or 0) * 10000,
                "change_pct": pct,
                "change_amt": change,
                "amplitude": 0.0,
                "turnover_rate": float(r.get("turnover_rate", 0) or 0),
                "pe_ratio": None,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error(f"获取 {code} 实时行情失败: {e}")
            return None

    async def get_history(self, code: str, period: str = "daily", days: int = 120) -> Optional[pd.DataFrame]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(self._fetch_history, code, period, days, retries=3, delay=5),
        )

    def _fetch_history(self, code: str, period: str, days: int) -> Optional[pd.DataFrame]:
        ts = self._get_ts()
        ts_code = self._code(code)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 30)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        try:
            df = ts.daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
            if df is None or df.empty:
                logger.warning(f"[{code}] 未获取到历史数据")
                return None

            df = df.rename(columns={
                "trade_date": "date",
                "vol": "volume",
                "amount": "turnover",
                "pct_chg": "change_pct",
            })

            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.sort_values("date").tail(days).reset_index(drop=True)

            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            logger.info(f"[{code}] 历史数据 {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取 {code} 历史数据失败: {e}")
            return None

    async def get_etf_nav(self, code: str) -> Optional[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_nav, code)

    def _fetch_nav(self, code: str) -> Optional[dict]:
        ts = self._get_ts()
        try:
            df = ts.fund_nav(ts_code=self._code(code))
            if df is None or df.empty:
                return None
            latest = df.iloc[-1]
            return {
                "nav": float(latest.get("NAV", 0)),
                "nav_date": str(latest.get("trade_date", "")),
            }
        except Exception as e:
            logger.debug(f"获取 {code} 净值失败: {e}")
            return None

    async def get_market_overview(self) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(self._fetch_overview, retries=2, delay=3),
        )

    def _fetch_overview(self) -> dict:
        ts = self._get_ts()
        indices = {
            "000001.SH": "上证指数",
            "399001.SZ": "深证成指",
            "399006.SZ": "创业板指",
            "000688.SH": "科创50",
        }
        result = {}
        try:
            today = datetime.now().strftime("%Y%m%d")
            df = ts.daily(ts_code=",".join(indices.keys()), start_date=today, end_date=today)
            if df is None or df.empty:
                return result
            for ts_code, name in indices.items():
                row = df[df["ts_code"] == ts_code]
                if not row.empty:
                    r = row.iloc[0]
                    result[name] = {
                        "price": float(r.get("close", 0)),
                        "change_pct": float(r.get("pct_chg", 0)),
                    }
        except Exception as e:
            logger.warning(f"获取大盘数据失败: {e}")
        return result
