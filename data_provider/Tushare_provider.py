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


class TuShareProvider:
    """基于 TuShare 的 ETF 数据提供者（兼容原接口名称）"""

    def __init__(self):
        self._ts = None
        self._token = None

    def _get_ts(self):
        if self._ts is None:
            import tushare as ts

            self._token = os.getenv("TUSHARE_TOKEN", "")
            if not self._token:
                raise RuntimeError("TUSHARE_TOKEN 未配置，请在 GitHub Secrets 中添加 TUSHARE_TOKEN")

            proxy = os.getenv("TUSHARE_PROXY", "").rstrip("/")
            if proxy:
                os.environ["HTTP_PROXY"] = proxy
                os.environ["HTTPS_PROXY"] = proxy
                os.environ["http_proxy"] = proxy
                os.environ["https_proxy"] = proxy
                logger.info(f"使用代理: {proxy}")

            ts.set_token(self._token)
            self._ts = ts.pro_api(self._token)
            logger.info("TuShare 连接初始化成功")
        return self._ts

    def _ts_code(self, code: str) -> str:
        if code.startswith(("5", "11", "13")):
            return f"{code}.SH"
        else:
            return f"{code}.SZ"

    async def get_realtime_quote(self, code: str) -> Optional[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: _with_retry(
                self._fetch_realtime_quote, code, retries=3, delay=5
            ),
        )

    def _fetch_realtime_quote(self, code: str) -> Optional[dict]:
        ts = self._get_ts()
        ts_code = self._ts_code(code)

        try:
            today = datetime.now().strftime("%Y%m%d")
            df = ts.pro_bar(
                ts_code=ts_code,
                start_date=today,
                end_date=today,
                adj="hfq",
            )

            if df is None or df.empty:
                df = ts.daily(ts_code=ts_code, start_date=today, end_date=today)

            if df is None or df.empty:
                logger.warning(f"[{code}] 未找到实时行情数据")
                return None

            r = df.iloc[0]
            close = float(r.get("close", 0))
            pre_close = float(r.get("pre_close", close))
            change = float(r.get("change", 0)) if r.get("change") is not None else (close - pre_close)
            pct_chg = float(r.get("pct_chg", 0)) if r.get("pct_chg") is not None else 0.0

            amount = r.get("amount", 0)
            turnover = float(amount) * 10000 if amount is not None else 0.0
            vol = r.get("vol", 0)
            volume = float(vol) * 100 if vol is not None else 0.0

            return {
                "code": code,
                "name": str(r.get("name", f"ETF-{code}")),
                "price": close,
                "open": float(r.get("open", close)),
                "high": float(r.get("high", close)),
                "low": float(r.get("low", close)),
                "prev_close": pre_close,
                "volume": volume,
                "turnover": turnover,
                "change_pct": pct_chg,
                "change_amt": change,
                "amplitude": 0.0,
                "turnover_rate": float(r.get("turnover_rate", 0)) if r.get("turnover_rate") is not None else 0.0,
                "pe_ratio": None,
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
        ts = self._get_ts()
        ts_code = self._ts_code(code)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 30)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        try:
            df = ts.pro_bar(
                ts_code=ts_code,
                start_date=start_str,
                end_date=end_str,
                freq="D",
                adj="hfq",
            )

            if df is None or df.empty:
                try:
                    df = ts.fund_bar(
                        ts_code=ts_code,
                        start_date=start_str,
                        end_date=end_str,
                        adj="hfq",
                    )
                except Exception:
                    df = None

            if df is None or df.empty:
                logger.warning(f"[{code}] 未获取到历史数据")
                return None

            col_map = {
                "trade_date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "amount": "turnover",
                "pct_chg": "change_pct",
                "change": "change_amt",
            }
            rename_cols = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_cols)

            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            elif "trade_date" in df.columns:
                df["date"] = pd.to_datetime(df["trade_date"], errors="coerce")

            df = df.dropna(subset=["date"])
            df = df.sort_values("date").tail(days).reset_index(drop=True)

            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            logger.info(f"[{code}] 历史数据获取成功，共 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"获取 {code} 历史数据失败: {e}")
            return None

    async def get_etf_nav(self, code: str) -> Optional[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._fetch_etf_nav, code)

    def _fetch_etf_nav(self, code: str) -> Optional[dict]:
        ts = self._get_ts()
        try:
            df = ts.fund_nav(ts_code=self._ts_code(code))
            if df is None or df.empty:
                return None
            latest = df.iloc[-1]
            nav = float(latest.get("NAV", 0))
            nav_date = str(latest.get("trade_date", ""))
            return {"nav": nav, "nav_date": nav_date}
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
        ts = self._get_ts()
        indices = {
            "000001.SH": "上证指数",
            "399001.SZ": "深证成指",
            "399006.SZ": "创业板指",
            "000688.SH": "科创50",
        }
        result = {}
        try:
            ts_codes = ",".join(indices.keys())
            df = ts.daily(ts_code=ts_codes)
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
