"""
TuShare HTTP 数据提供者
直接对 TuShare 反向代理发 HTTP 请求获取数据
"""

from __future__ import annotations

import asyncio
import os
import time
import json
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import httpx

from src.logger import logger


def _with_retry(func, retries=3, delay=5, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            result = func(**kwargs)
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
        self._token = None
        self._proxy = None
        self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._token = os.getenv("TUSHARE_TOKEN", "")
            self._proxy = os.getenv("TUSHARE_PROXY", "").rstrip("/")
            if not self._token:
                raise RuntimeError("TUSHARE_TOKEN 未配置，请在 GitHub Secrets 中添加 TUSHARE_TOKEN")
            if self._proxy:
                logger.info(f"使用代理: {self._proxy}")
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    def _code(self, code: str) -> str:
        if code.startswith(("5", "11", "13")):
            return f"{code}.SH"
        return f"{code}.SZ"

    def _url(self, api_name: str) -> str:
        return f"{self._proxy}/{api_name}"

    async def _call(self, api_name: str, params: dict) -> Optional[dict]:
        client = self._get_client()
        payload = {"api_name": api_name, "token": self._token, "params": params, "fields": ""}
        try:
            resp = await client.post(self._url(api_name), json=payload)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                logger.warning(f"API {api_name} 错误: {data.get('msg')}")
                return None
            return data.get("data")
        except Exception as e:
            logger.error(f"API {api_name} 调用失败: {e}")
            return None

    async def get_realtime_quote(self, code: str) -> Optional[dict]:
        today = datetime.now().strftime("%Y%m%d")
        return await _with_retry(self._fetch_realtime, code=code, today=today, retries=3, delay=5)

    async def _fetch_realtime(self, code: str, today: str) -> Optional[dict]:
        ts_code = self._code(code)
        data = await self._call("daily", {"ts_code": ts_code, "start_date": today, "end_date": today})
        if not data or not data.get("items"):
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            data = await self._call("daily", {"ts_code": ts_code, "start_date": yesterday, "end_date": yesterday})
        if not data or not data.get("items"):
            logger.warning(f"[{code}] 未找到行情")
            return None
        fields = data.get("fields", [])
        r = dict(zip(fields, data["items"][0]))
        close = float(r.get("close", 0))
        pre = float(r.get("pre_close", close))
        chg = float(r.get("change", 0)) if r.get("change") is not None else (close - pre)
        pct = float(r.get("pct_chg", 0)) if r.get("pct_chg") is not None else 0.0
        return {
            "code": code,
            "name": str(r.get("name", f"ETF-{code}")),
            "price": close,
            "open": float(r.get("open", close)),
            "high": float(r.get("high", close)),
            "low": float(r.get("low", close)),
            "prev_close": pre,
            "volume": float(r.get("vol", 0) or 0) * 100,
            "turnover": float(r.get("amount", 0) or 0) * 10000,
            "change_pct": pct,
            "change_amt": chg,
            "amplitude": 0.0,
            "turnover_rate": float(r.get("turnover_rate", 0) or 0),
            "pe_ratio": None,
            "timestamp": datetime.now().isoformat(),
        }

    async def get_history(self, code: str, period: str = "daily", days: int = 120) -> Optional[pd.DataFrame]:
        return await _with_retry(self._fetch_history, code=code, period=period, days=days, retries=3, delay=5)

    async def _fetch_history(self, code: str, period: str, days: int) -> Optional[pd.DataFrame]:
        ts_code = self._code(code)
        end = datetime.now()
        start = end - timedelta(days=days + 30)
        data = await self._call("daily", {
            "ts_code": ts_code,
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
        })
        if not data or not data.get("items"):
            logger.warning(f"[{code}] 无历史数据")
            return None
        df = pd.DataFrame(data["items"], columns=data["fields"])
        df = df.rename(columns={"trade_date": "date", "vol": "volume", "amount": "turnover", "pct_chg": "change_pct"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").tail(days).reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        logger.info(f"[{code}] 历史数据 {len(df)} 条")
        return df

    async def get_etf_nav(self, code: str) -> Optional[dict]:
        return await _with_retry(self._fetch_nav, code=code, retries=2, delay=3)

    async def _fetch_nav(self, code: str) -> Optional[dict]:
        data = await self._call("fund_nav", {"ts_code": self._code(code)})
        if not data or not data.get("items"):
            return None
        r = dict(zip(data["fields"], data["items"][-1]))
        return {"nav": float(r.get("NAV", 0)), "nav_date": str(r.get("trade_date", ""))}

    async def get_market_overview(self) -> dict:
        return await _with_retry(self._fetch_overview, retries=2, delay=3)

    async def _fetch_overview(self) -> dict:
        today = datetime.now().strftime("%Y%m%d")
        indices = {"000001.SH": "上证指数", "399001.SZ": "深证成指", "399006.SZ": "创业板指", "000688.SH": "科创50"}
        result = {}
        data = await self._call("daily", {"ts_code": ",".join(indices.keys()), "start_date": today, "end_date": today})
        if not data or not data.get("items"):
            return result
        df = pd.DataFrame(data["items"], columns=data["fields"])
        for ts_code, name in indices.items():
            row = df[df["ts_code"] == ts_code]
            if not row.empty:
                r = row.iloc[0]
                result[name] = {"price": float(r.get("close", 0)), "change_pct": float(r.get("pct_chg", 0))}
        return result
