"""
推送分发器
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
import base64
import urllib.parse
from typing import List, Optional, Callable, Any

import httpx

from src.config import config
from src.logger import logger
from src.models import ETFAnalysisResult
from src.technical import TechnicalAnalyzer


def _f(v, fmt=".4f"):
    if v is None:
        return "N/A"
    try:
        return format(v, fmt)
    except Exception:
        return str(v)


def _build_summary_message(results: List[ETFAnalysisResult]) -> str:
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"📊 国内ETF日报 {now}", f"共分析 {len(results)} 只ETF\n"]
    for r in results:
        sig = TechnicalAnalyzer.signal_to_emoji(r.overall_signal or "neutral")
        lines.append(f"{sig} {r.name}({r.code}) {(r.change_pct or 0):+.2f}% 评分{(r.signal_score or 0):+.0f} [{r.overall_signal or 'neutral'}]")
    if results:
        top = max(results, key=lambda x: x.change_pct or 0)
        bot = min(results, key=lambda x: x.change_pct or 0)
        lines.append(f"\n涨幅最大: {top.name} {(top.change_pct or 0):+.2f}%")
        lines.append(f"跌幅最大: {bot.name} {(bot.change_pct or 0):+.2f}%")
    return "\n".join(lines)


def _build_markdown_message(results: List[ETFAnalysisResult]) -> str:
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"# 📊 国内ETF智能日报", f"> {now} | 共 {len(results)} 只ETF", ""]
    lines += ["| 代码 | 名称 | 涨跌幅 | 评分 | 信号 |", "|------|------|--------|------|------|"]
    for r in results:
        sig = TechnicalAnalyzer.signal_to_emoji(r.overall_signal or "neutral")
        lines.append(f"| {r.code} | {r.name} | {(r.change_pct or 0):+.2f}% | {(r.signal_score or 0):+.0f} | {sig} |")
    lines.append("")
    for r in results:
        sig = TechnicalAnalyzer.signal_to_emoji(r.overall_signal or "neutral")
        lines += ["---", f"## {sig} {r.name}（{r.code}）", f"> 跟踪指数：**{r.index or '未知'}** | 类型：{r.etf_type or '其他'}", ""]
        lines += ["### 📈 今日行情",
            f"- 最新价：**{(r.price or 0):.4f}** 元",
            f"- 涨跌幅：{(r.change_pct or 0):+.2f}%",
            f"- 成交额：{(r.turnover or 0)/1e8:.2f} 亿",
            f"- 换手率：{(r.turnover_rate or 0):.2f}%", ""]
        lines += ["### 🔬 技术指标",
            f"- MA5/10/20/60/120：{_f(r.ma5)}/{_f(r.ma10)}/{_f(r.ma20)}/{_f(r.ma60)}/{_f(r.ma120)}",
            f"- RSI(14)：{_f(r.rsi14)} [{r.rsi_signal or 'neutral'}]",
            f"- MACD：DIF={_f(r.macd)} DEA={_f(r.macd_signal_line)} 柱={_f(r.macd_hist)} [{r.macd_cross or 'none'}]",
            f"- KDJ：K={_f(r.kdj_k)} D={_f(r.kdj_d)} J={_f(r.kdj_j)} [{r.kdj_cross or 'none'}]",
            f"- 量比：{_f(r.volume_ratio)} [{r.volume_trend or 'normal'}]",
            f"**综合信号**：{sig} {r.overall_signal or 'neutral'}（评分：{(r.signal_score or 0):+.1f}）", ""]
        if r.signals:
            lines.append("")
            for s in r.signals:
                lines.append(f"- {s}")
        if r.llm_analysis:
            lines += ["", "### 🤖 AI 智能分析", r.llm_analysis]
        else:
            lines += ["", "### 🤖 AI 智能分析", "_暂无AI分析内容_"]
        lines.append("")
    lines += ["---", "_本分析由ETF智能分析系统自动生成，仅供参考，不构成投资建议。_"]
    return "\n".join(lines)


async def _send_with_retry(send_func: Callable, *args, max_retries: int = 2, channel_name: str = "unknown") -> bool:
    for attempt in range(max_retries + 1):
        try:
            if await send_func(*args):
                return True
        except Exception as e:
            logger.warning(f"{channel_name} 第{attempt + 1}次失败: {e}")
        if attempt < max_retries:
            await asyncio.sleep(2 ** attempt)
    return False


class FeishuNotifier:
    async def send(self, text: str, markdown: str = "") -> bool:
        if not config.feishu_webhook:
            return False
        try:
            content = (text or markdown or "")[:500]
            payload = {"msg_type": "text", "content": {"text": content}}
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    config.feishu_webhook,
                    headers={"Content-Type": "application/json"},
                    json=payload,
                )
                data = resp.json()
                if data.get("code") == 0 or data.get("StatusCode") == 0:
                    logger.info("飞书推送成功")
                    return True
                logger.warning(f"飞书推送失败: {data}")
        except Exception as e:
            logger.error(f"飞书推送异常: {e}")
        return False


class NotifyDispatcher:
    def __init__(self):
        self.feishu = FeishuNotifier()

    async def send_all(self, results: List[ETFAnalysisResult]):
        if not results:
            return
        text = _build_summary_message(results)
        md = _build_markdown_message(results)
        tasks = []
        if config.feishu_webhook:
            tasks.append(_send_with_retry(self.feishu.send, text, md, channel_name="飞书"))
        if tasks:
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"推送完成: {[o for o in outcomes if o]}")
