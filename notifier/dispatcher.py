"""
推送分发器（优化版 v3）
支持：企业微信 / 钉钉 / 飞书 / Telegram / 邮件 / PushPlus（微信）
新增：飞书支持完整AI分析内容，各渠道统一发送完整报告
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


# ── 消息构建 ─────────────────────────────────────────────────────────────────

def _build_summary_message(results: List[ETFAnalysisResult]) -> str:
    """生成推送摘要文本（纯文本版）"""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"📊 国内ETF日报 {now}",
        f"共分析 {len(results)} 只ETF\n",
    ]

    for r in results:
        signal_emoji = TechnicalAnalyzer.signal_to_emoji(r.overall_signal)
        lines.append(
            f"{signal_emoji} {r.name}({r.code}) "
            f"{r.change_pct:+.2f}% 评分{r.signal_score:+.0f} "
            f"[{r.overall_signal}]"
        )

    if results:
        top_gain = max(results, key=lambda r: r.change_pct)
        top_loss = min(results, key=lambda r: r.change_pct)
        lines.append(f"\n涨幅最大: {top_gain.name} {top_gain.change_pct:+.2f}%")
        lines.append(f"跌幅最大: {top_loss.name} {top_loss.change_pct:+.2f}%")

    return "\n".join(lines)


def _build_markdown_message(results: List[ETFAnalysisResult]) -> str:
    """生成完整Markdown报告（包含AI详细分析）"""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"# 📊 国内ETF智能日报",
        f"> {now} | 共 {len(results)} 只ETF",
        "",
    ]

    # 信号总览表格
    lines += [
        "## 📋 信号总览",
        "",
        "| 代码 | 名称 | 涨跌幅 | 评分 | 信号 |",
        "|------|------|--------|------|------|",
    ]

    for r in results:
        signal_text = TechnicalAnalyzer.signal_to_emoji(r.overall_signal)
        lines.append(
            f"| {r.code} | {r.name} | {r.change_pct:+.2f}% | {r.signal_score:+.0f} | {signal_text} |"
        )

    lines.append("")

    # ── 每只ETF的详细分析 ──────────────────────────────────────────────────
    for r in results:
        signal_emoji = TechnicalAnalyzer.signal_to_emoji(r.overall_signal)
        lines += [
            f"---",
            f"## {signal_emoji} {r.name}（{r.code}）",
            f"> 跟踪指数：**{r.index}** | 类型：{r.etf_type} | 分析时间：{r.analyzed_at}",
            "",
        ]

        # 行情数据
        lines += [
            "### 📈 今日行情",
            f"- 最新价：**{r.price:.4f}** 元",
            f"- 涨跌幅：{r.change_pct:+.2f}%",
            f"- 成交额：{r.turnover/1e8:.2f} 亿",
            f"- 换手率：{r.turnover_rate:.2f}%",
            f"- 振幅：{r.amplitude:.2f}%",
            "",
        ]

        # 技术指标
        lines += [
            "### 🔬 技术指标",
            f"- MA5/10/20/60/120：{r.ma5}/{r.ma10}/{r.ma20}/{r.ma60}/{r.ma120}",
            f"- RSI(14)：{r.rsi14:.1f} [{r.rsi_signal}]",
            f"- MACD：DIF={r.macd:.4f} DEA={r.macd_signal_line:.4f} 柱={r.macd_hist:.4f} [{r.macd_cross}]",
            f"- KDJ：K={r.kdj_k:.1f} D={r.kdj_d:.1f} J={r.kdj_j:.1f} [{r.kdj_cross}]",
            f"- 量比：{r.volume_ratio:.2f} [{r.volume_trend}]",
            "",
            f"**综合信号**：{signal_emoji} {r.overall_signal}（评分：{r.signal_score:+.1f}）",
        ]

        # 技术信号列表
        if r.signals:
            lines.append("")
            for sig in r.signals:
                lines.append(f"- {sig}")

        # ── AI 详细分析（核心内容）─────────────────────────────────────────
        if r.llm_analysis:
            lines += [
                "",
                "### 🤖 AI 智能分析",
                r.llm_analysis,
            ]
        else:
            lines += [
                "",
                "### 🤖 AI 智能分析",
                "_暂无AI分析内容（可能为行情数据获取失败）_",
            ]

        lines.append("")

    # 免责声明
    lines += [
        "---",
        "",
        "_本分析由ETF智能分析系统自动生成，仅供参考，不构成投资建议。投资者需自行判断风险。_",
    ]

    return "\n".join(lines)


# ── 重试机制 ─────────────────────────────────────────────────────────────────

async def _send_with_retry(
    send_func: Callable[..., Any],
    *args,
    max_retries: int = 2,
    channel_name: str = "unknown",
) -> bool:
    for attempt in range(max_retries + 1):
        try:
            result = await send_func(*args)
            if result:
                return True
        except Exception as e:
            logger.warning(f"{channel_name} 第{attempt + 1}次推送失败: {e}")
        if attempt < max_retries:
            await asyncio.sleep(2 ** attempt)
    return False


# ── 各渠道推送器 ─────────────────────────────────────────────────────────────

class WeComNotifier:
    async def send(self, text: str) -> bool:
        if not config.wecom_webhook:
            return False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    config.wecom_webhook,
                    json={"msgtype": "text", "text": {"content": text}},
                )
                data = resp.json()
                if data.get("errcode") == 0:
                    logger.info("企业微信推送成功")
                    return True
                logger.warning(f"企业微信推送失败: {data}")
        except Exception as e:
            logger.error(f"企业微信推送异常: {e}")
        return False


class DingTalkNotifier:
    async def send(self, text: str) -> bool:
        if not config.dingtalk_webhook:
            return False
        try:
            headers = {"Content-Type": "application/json"}
            webhook = config.dingtalk_webhook
            if config.dingtalk_secret:
                ts = str(round(time.time() * 1000))
                string_to_sign = f"{ts}\n{config.dingtalk_secret}"
                hmac_code = hmac.new(
                    config.dingtalk_secret.encode("utf-8"),
                    string_to_sign.encode("utf-8"),
                    digestmod=hashlib.sha256,
                ).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
                webhook = f"{webhook}&timestamp={ts}&sign={sign}"
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    webhook,
                    headers=headers,
                    json={"msgtype": "text", "text": {"content": text}},
                )
                data = resp.json()
                if data.get("errcode") == 0:
                    logger.info("钉钉推送成功")
                    return True
                logger.warning(f"钉钉推送失败: {data}")
        except Exception as e:
            logger.error(f"钉钉推送异常: {e}")
        return False


class FeishuNotifier:
    """飞书推送，支持分段发送长报告"""

    async def send(self, text: str, markdown: str = "") -> bool:
        if not config.feishu_webhook:
            return False
        content = markdown if markdown else text
        try:
            headers = {"Content-Type": "application/json"}
            webhook = config.feishu_webhook

            if config.feishu_secret:
                ts = str(int(time.time()))
                string_to_sign = f"{ts}\n{config.feishu_secret}"
                hmac_code = hmac.new(
                    string_to_sign.encode("utf-8"),
                    b"",
                    digestmod=hashlib.sha256,
                ).digest()
                sign = base64.b64encode(hmac_code).decode("utf-8")
            else:
                ts, sign = None, None

            if len(content) > 500:
                return await self._send_long_message(webhook, headers, ts, sign, content)
            else:
                payload = self._build_payload(content, ts, sign)
                return await self._post(webhook, headers, payload)

        except Exception as e:
            logger.error(f"飞书推送异常: {e}")
            return False

    def _build_payload(self, content: str, ts: Optional[str], sign: Optional[str]) -> dict:
        payload = {"msg_type": "text", "content": {"text": content}}
        if ts and sign:
            payload["timestamp"] = ts
            payload["sign"] = sign
        return payload

    async def _send_long_message(
        self, webhook: str, headers: dict, ts: Optional[str], sign: Optional[str], content: str
    ) -> bool:
        chunk_size = 500
        chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]
        for i, chunk in enumerate(chunks):
            prefix = f"📊 ETF分析报告 ({i+1}/{len(chunks)})\n\n" if len(chunks) > 1 else ""
            payload = self._build_payload(prefix + chunk, None, None)
            ok = await self._post(webhook, headers, payload)
            if not ok:
                return False
            if i < len(chunks) - 1:
                await asyncio.sleep(1)
        logger.info(f"飞书分段推送完成，共 {len(chunks)} 段")
        return True

    async def _post(self, webhook: str, headers: dict, payload: dict) -> bool:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(webhook, headers=headers, json=payload)
            data = resp.json()
            if data.get("code") == 0 or data.get("StatusCode") == 0:
                logger.info("飞书推送成功")
                return True
            logger.warning(f"飞书推送失败: {data}")
            return False


class TelegramNotifier:
    async def send(self, text: str, markdown: str = "") -> bool:
        if not config.telegram_bot_token or not config.telegram_chat_id:
            return False
        try:
            url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
            content = markdown if markdown else text
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    url,
                    json={
                        "chat_id": config.telegram_chat_id,
                        "text": content,
                        "parse_mode": "HTML",
                    },
                )
                if resp.status_code == 200:
                    logger.info("Telegram推送成功")
                    return True
                logger.warning(f"Telegram推送失败: {resp.text}")
        except Exception as e:
            logger.error(f"Telegram推送异常: {e}")
        return False


class PushPlusNotifier:
    API_URL = "http://www.pushplus.plus/send"

    async def send(self, title: str, content: str) -> bool:
        if not config.pushplus_token:
            return False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    self.API_URL,
                    json={
                        "token": config.pushplus_token,
                        "title": title,
                        "content": content,
                        "template": "markdown",
                    },
                )
                data = resp.json()
                if data.get("code") == 200:
                    logger.info("PushPlus推送成功")
                    return True
                logger.warning(f"PushPlus推送失败: {data}")
        except Exception as e:
            logger.error(f"PushPlus推送异常: {e}")
        return False


class EmailNotifier:
    async def send(self, subject: str, text: str) -> bool:
        if not all([config.smtp_host, config.smtp_user, config.smtp_pass, config.smtp_to]):
            return False
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.header import Header
            msg = MIMEText(text, "plain", "utf-8")
            msg["Subject"] = Header(subject, "utf-8")
            msg["From"] = config.smtp_user
            msg["To"] = config.smtp_to
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._smtp_send, msg)
            logger.info("邮件推送成功")
            return True
        except Exception as e:
            logger.error(f"邮件推送失败: {e}")
            return False

    def _smtp_send(self, msg):
        import smtplib
        with smtplib.SMTP_SSL(config.smtp_host, config.smtp_port) as server:
            server.login(config.smtp_user, config.smtp_pass)
            server.sendmail(config.smtp_user, config.smtp_to.split(","), msg.as_string())


# ── 推送分发器 ───────────────────────────────────────────────────────────────

class NotifyDispatcher:
    def __init__(self):
        self.wecom = WeComNotifier()
        self.dingtalk = DingTalkNotifier()
        self.feishu = FeishuNotifier()
        self.telegram = TelegramNotifier()
        self.pushplus = PushPlusNotifier()
        self.email = EmailNotifier()

    def _should_send(self, results: List[ETFAnalysisResult]) -> bool:
        level = config.notify_level
        if level == "all":
            return True
        if level == "strong":
            return any(r.overall_signal in ("strong_buy", "strong_sell") for r in results)
        return True

    async def send_all(self, results: List[ETFAnalysisResult]):
        if not results:
            logger.warning("无分析结果，跳过推送")
            return
        if not self._should_send(results):
            logger.info(f"推送级别为 {config.notify_level}，当前无触发信号，跳过推送")
            return

        text_summary = _build_summary_message(results)
        full_report = _build_markdown_message(results)
        subject = f"ETF日报 {results[0].analyzed_at[:10] if results else ''}"

        channels = list(config.notify_channels) if config.notify_channels else []
        if not channels:
            if config.wecom_webhook:
                channels.append("wecom")
            if config.dingtalk_webhook:
                channels.append("dingtalk")
            if config.feishu_webhook:
                channels.append("feishu")
            if config.telegram_bot_token and config.telegram_chat_id:
                channels.append("telegram")
            if config.pushplus_token:
                channels.append("pushplus")
            if all([config.smtp_host, config.smtp_user, config.smtp_pass, config.smtp_to]):
                channels.append("email")

        tasks, channel_names = [], []
        for ch in channels:
            ch = ch.strip().lower()
            if ch == "wecom" and config.wecom_webhook:
                tasks.append(_send_with_retry(self.wecom.send, text_summary, channel_name="企业微信"))
                channel_names.append("企业微信")
            elif ch == "dingtalk" and config.dingtalk_webhook:
                tasks.append(_send_with_retry(self.dingtalk.send, text_summary, channel_name="钉钉"))
                channel_names.append("钉钉")
            elif ch == "feishu" and config.feishu_webhook:
                # 飞书发送完整报告
                tasks.append(_send_with_retry(self.feishu.send, text_summary, full_report, channel_name="飞书"))
                channel_names.append("飞书")
            elif ch == "telegram" and config.telegram_bot_token:
                tasks.append(_send_with_retry(self.telegram.send, text_summary, full_report, channel_name="Telegram"))
                channel_names.append("Telegram")
            elif ch == "pushplus" and config.pushplus_token:
                tasks.append(_send_with_retry(self.pushplus.send, subject, full_report, channel_name="PushPlus"))
                channel_names.append("PushPlus")
            elif ch == "email" and config.smtp_host:
                tasks.append(_send_with_retry(self.email.send, subject, full_report, channel_name="邮件"))
                channel_names.append("邮件")

        if not tasks:
            logger.warning("无已配置的推送渠道，跳过推送")
            return

        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        success, failed = [], []
        for name, outcome in zip(channel_names, outcomes):
            if isinstance(outcome, Exception):
                failed.append(f"{name}(异常:{outcome})")
            elif outcome:
                success.append(name)
            else:
                failed.append(name)
        logger.info(f"推送完成 — 成功: {success or '无'}, 失败: {failed or '无'}")
