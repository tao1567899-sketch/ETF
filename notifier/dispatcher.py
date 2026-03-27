"""
推送分发器（优化版 v2）
支持：企业微信 / 钉钉 / 飞书 / Telegram / 邮件 / PushPlus（微信）
优化：Markdown格式推送、分级推送、重试机制、推送结果汇总
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

    strong_buy = [r for r in results if r.overall_signal == "strong_buy"]
    buy = [r for r in results if r.overall_signal == "buy"]
    sell = [r for r in results if r.overall_signal in ("sell", "strong_sell")]

    if strong_buy:
        lines.append("🚀 强烈看多:")
        for r in strong_buy:
            lines.append(f"  {r.name}({r.code}) {r.change_pct:+.2f}% 评分{r.signal_score:+.0f}")

    if buy:
        lines.append("📈 看多:")
        for r in buy:
            lines.append(f"  {r.name}({r.code}) {r.change_pct:+.2f}% 评分{r.signal_score:+.0f}")

    if sell:
        lines.append("📉 看空:")
        for r in sell:
            lines.append(f"  {r.name}({r.code}) {r.change_pct:+.2f}% 评分{r.signal_score:+.0f}")

    # 最大涨跌
    if results:
        top_gain = max(results, key=lambda r: r.change_pct)
        top_loss = min(results, key=lambda r: r.change_pct)
        lines.append(f"\n涨幅最大: {top_gain.name} {top_gain.change_pct:+.2f}%")
        lines.append(f"跌幅最大: {top_loss.name} {top_loss.change_pct:+.2f}%")

    return "\n".join(lines)


def _build_markdown_message(results: List[ETFAnalysisResult]) -> str:
    """生成推送内容（Markdown版，用于 PushPlus / Telegram）"""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"# 📊 国内ETF智能日报",
        f"> {now} | 共 {len(results)} 只ETF",
        "",
        "## 信号总览",
        "",
        "| 代码 | 名称 | 涨跌幅 | 评分 | 信号 |",
        "|------|------|--------|------|------|",
    ]

    for r in results:
        signal_text = TechnicalAnalyzer.signal_to_emoji(r.overall_signal)
        lines.append(
            f"| {r.code} | {r.name} | {r.change_pct:+.2f}% | {r.signal_score:+.0f} | {signal_text} |"
        )

    # 强信号提示
    strong_buy = [r for r in results if r.overall_signal == "strong_buy"]
    strong_sell = [r for r in results if r.overall_signal == "strong_sell"]

    if strong_buy:
        lines += ["", "## 🚀 强烈看多信号", ""]
        for r in strong_buy:
            top_signals = " / ".join(r.signals[:3]) if r.signals else "综合信号"
            lines.append(f"**{r.name}**({r.code}) — {top_signals}")

    if strong_sell:
        lines += ["", "## 🔴 强烈看空信号", ""]
        for r in strong_sell:
            top_signals = " / ".join(r.signals[:3]) if r.signals else "综合信号"
            lines.append(f"**{r.name}**({r.code}) — {top_signals}")

    # 涨跌排行
    if results:
        lines += ["", "## 涨跌排行", ""]
        sorted_by_change = sorted(results, key=lambda r: r.change_pct, reverse=True)
        lines.append(f"📈 最大涨幅: **{sorted_by_change[0].name}** {sorted_by_change[0].change_pct:+.2f}%")
        lines.append(f"📉 最大跌幅: **{sorted_by_change[-1].name}** {sorted_by_change[-1].change_pct:+.2f}%")

    lines += [
        "",
        "---",
        "*本分析由ETF智能分析系统自动生成，仅供参考，不构成投资建议。*",
    ]

    return "\n".join(lines)


# ── 重试机制 ─────────────────────────────────────────────────────────────────

async def _send_with_retry(
    send_func: Callable[..., Any],
    *args,
    max_retries: int = 2,
    channel_name: str = "unknown",
) -> bool:
    """带重试的推送发送"""
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
    """企业微信 Webhook 推送"""

    async def send(self, text: str) -> bool:
        if not config.wecom_webhook:
            return False
        try:
            async with httpx.AsyncClient(timeout=10) as client:
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
    """钉钉机器人推送"""

    async def send(self, text: str) -> bool:
        if not config.dingtalk_webhook:
            return False
        try:
            headers = {"Content-Type": "application/json"}
            webhook = config.dingtalk_webhook

            # 加签
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

            async with httpx.AsyncClient(timeout=10) as client:
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
    """飞书自定义机器人 Webhook 推送（支持富文本）"""

    async def send(self, text: str) -> bool:
        if not config.feishu_webhook:
            return False
        try:
            # 飞书 Webhook 签名验证
            headers = {"Content-Type": "application/json"}
            webhook = config.feishu_webhook
            payload: dict

            if config.feishu_secret:
                ts = str(int(time.time()))
                string_to_sign = f"{ts}\n{config.feishu_secret}"
                hmac_code = hmac.new(
                    string_to_sign.encode("utf-8"),
                    b"",
                    digestmod=hashlib.sha256,
                ).digest()
                sign = base64.b64encode(hmac_code).decode("utf-8")
                payload = {
                    "timestamp": ts,
                    "sign": sign,
                    "msg_type": "text",
                    "content": {"text": text},
                }
            else:
                payload = {
                    "msg_type": "text",
                    "content": {"text": text},
                }

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(webhook, headers=headers, json=payload)
                data = resp.json()
                if data.get("code") == 0 or data.get("StatusCode") == 0:
                    logger.info("飞书推送成功")
                    return True
                logger.warning(f"飞书推送失败: {data}")
        except Exception as e:
            logger.error(f"飞书推送异常: {e}")
        return False


class TelegramNotifier:
    """Telegram Bot 推送（支持 Markdown）"""

    async def send(self, text: str, markdown: str = "") -> bool:
        if not config.telegram_bot_token or not config.telegram_chat_id:
            return False
        try:
            url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
            # Telegram 用 MarkdownV2 或 HTML
            content = markdown if markdown else text
            async with httpx.AsyncClient(timeout=20) as client:
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
    """PushPlus 微信推送（支持 Markdown 模板）"""

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
    """邮件推送（SMTP）"""

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
    """推送分发器，统一调度所有渠道（优化版 v2）"""

    def __init__(self):
        self.wecom = WeComNotifier()
        self.dingtalk = DingTalkNotifier()
        self.feishu = FeishuNotifier()
        self.telegram = TelegramNotifier()
        self.pushplus = PushPlusNotifier()
        self.email = EmailNotifier()

    def _should_send(self, results: List[ETFAnalysisResult]) -> bool:
        """根据 notify_level 判断是否需要推送"""
        level = config.notify_level
        if level == "all":
            return True
        if level == "strong":
            return any(
                r.overall_signal in ("strong_buy", "strong_sell")
                for r in results
            )
        # daily 模式始终发送
        return True

    async def send_all(self, results: List[ETFAnalysisResult]):
        """向所有已配置渠道发送分析结果（带重试和结果汇总）"""
        if not results:
            logger.warning("无分析结果，跳过推送")
            return

        if not self._should_send(results):
            logger.info(f"推送级别为 {config.notify_level}，当前无触发信号，跳过推送")
            return

        text = _build_summary_message(results)
        markdown = _build_markdown_message(results)
        subject = f"ETF日报 {results[0].analyzed_at[:10] if results else ''}"

        channels = config.notify_channels
        tasks = []
        channel_names = []

        # 只推送已配置的渠道（而非所有渠道）
        if not channels:
            # 未显式指定渠道时，自动检测哪些已配置
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

        for ch in channels:
            ch = ch.strip().lower()
            if ch == "wecom" and config.wecom_webhook:
                tasks.append(_send_with_retry(self.wecom.send, text, channel_name="企业微信"))
                channel_names.append("企业微信")
            elif ch == "dingtalk" and config.dingtalk_webhook:
                tasks.append(_send_with_retry(self.dingtalk.send, text, channel_name="钉钉"))
                channel_names.append("钉钉")
            elif ch == "feishu" and config.feishu_webhook:
                tasks.append(_send_with_retry(self.feishu.send, text, channel_name="飞书"))
                channel_names.append("飞书")
            elif ch == "telegram" and config.telegram_bot_token:
                tasks.append(_send_with_retry(self.telegram.send, text, markdown, channel_name="Telegram"))
                channel_names.append("Telegram")
            elif ch == "pushplus" and config.pushplus_token:
                tasks.append(_send_with_retry(self.pushplus.send, subject, markdown, channel_name="PushPlus"))
                channel_names.append("PushPlus")
            elif ch == "email" and config.smtp_host:
                tasks.append(_send_with_retry(self.email.send, subject, text, channel_name="邮件"))
                channel_names.append("邮件")

        if not tasks:
            logger.warning("无已配置的推送渠道，跳过推送")
            return

        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        # 汇总推送结果
        success = []
        failed = []
        for name, outcome in zip(channel_names, outcomes):
            if isinstance(outcome, Exception):
                failed.append(f"{name}(异常:{outcome})")
            elif outcome:
                success.append(name)
            else:
                failed.append(name)

        logger.info(f"推送完成 — 成功: {success or '无'}, 失败: {failed or '无'}")
