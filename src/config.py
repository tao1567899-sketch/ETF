"""
配置管理模块 - 统一读取 .env 和环境变量（优化版 v2）
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv

# 加载 .env 文件
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)


@dataclass
class Config:
    # ── LLM 配置 ─────────────────────────────────────────────────────────────
    llm_base_url: str = field(
        default_factory=lambda: os.getenv(
            "LLM_BASE_URL", "https://api.minimax.chat/v1"
        )
    )
    llm_api_key: str = field(
        default_factory=lambda: os.getenv("LLM_API_KEY", "")
    )
    llm_model: str = field(
        default_factory=lambda: os.getenv("LLM_MODEL", "MiniMax-Text-01")
    )
    llm_max_tokens: int = field(
        default_factory=lambda: int(os.getenv("LLM_MAX_TOKENS", "2048"))
    )

    # ── ETF 列表配置（优化默认列表：宽基+AI+红利+黄金+弹性） ────────────────
    etf_list: List[str] = field(
        default_factory=lambda: [
            code.strip()
            for code in os.getenv(
                "ETF_LIST",
                "510050,510300,510500,588000,512930,512480,510880,512890,518880,512400,159915,512880",
            ).split(",")
            if code.strip()
        ]
    )

    # ── 数据源配置 ────────────────────────────────────────────────────────────
    data_source: str = field(
        default_factory=lambda: os.getenv("DATA_SOURCE", "akshare")
    )
    tushare_token: str = field(
        default_factory=lambda: os.getenv("TUSHARE_TOKEN", "")
    )

    # ── 技术分析参数（新增 MA120 半年线） ────────────────────────────────────
    ma_periods: List[int] = field(default_factory=lambda: [5, 10, 20, 60, 120])
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bollinger_period: int = 20
    bollinger_std: float = 2.0

    # ── 推送配置 ──────────────────────────────────────────────────────────────
    notify_channels: List[str] = field(
        default_factory=lambda: [
            ch.strip()
            for ch in os.getenv("NOTIFY_CHANNELS", "").split(",")
            if ch.strip()
        ]
    )
    # 推送级别: all=全部推送, strong=仅强信号, daily=仅日报
    notify_level: str = field(
        default_factory=lambda: os.getenv("NOTIFY_LEVEL", "all")
    )

    # 企业微信
    wecom_webhook: str = field(
        default_factory=lambda: os.getenv("WECOM_WEBHOOK", "")
    )
    # 钉钉
    dingtalk_webhook: str = field(
        default_factory=lambda: os.getenv("DINGTALK_WEBHOOK", "")
    )
    dingtalk_secret: str = field(
        default_factory=lambda: os.getenv("DINGTALK_SECRET", "")
    )
    # 飞书
    feishu_webhook: str = field(
        default_factory=lambda: os.getenv("FEISHU_WEBHOOK", "")
    )
    feishu_secret: str = field(
        default_factory=lambda: os.getenv("FEISHU_SECRET", "")
    )
    # Telegram
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
    # PushPlus（微信推送）
    pushplus_token: str = field(
        default_factory=lambda: os.getenv("PUSHPLUS_TOKEN", "")
    )
    # 邮件
    smtp_host: str = field(default_factory=lambda: os.getenv("SMTP_HOST", ""))
    smtp_port: int = field(
        default_factory=lambda: int(os.getenv("SMTP_PORT", "465"))
    )
    smtp_user: str = field(default_factory=lambda: os.getenv("SMTP_USER", ""))
    smtp_pass: str = field(default_factory=lambda: os.getenv("SMTP_PASS", ""))
    smtp_to: str = field(default_factory=lambda: os.getenv("SMTP_TO", ""))

    # ── 定时任务配置 ──────────────────────────────────────────────────────────
    schedule_cron: str = field(
        default_factory=lambda: os.getenv("SCHEDULE_CRON", "0 16 * * 1-5")
    )
    schedule_timezone: str = field(
        default_factory=lambda: os.getenv("SCHEDULE_TIMEZONE", "Asia/Shanghai")
    )

    # ── 报告配置 ──────────────────────────────────────────────────────────────
    report_dir: str = field(
        default_factory=lambda: os.getenv("REPORT_DIR", "reports")
    )
    report_language: str = field(
        default_factory=lambda: os.getenv("REPORT_LANGUAGE", "zh")
    )

    # ── Web服务配置 ───────────────────────────────────────────────────────────
    web_host: str = field(
        default_factory=lambda: os.getenv("WEB_HOST", "0.0.0.0")
    )
    web_port: int = field(
        default_factory=lambda: int(os.getenv("WEB_PORT", "8080"))
    )

    def validate(self) -> List[str]:
        """验证必要配置，返回警告信息列表"""
        warnings = []
        if not self.llm_api_key:
            warnings.append("⚠️  LLM_API_KEY 未配置，LLM分析功能将不可用")
        if not self.etf_list:
            warnings.append("⚠️  ETF_LIST 为空，将使用默认ETF列表")
        return warnings


# 全局单例
config = Config()
