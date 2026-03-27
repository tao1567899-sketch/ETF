"""
ETF分析结果数据类
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from src.technical import TechnicalAnalyzer


@dataclass
class ETFAnalysisResult:
    """单只ETF的完整分析结果"""

    # 基本信息
    code: str
    name: str
    index: str = ""
    etf_type: str = ""

    # 行情数据
    price: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    prev_close: float = 0.0
    change_pct: float = 0.0
    change_amt: float = 0.0
    volume: float = 0.0
    turnover: float = 0.0
    turnover_rate: float = 0.0
    amplitude: float = 0.0

    # 净值数据
    nav: Optional[float] = None
    nav_date: Optional[str] = None
    premium_discount: Optional[float] = None  # 折溢价率 %

    # 技术指标
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    ma120: Optional[float] = None
    ma_trend: str = "unknown"
    rsi14: Optional[float] = None
    rsi_signal: str = "neutral"
    macd: Optional[float] = None
    macd_signal_line: Optional[float] = None
    macd_hist: Optional[float] = None
    macd_cross: str = "none"
    macd_hist_trend: str = "none"
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_position: str = "middle"
    kdj_k: Optional[float] = None
    kdj_d: Optional[float] = None
    kdj_j: Optional[float] = None
    kdj_cross: str = "none"
    volume_ratio: Optional[float] = None
    volume_trend: str = "normal"
    adx: Optional[float] = None
    adx_trend: str = "none"
    obv: Optional[float] = None
    obv_trend: str = "none"

    # 综合信号
    overall_signal: str = "neutral"
    signal_score: float = 0.0
    signals: List[str] = field(default_factory=list)

    # LLM分析
    llm_analysis: Optional[str] = None

    # 分析时间
    analyzed_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M"))

    # 错误信息
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """转换为字典（用于API响应和序列化）"""
        return {
            "code": self.code,
            "name": self.name,
            "index": self.index,
            "type": self.etf_type,
            "quote": {
                "price": self.price,
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "prev_close": self.prev_close,
                "change_pct": self.change_pct,
                "change_amt": self.change_amt,
                "volume": self.volume,
                "turnover": self.turnover,
                "turnover_rate": self.turnover_rate,
                "amplitude": self.amplitude,
            },
            "nav": {
                "nav": self.nav,
                "nav_date": self.nav_date,
                "premium_discount": self.premium_discount,
            },
            "technical": {
                "ma5": self.ma5,
                "ma10": self.ma10,
                "ma20": self.ma20,
                "ma60": self.ma60,
                "ma120": self.ma120,
                "ma_trend": self.ma_trend,
                "rsi14": self.rsi14,
                "rsi_signal": self.rsi_signal,
                "macd": self.macd,
                "macd_signal": self.macd_signal_line,
                "macd_hist": self.macd_hist,
                "macd_cross": self.macd_cross,
                "macd_hist_trend": self.macd_hist_trend,
                "bb_upper": self.bb_upper,
                "bb_middle": self.bb_middle,
                "bb_lower": self.bb_lower,
                "bb_position": self.bb_position,
                "kdj_k": self.kdj_k,
                "kdj_d": self.kdj_d,
                "kdj_j": self.kdj_j,
                "kdj_cross": self.kdj_cross,
                "volume_ratio": self.volume_ratio,
                "volume_trend": self.volume_trend,
                "adx": self.adx,
                "adx_trend": self.adx_trend,
                "obv": self.obv,
                "obv_trend": self.obv_trend,
                "overall_signal": self.overall_signal,
                "signal_score": self.signal_score,
                "signals": self.signals,
            },
            "llm_analysis": self.llm_analysis,
            "analyzed_at": self.analyzed_at,
            "error": self.error,
        }

    def to_console_report(self) -> str:
        """生成控制台输出报告"""
        signal_emoji = TechnicalAnalyzer.signal_to_emoji(self.overall_signal)

        lines = [
            f"\n{'='*60}",
            f"  📊 {self.name} ({self.code})",
            f"  跟踪指数: {self.index}  |  类型: {self.etf_type}",
            f"{'='*60}",
            f"  💰 最新价: {self.price:.4f}  涨跌: {self.change_pct:+.2f}%",
            f"  📈 高: {self.high:.4f}  低: {self.low:.4f}  昨收: {self.prev_close:.4f}",
            f"  💹 成交额: {self.turnover/1e8:.2f}亿  换手率: {self.turnover_rate:.2f}%",
        ]

        if self.nav:
            pct = f"({self.premium_discount:+.2f}%)" if self.premium_discount else ""
            lines.append(f"  📋 净值: {self.nav:.4f}  {pct}")

        lines += [
            f"\n  【技术指标】",
            f"  MA5={self.ma5 or 'N/A'}  MA10={self.ma10 or 'N/A'}  MA20={self.ma20 or 'N/A'}  MA60={self.ma60 or 'N/A'}  MA120={self.ma120 or 'N/A'}",
            f"  RSI14={self.rsi14 or 'N/A'} [{self.rsi_signal}]",
            f"  MACD: DIF={self.macd or 'N/A'}  DEA={self.macd_signal_line or 'N/A'}  [{self.macd_cross}] 柱趋势:[{self.macd_hist_trend}]",
            f"  KDJ: K={self.kdj_k or 'N/A'}  D={self.kdj_d or 'N/A'}  J={self.kdj_j or 'N/A'}  [{self.kdj_cross}]",
            f"  量比: {self.volume_ratio or 'N/A'}  [{self.volume_trend}]",
            f"  ADX: {self.adx or 'N/A'}  [{self.adx_trend}]  OBV趋势: [{self.obv_trend}]",
            f"\n  【综合信号】 {signal_emoji}  (评分: {self.signal_score:+.1f})",
        ]

        for sig in self.signals:
            lines.append(f"  {sig}")

        if self.llm_analysis:
            lines += [
                f"\n  【AI 智能分析】",
                f"  {self.llm_analysis.replace(chr(10), chr(10)+'  ')}",
            ]

        lines.append(f"\n  分析时间: {self.analyzed_at}")
        lines.append(f"{'='*60}")

        return "\n".join(lines)

    def to_markdown(self) -> str:
        """生成 Markdown 格式报告"""
        signal_emoji = TechnicalAnalyzer.signal_to_emoji(self.overall_signal)
        lines = [
            f"## {self.name} ({self.code})",
            f"> 跟踪指数: **{self.index}** | 类型: {self.etf_type} | 分析时间: {self.analyzed_at}",
            "",
            "### 行情数据",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 最新价 | **{self.price:.4f}** |",
            f"| 涨跌幅 | {self.change_pct:+.2f}% |",
            f"| 成交额 | {self.turnover/1e8:.2f} 亿 |",
            f"| 换手率 | {self.turnover_rate:.2f}% |",
            f"| 振幅 | {self.amplitude:.2f}% |",
        ]

        if self.nav:
            pct = f"{self.premium_discount:+.2f}%" if self.premium_discount else "N/A"
            lines.append(f"| 净值 | {self.nav:.4f} (折溢价: {pct}) |")

        lines += [
            "",
            "### 技术指标",
            f"| 指标 | 数值 | 信号 |",
            f"|------|------|------|",
            f"| MA5/10/20/60/120 | {self.ma5}/{self.ma10}/{self.ma20}/{self.ma60}/{self.ma120} | {self.ma_trend} |",
            f"| RSI(14) | {self.rsi14} | {self.rsi_signal} |",
            f"| MACD DIF/DEA | {self.macd}/{self.macd_signal_line} | {self.macd_cross} / 柱:{self.macd_hist_trend} |",
            f"| KDJ K/D/J | {self.kdj_k}/{self.kdj_d}/{self.kdj_j} | {self.kdj_cross} |",
            f"| 量比 | {self.volume_ratio} | {self.volume_trend} |",
            f"| ADX | {self.adx} | {self.adx_trend} |",
            f"| OBV趋势 | - | {self.obv_trend} |",
            "",
            f"### 综合信号",
            f"**{signal_emoji}** (评分: {self.signal_score:+.1f}/100)",
            "",
        ]

        for sig in self.signals:
            lines.append(f"- {sig}")

        if self.llm_analysis:
            lines += [
                "",
                "### AI 智能分析",
                self.llm_analysis,
            ]

        lines.append("")
        return "\n".join(lines)
