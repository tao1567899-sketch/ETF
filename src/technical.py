"""
技术分析模块（优化版 v2）
计算 MA、RSI、MACD、布林带、KDJ、ADX、OBV 等技术指标
优化：权重调整、双向量能评估、MACD柱状图趋势、ADX趋势强度、OBV能量潮
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np

from src.config import config
from src.logger import logger


@dataclass
class TechnicalIndicators:
    """技术指标计算结果"""

    # 移动平均线
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    ma120: Optional[float] = None
    ma_trend: str = "unknown"  # bullish / bearish / sideways

    # RSI
    rsi14: Optional[float] = None
    rsi_signal: str = "neutral"  # overbought / oversold / neutral

    # MACD
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    macd_cross: str = "none"  # golden / dead / none
    macd_hist_trend: str = "none"  # expanding_bull / shrinking_bull / expanding_bear / shrinking_bear / none

    # 布林带
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: str = "middle"  # upper / lower / middle

    # KDJ
    kdj_k: Optional[float] = None
    kdj_d: Optional[float] = None
    kdj_j: Optional[float] = None
    kdj_cross: str = "none"  # golden / dead / none

    # 量能
    volume_ma5: Optional[float] = None
    volume_ratio: Optional[float] = None  # 量比（当日量 / MA5量）
    volume_trend: str = "normal"  # amplified / shrinking / normal

    # ADX 趋势强度
    adx: Optional[float] = None
    adx_trend: str = "none"  # strong_trend / weak_trend / none

    # OBV 能量潮
    obv: Optional[float] = None
    obv_trend: str = "none"  # rising / falling / none

    # 综合信号
    overall_signal: str = "neutral"  # strong_buy / buy / neutral / sell / strong_sell
    signal_score: float = 0.0  # -100 ~ +100
    signals: List[str] = field(default_factory=list)


class TechnicalAnalyzer:
    """技术分析计算器（优化版）"""

    def __init__(self):
        self.ma_periods = config.ma_periods
        self.rsi_period = config.rsi_period
        self.macd_fast = config.macd_fast
        self.macd_slow = config.macd_slow
        self.macd_signal = config.macd_signal
        self.bb_period = config.bollinger_period
        self.bb_std = config.bollinger_std

    def analyze(self, df: pd.DataFrame) -> TechnicalIndicators:
        """对历史数据进行技术分析，返回指标结果"""
        if df is None or len(df) < 30:
            logger.warning("历史数据不足，无法进行技术分析")
            return TechnicalIndicators()

        result = TechnicalIndicators()
        close = df["close"]
        volume = df["volume"] if "volume" in df.columns else None

        try:
            self._calc_ma(df, close, result)
            self._calc_rsi(close, result)
            self._calc_macd(close, result)
            self._calc_bollinger(close, result)
            self._calc_kdj(df, result)
            if volume is not None:
                self._calc_volume(volume, result)
                self._calc_obv(close, volume, result)
            self._calc_adx(df, result)
            self._calc_overall_signal(result)
        except Exception as e:
            logger.error(f"技术分析计算失败: {e}")

        return result

    def _calc_ma(self, df: pd.DataFrame, close: pd.Series, result: TechnicalIndicators):
        for period in self.ma_periods:
            if len(close) >= period:
                ma_val = float(close.rolling(period).mean().iloc[-1])
                setattr(result, f"ma{period}", round(ma_val, 4))

        # 判断趋势
        price = float(close.iloc[-1])
        ma5, ma20 = result.ma5, result.ma20
        if ma5 and ma20:
            if price > ma5 > ma20:
                result.ma_trend = "bullish"
            elif price < ma5 < ma20:
                result.ma_trend = "bearish"
            else:
                result.ma_trend = "sideways"

    def _calc_rsi(self, close: pd.Series, result: TechnicalIndicators):
        if len(close) < self.rsi_period + 1:
            return
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=self.rsi_period - 1, min_periods=self.rsi_period).mean()
        avg_loss = loss.ewm(com=self.rsi_period - 1, min_periods=self.rsi_period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        result.rsi14 = round(float(rsi.iloc[-1]), 2)
        if result.rsi14 >= 70:
            result.rsi_signal = "overbought"
        elif result.rsi14 <= 30:
            result.rsi_signal = "oversold"
        else:
            result.rsi_signal = "neutral"

    def _calc_macd(self, close: pd.Series, result: TechnicalIndicators):
        if len(close) < self.macd_slow + self.macd_signal:
            return
        ema_fast = close.ewm(span=self.macd_fast, adjust=False).mean()
        ema_slow = close.ewm(span=self.macd_slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=self.macd_signal, adjust=False).mean()
        hist = (dif - dea) * 2

        result.macd = round(float(dif.iloc[-1]), 4)
        result.macd_signal = round(float(dea.iloc[-1]), 4)
        result.macd_hist = round(float(hist.iloc[-1]), 4)

        # 判断金叉/死叉（最近2根）
        if len(dif) >= 2:
            prev_dif, curr_dif = float(dif.iloc[-2]), float(dif.iloc[-1])
            prev_dea, curr_dea = float(dea.iloc[-2]), float(dea.iloc[-1])
            if prev_dif < prev_dea and curr_dif > curr_dea:
                result.macd_cross = "golden"
            elif prev_dif > prev_dea and curr_dif < curr_dea:
                result.macd_cross = "dead"

        # MACD柱状图连续趋势判断
        if len(hist) >= 3:
            h1, h2, h3 = float(hist.iloc[-3]), float(hist.iloc[-2]), float(hist.iloc[-1])
            if h3 > h2 > h1 and h3 > 0:
                result.macd_hist_trend = "expanding_bull"
            elif 0 < h3 < h2 < h1:
                result.macd_hist_trend = "shrinking_bull"
            elif h3 < h2 < h1 and h3 < 0:
                result.macd_hist_trend = "expanding_bear"
            elif h1 < h2 < h3 < 0:
                result.macd_hist_trend = "shrinking_bear"

    def _calc_bollinger(self, close: pd.Series, result: TechnicalIndicators):
        if len(close) < self.bb_period:
            return
        middle = close.rolling(self.bb_period).mean()
        std = close.rolling(self.bb_period).std()
        upper = middle + self.bb_std * std
        lower = middle - self.bb_std * std

        price = float(close.iloc[-1])
        result.bb_upper = round(float(upper.iloc[-1]), 4)
        result.bb_middle = round(float(middle.iloc[-1]), 4)
        result.bb_lower = round(float(lower.iloc[-1]), 4)

        band_range = result.bb_upper - result.bb_lower
        result.bb_width = round(band_range / result.bb_middle * 100, 2) if result.bb_middle else None

        if band_range > 0:
            pos = (price - result.bb_lower) / band_range
            if pos > 0.85:
                result.bb_position = "upper"
            elif pos < 0.15:
                result.bb_position = "lower"
            else:
                result.bb_position = "middle"

    def _calc_kdj(self, df: pd.DataFrame, result: TechnicalIndicators):
        if "high" not in df.columns or len(df) < 9:
            return
        period = 9
        low_min = df["low"].rolling(period).min()
        high_max = df["high"].rolling(period).max()
        rsv = (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan) * 100
        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        j = 3 * k - 2 * d

        result.kdj_k = round(float(k.iloc[-1]), 2)
        result.kdj_d = round(float(d.iloc[-1]), 2)
        result.kdj_j = round(float(j.iloc[-1]), 2)

        if len(k) >= 2:
            if float(k.iloc[-2]) < float(d.iloc[-2]) and float(k.iloc[-1]) > float(d.iloc[-1]):
                result.kdj_cross = "golden"
            elif float(k.iloc[-2]) > float(d.iloc[-2]) and float(k.iloc[-1]) < float(d.iloc[-1]):
                result.kdj_cross = "dead"

    def _calc_volume(self, volume: pd.Series, result: TechnicalIndicators):
        if len(volume) < 5:
            return
        vol_ma5 = volume.rolling(5).mean()
        result.volume_ma5 = round(float(vol_ma5.iloc[-1]), 0)
        curr_vol = float(volume.iloc[-1])
        if result.volume_ma5 and result.volume_ma5 > 0:
            result.volume_ratio = round(curr_vol / result.volume_ma5, 2)
            if result.volume_ratio >= 2.0:
                result.volume_trend = "amplified"
            elif result.volume_ratio <= 0.5:
                result.volume_trend = "shrinking"

    def _calc_adx(self, df: pd.DataFrame, result: TechnicalIndicators):
        """计算 ADX 趋势强度指标"""
        if "high" not in df.columns or "low" not in df.columns or len(df) < 28:
            return
        try:
            high = df["high"]
            low = df["low"]
            close = df["close"]
            period = 14

            # True Range
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # Directional Movement
            up_move = high - high.shift(1)
            down_move = low.shift(1) - low
            plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0), index=df.index)
            minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0), index=df.index)

            # Smoothed averages
            atr = tr.ewm(span=period, adjust=False).mean()
            plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
            minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)

            # ADX
            dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
            adx = dx.ewm(span=period, adjust=False).mean()

            result.adx = round(float(adx.iloc[-1]), 2)
            if result.adx >= 25:
                result.adx_trend = "strong_trend"
            else:
                result.adx_trend = "weak_trend"
        except Exception as e:
            logger.debug(f"ADX计算失败: {e}")

    def _calc_obv(self, close: pd.Series, volume: pd.Series, result: TechnicalIndicators):
        """计算 OBV 能量潮"""
        if len(close) < 10:
            return
        try:
            direction = np.sign(close.diff())
            obv = (volume * direction).cumsum()
            result.obv = round(float(obv.iloc[-1]), 0)

            # OBV 趋势：最近5日 OBV 的线性回归斜率
            if len(obv) >= 5:
                recent_obv = obv.iloc[-5:].values
                x = np.arange(5)
                slope = np.polyfit(x, recent_obv, 1)[0]
                if slope > 0:
                    result.obv_trend = "rising"
                elif slope < 0:
                    result.obv_trend = "falling"
        except Exception as e:
            logger.debug(f"OBV计算失败: {e}")

    def _calc_overall_signal(self, result: TechnicalIndicators):
        """综合评分（优化权重 v2）"""
        score = 0.0
        signals = []

        # ── MA趋势 (权重 20) ──────────────────────────────────────────────
        if result.ma_trend == "bullish":
            score += 20
            signals.append("✅ 均线多头排列")
        elif result.ma_trend == "bearish":
            score -= 20
            signals.append("❌ 均线空头排列")

        # MA120 长期趋势加成
        if result.ma120 is not None and result.ma5 is not None:
            if result.ma5 > result.ma120:
                score += 3
            elif result.ma5 < result.ma120:
                score -= 3

        # ── RSI (权重 12) ─────────────────────────────────────────────────
        if result.rsi_signal == "oversold":
            score += 12
            signals.append(f"✅ RSI超卖({result.rsi14:.1f})，可能反弹")
        elif result.rsi_signal == "overbought":
            score -= 12
            signals.append(f"⚠️ RSI超买({result.rsi14:.1f})，注意回调")

        # ── MACD (权重 22) ────────────────────────────────────────────────
        if result.macd_cross == "golden":
            score += 22
            signals.append("✅ MACD金叉，上涨信号")
        elif result.macd_cross == "dead":
            score -= 22
            signals.append("❌ MACD死叉，下跌信号")
        elif result.macd_hist and result.macd_hist > 0:
            score += 8
        elif result.macd_hist and result.macd_hist < 0:
            score -= 8

        # MACD柱状图趋势加成
        if result.macd_hist_trend == "expanding_bull":
            score += 5
            signals.append("✅ MACD红柱持续放大，多头动能增强")
        elif result.macd_hist_trend == "expanding_bear":
            score -= 5
            signals.append("❌ MACD绿柱持续放大，空头动能增强")
        elif result.macd_hist_trend == "shrinking_bear":
            score += 3
            signals.append("✅ MACD绿柱缩短，空头动能减弱")
        elif result.macd_hist_trend == "shrinking_bull":
            score -= 3
            signals.append("⚠️ MACD红柱缩短，多头动能减弱")

        # ── KDJ (权重 18) ─────────────────────────────────────────────────
        if result.kdj_cross == "golden":
            score += 18
            signals.append("✅ KDJ金叉，短期看多")
        elif result.kdj_cross == "dead":
            score -= 18
            signals.append("❌ KDJ死叉，短期看空")
        elif result.kdj_j and result.kdj_j < 10:
            score += 10
            signals.append(f"✅ KDJ-J极度超卖({result.kdj_j:.1f})")
        elif result.kdj_j and result.kdj_j > 90:
            score -= 10
            signals.append(f"⚠️ KDJ-J极度超买({result.kdj_j:.1f})")

        # ── 布林带 (权重 12) ──────────────────────────────────────────────
        if result.bb_position == "lower":
            score += 12
            signals.append("✅ 触及布林带下轨，支撑位")
        elif result.bb_position == "upper":
            score -= 12
            signals.append("⚠️ 触及布林带上轨，压力位")

        # ── 量能 (权重 16，双向评估) ─────────────────────────────────────
        if result.volume_trend == "amplified":
            if score > 0:
                score += 16
                signals.append("✅ 放量上涨，趋势增强")
            elif score < 0:
                score -= 16
                signals.append("❌ 放量下跌，风险加大")
            else:
                signals.append("ℹ️ 放量，关注方向选择")
        elif result.volume_trend == "shrinking":
            if score > 0:
                score -= 5
                signals.append("⚠️ 缩量上涨，动能不足")
            elif score < 0:
                score += 5
                signals.append("✅ 缩量下跌，抛压减弱")

        # ── ADX 趋势强度（辅助信号） ─────────────────────────────────────
        if result.adx is not None:
            if result.adx_trend == "strong_trend":
                # 强趋势时信号可信度更高，放大评分
                if abs(score) > 10:
                    amplifier = round(score * 0.1, 1)
                    score += amplifier
                    signals.append(f"📊 ADX={result.adx:.0f}，趋势明确，信号增强")
            elif result.adx_trend == "weak_trend" and abs(score) > 20:
                # 弱趋势时信号可靠性降低
                dampener = round(score * -0.1, 1)
                score += dampener
                signals.append(f"📊 ADX={result.adx:.0f}，趋势不明，信号衰减")

        # ── OBV 资金流向（辅助信号） ─────────────────────────────────────
        if result.obv_trend == "rising" and score > 0:
            score += 3
            signals.append("✅ OBV上升，资金持续流入")
        elif result.obv_trend == "falling" and score < 0:
            score -= 3
            signals.append("❌ OBV下降，资金持续流出")
        elif result.obv_trend == "rising" and score < 0:
            signals.append("ℹ️ OBV上升但技术面偏空，关注背离")
        elif result.obv_trend == "falling" and score > 0:
            signals.append("ℹ️ OBV下降但技术面偏多，关注背离")

        # ── 限制评分范围 ─────────────────────────────────────────────────
        score = max(-100, min(100, score))

        result.signal_score = round(score, 1)
        result.signals = signals

        if score >= 50:
            result.overall_signal = "strong_buy"
        elif score >= 20:
            result.overall_signal = "buy"
        elif score <= -50:
            result.overall_signal = "strong_sell"
        elif score <= -20:
            result.overall_signal = "sell"
        else:
            result.overall_signal = "neutral"

    @staticmethod
    def signal_to_emoji(signal: str) -> str:
        mapping = {
            "strong_buy": "🚀 强烈看多",
            "buy": "📈 看多",
            "neutral": "➡️ 中性",
            "sell": "📉 看空",
            "strong_sell": "🔴 强烈看空",
        }
        return mapping.get(signal, "❓ 未知")
