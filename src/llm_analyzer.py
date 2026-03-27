"""
LLM 智能分析模块
调用 DeepSeek / OpenAI 兼容接口生成 ETF 分析报告
"""

from __future__ import annotations

import json
import os
from typing import Optional

import httpx

from src.config import config
from src.logger import logger


MINIMAX_GROUP_ID = os.getenv("MINIMAX_GROUP_ID", "")

SYSTEM_PROMPT = """你是一位专业的国内ETF基金分析师，拥有深厚的技术分析和基本面研究经验。
你的分析风格：
1. 客观中立，基于数据说话
2. 结合技术指标和市场环境进行综合研判
3. 明确指出风险和机会
4. 语言简洁专业，避免过度乐观或悲观
5. 每次分析都要给出明确的操作建议

重要声明：你的分析仅供参考，不构成投资建议，投资者需自行判断风险。"""


def build_analysis_prompt(etf_data: dict) -> str:
    """构建ETF分析提示词"""
    code = etf_data.get("code", "")
    name = etf_data.get("name", "")
    quote = etf_data.get("quote", {})
    tech = etf_data.get("technical", {})
    market = etf_data.get("market_overview", {})
    registry = etf_data.get("registry_info", {})

    # 格式化技术指标
    signals_text = "\n".join(tech.get("signals", [])) or "无明显信号"

    market_text = "\n".join(
        [f"- {name}: {info['price']:.2f} ({info['change_pct']:+.2f}%)"
         for name, info in market.items()]
    ) if market else "获取失败"

    prompt = f"""请对以下ETF进行专业分析：

## 基本信息
- 代码: {code}
- 名称: {name}
- 跟踪指数: {registry.get('index', '未知')}
- 类型: {registry.get('type', '未知')}

## 今日行情
- 最新价: {quote.get('price', 0):.4f} 元
- 涨跌幅: {quote.get('change_pct', 0):+.2f}%
- 成交额: {quote.get('turnover', 0)/1e8:.2f} 亿元
- 换手率: {quote.get('turnover_rate', 0):.2f}%
- 振幅: {quote.get('amplitude', 0):.2f}%

## 技术指标
- 均线: MA5={tech.get('ma5', 'N/A')}, MA10={tech.get('ma10', 'N/A')}, MA20={tech.get('ma20', 'N/A')}, MA60={tech.get('ma60', 'N/A')}, MA120={tech.get('ma120', 'N/A')}
- 均线趋势: {tech.get('ma_trend', 'N/A')}
- RSI(14): {tech.get('rsi14', 'N/A')} [{tech.get('rsi_signal', 'N/A')}]
- MACD: DIF={tech.get('macd', 'N/A')}, DEA={tech.get('macd_signal', 'N/A')}, 柱={tech.get('macd_hist', 'N/A')} [{tech.get('macd_cross', 'none')}] 柱趋势:[{tech.get('macd_hist_trend', 'none')}]
- 布林带: 上={tech.get('bb_upper', 'N/A')}, 中={tech.get('bb_middle', 'N/A')}, 下={tech.get('bb_lower', 'N/A')} [位置:{tech.get('bb_position', 'N/A')}]
- KDJ: K={tech.get('kdj_k', 'N/A')}, D={tech.get('kdj_d', 'N/A')}, J={tech.get('kdj_j', 'N/A')} [{tech.get('kdj_cross', 'none')}]
- 量比: {tech.get('volume_ratio', 'N/A')} [{tech.get('volume_trend', 'N/A')}]
- ADX趋势强度: {tech.get('adx', 'N/A')} [{tech.get('adx_trend', 'N/A')}]
- OBV资金流向: [{tech.get('obv_trend', 'N/A')}]

## 技术信号汇总
- 综合评分: {tech.get('signal_score', 0):.1f}/100
- 综合信号: {tech.get('overall_signal', 'neutral')}
{signals_text}

## 大盘环境
{market_text}

---
请按以下结构输出分析报告（控制在600字以内）：

**【行情解读】** （2-3句，描述今日走势特征）

**【技术分析】** （3-4句，解读关键指标含义）

**【风险提示】** （1-2句，指出主要风险）

**【操作建议】** （明确给出：持有/观望/分批买入/减仓，并说明理由和关键价位）
"""
    return prompt


class LLMAnalyzer:
    """LLM 智能分析器"""

    def __init__(self):
        self.api_key = config.llm_api_key
        self.base_url = config.llm_base_url.rstrip("/")
        self.model = config.llm_model
        self.max_tokens = config.llm_max_tokens

    def is_available(self) -> bool:
        return bool(self.api_key)

    async def analyze(self, etf_data: dict) -> Optional[str]:
        """调用LLM生成ETF分析报告"""
        if not self.is_available():
            logger.warning("LLM API Key未配置，跳过AI分析")
            return None

        prompt = build_analysis_prompt(etf_data)

        try:
            # MiniMax 需要在 URL 中附加 GroupId
            url = f"{self.base_url}/chat/completions"
            if MINIMAX_GROUP_ID and "minimax" in self.base_url:
                url = f"{url}?GroupId={MINIMAX_GROUP_ID}"

            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "max_tokens": self.max_tokens,
                        "temperature": 0.3,
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                    },
                )
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                return content.strip()

        except httpx.HTTPStatusError as e:
            logger.error(f"LLM API HTTP错误 {e.response.status_code}: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"LLM分析失败: {e}")
            return None

    async def generate_market_summary(self, results: list) -> Optional[str]:
        """生成多ETF的市场综合点评"""
        if not self.is_available() or not results:
            return None

        summary_data = []
        for r in results:
            summary_data.append(
                f"- {r.get('name', '')}({r.get('code', '')}): "
                f"{r.get('change_pct', 0):+.2f}%, "
                f"信号={r.get('overall_signal', 'neutral')}, "
                f"评分={r.get('signal_score', 0):.0f}"
            )

        prompt = f"""今日分析了以下{len(results)}只国内ETF：

{chr(10).join(summary_data)}

请用100字以内，给出今日ETF市场的整体点评，包括：
1. 市场整体情绪（乐观/谨慎/悲观）
2. 哪类ETF值得重点关注
3. 一句话核心观点
"""
        try:
            url = f"{self.base_url}/chat/completions"
            if MINIMAX_GROUP_ID and "minimax" in self.base_url:
                url = f"{url}?GroupId={MINIMAX_GROUP_ID}"
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "max_tokens": 300,
                        "temperature": 0.3,
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                    },
                )
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.error(f"生成市场综合点评失败: {e}")
            return None
