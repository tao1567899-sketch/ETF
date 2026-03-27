"""
报告生成器
将分析结果保存为 Markdown 文件
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

from src.config import config
from src.logger import logger
from src.models import ETFAnalysisResult


class ReportWriter:
    """Markdown 报告写入器"""

    def __init__(self):
        self.report_dir = Path(config.report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def save_markdown(self, results: List[ETFAnalysisResult]) -> Path:
        """生成并保存 Markdown 分析报告"""
        now = datetime.now()
        filename = f"etf_analysis_{now.strftime('%Y%m%d_%H%M')}.md"
        filepath = self.report_dir / filename

        lines = [
            f"# 国内ETF智能分析报告",
            f"> 生成时间: {now.strftime('%Y-%m-%d %H:%M:%S')}",
            f"> 分析ETF数量: {len(results)}",
            "",
            "---",
            "",
            "## 信号汇总",
            "",
            "| 代码 | 名称 | 最新价 | 涨跌幅 | 评分 | 信号 |",
            "|------|------|--------|--------|------|------|",
        ]

        from src.technical import TechnicalAnalyzer
        for r in results:
            signal_text = TechnicalAnalyzer.signal_to_emoji(r.overall_signal)
            lines.append(
                f"| {r.code} | {r.name} | {r.price:.4f} | "
                f"{r.change_pct:+.2f}% | {r.signal_score:+.1f} | {signal_text} |"
            )

        lines += ["", "---", ""]

        # 详细分析
        for r in results:
            lines.append(r.to_markdown())
            lines.append("---")
            lines.append("")

        content = "\n".join(lines)
        filepath.write_text(content, encoding="utf-8")
        logger.info(f"报告已保存: {filepath}")
        return filepath

    def get_latest_report(self) -> Path | None:
        """获取最新报告路径"""
        reports = sorted(self.report_dir.glob("etf_analysis_*.md"), reverse=True)
        return reports[0] if reports else None
