#!/usr/bin/env python3
"""
国内ETF智能分析系统 - 主程序入口
支持：行情数据采集 + 技术分析 + LLM智能报告 + 定时推送 + Web仪表盘
"""

import argparse
import asyncio
import sys
from pathlib import Path

# 将项目根目录加入路径
sys.path.insert(0, str(Path(__file__).parent))

from src.config import config
from src.analyzer import ETFAnalyzer
from src.logger import logger


def parse_args():
    parser = argparse.ArgumentParser(description="国内ETF智能分析系统")
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # 分析命令
    analyze_parser = subparsers.add_parser("analyze", help="分析指定ETF")
    analyze_parser.add_argument(
        "--codes",
        nargs="+",
        default=None,
        help="ETF代码列表，如 510050 510300，默认使用配置文件",
    )
    analyze_parser.add_argument(
        "--output",
        choices=["console", "file", "notify", "all"],
        default="console",
        help="输出方式",
    )

    # 服务器命令
    subparsers.add_parser("server", help="启动Web仪表盘服务")

    # 调度命令
    subparsers.add_parser("schedule", help="启动定时任务调度器")

    # 列出ETF命令
    subparsers.add_parser("list", help="列出常见国内ETF")

    return parser.parse_args()


async def run_analyze(args):
    codes = args.codes or config.etf_list
    logger.info(f"开始分析 {len(codes)} 只ETF: {codes}")

    analyzer = ETFAnalyzer()
    results = await analyzer.analyze_batch(codes)

    if args.output in ("console", "all"):
        for result in results:
            print(result.to_console_report())

    if args.output in ("file", "all"):
        from src.reporter import ReportWriter
        writer = ReportWriter()
        path = writer.save_markdown(results)
        logger.info(f"报告已保存至: {path}")

    if args.output in ("notify", "all"):
        from notifier.dispatcher import NotifyDispatcher
        dispatcher = NotifyDispatcher()
        await dispatcher.send_all(results)
        logger.info("推送通知已发送")


def run_server():
    import uvicorn
    from web.app import app
    logger.info("启动Web仪表盘 http://0.0.0.0:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=False)


def run_schedule():
    from scheduler.cron import start_scheduler
    logger.info("启动定时任务调度器...")
    start_scheduler()


def run_list():
    from data_provider.etf_registry import ETF_REGISTRY
    print("\n📋 常见国内ETF列表:\n")
    print(f"{'代码':<10} {'名称':<20} {'跟踪指数':<20} {'类型'}")
    print("-" * 65)
    for code, info in ETF_REGISTRY.items():
        print(f"{code:<10} {info['name']:<20} {info['index']:<20} {info['type']}")
    print()


def main():
    args = parse_args()

    if args.command == "analyze":
        asyncio.run(run_analyze(args))
    elif args.command == "server":
        run_server()
    elif args.command == "schedule":
        run_schedule()
    elif args.command == "list":
        run_list()
    else:
        print("国内ETF智能分析系统 v1.0")
        print("使用 --help 查看帮助")
        print("\n快速开始:")
        print("  python main.py list                          # 查看ETF列表")
        print("  python main.py analyze --codes 510050 510300  # 分析ETF")
        print("  python main.py analyze --output all          # 分析+推送+保存")
        print("  python main.py server                        # 启动Web仪表盘")
        print("  python main.py schedule                      # 启动定时任务")


if __name__ == "__main__":
    main()
