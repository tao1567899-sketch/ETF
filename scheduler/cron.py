"""
定时任务调度器
支持 cron 表达式配置，在 A 股收盘后自动运行分析
"""

import asyncio
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from src.config import config
from src.logger import logger


async def run_daily_analysis():
    """每日定时分析任务"""
    from src.analyzer import ETFAnalyzer
    from src.reporter import ReportWriter
    from notifier.dispatcher import NotifyDispatcher

    logger.info("🕐 定时任务触发：开始每日ETF分析...")

    analyzer = ETFAnalyzer()
    results = await analyzer.analyze_batch(config.etf_list)

    # 保存报告
    writer = ReportWriter()
    report_path = writer.save_markdown(results)
    logger.info(f"📝 报告已保存: {report_path}")

    # 推送通知
    dispatcher = NotifyDispatcher()
    await dispatcher.send_all(results)

    logger.info(f"✅ 定时分析完成，共分析 {len(results)} 只ETF")


def _run_analysis_sync():
    """同步包装器，供 APScheduler 调用"""
    asyncio.run(run_daily_analysis())


def start_scheduler():
    """启动定时调度器"""
    scheduler = BlockingScheduler(timezone=config.schedule_timezone)

    # 解析 cron 表达式
    cron_parts = config.schedule_cron.split()
    trigger = CronTrigger(
        minute=cron_parts[0] if len(cron_parts) > 0 else "0",
        hour=cron_parts[1] if len(cron_parts) > 1 else "16",
        day=cron_parts[2] if len(cron_parts) > 2 else "*",
        month=cron_parts[3] if len(cron_parts) > 3 else "*",
        day_of_week=cron_parts[4] if len(cron_parts) > 4 else "mon-fri",
        timezone=config.schedule_timezone,
    )

    scheduler.add_job(
        _run_analysis_sync,
        trigger=trigger,
        id="daily_etf_analysis",
        name="每日ETF智能分析",
        max_instances=1,
        replace_existing=True,
    )

    # 优雅退出
    def shutdown(signum, frame):
        logger.info("收到退出信号，正在关闭调度器...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(f"📅 调度器已启动")
    logger.info(f"   Cron表达式: {config.schedule_cron}")
    logger.info(f"   时区: {config.schedule_timezone}")
    logger.info(f"   ETF列表: {config.etf_list}")

    # 显示下次执行时间
    job = scheduler.get_job("daily_etf_analysis")
    if job:
        logger.info(f"   下次执行: {job.next_run_time}")

    scheduler.start()
