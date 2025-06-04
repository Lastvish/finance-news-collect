import os
import sys
import logging
import argparse
from datetime import datetime
from data_collector import DataCollector
from notion_updater import NotionUpdater
from scheduler import EventScheduler
from config import LOG_FILE, LOG_LEVEL
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# 配置日志
log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_once(task_type):
    """立即运行一次任务"""
    collector = DataCollector()
    updater = NotionUpdater()
    
    if task_type == "daily":
        logger.info("运行每日数据收集任务")
        events = collector.collect_daily_events()
    elif task_type == "breaking":
        logger.info("运行突发新闻收集任务")
        events = collector.collect_breaking_news()
    elif task_type == "earnings":
        logger.info("运行财报事件收集任务")
        events = collector.collect_earnings_events()
    else:
        logger.error(f"未知的任务类型: {task_type}")
        return
    
    created_count = updater.update_notion_with_events(events)
    logger.info(f"任务完成，创建了 {created_count} 个事件")

def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="美股市场重大事件自动收集与Notion更新系统")
    parser.add_argument("--run-once", choices=["daily", "breaking", "earnings"], help="立即运行一次任务 (daily/breaking/earnings)")
    parser.add_argument("--daemon", action="store_true", help="以守护进程模式运行定时任务")
    
    args = parser.parse_args()
    
    if args.run_once:
        run_once(args.run_once)
    elif args.daemon:
        logger.info("以守护进程模式启动调度器")
        scheduler = EventScheduler()
        scheduler.run()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()