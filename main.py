import os
import sys
import logging
import argparse
from datetime import datetime
from data_collector import DataCollector
from notion_updater import NotionUpdater
from scheduler import EventScheduler
from config import LOG_FILE, LOG_LEVEL

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
    
    if task_type == "weekly":
        logger.info("Running weekly collection task once")
        events = collector.collect_weekly_events()
    elif task_type == "daily":
        logger.info("Running daily collection task once")
        events = collector.collect_daily_events()
    else:
        logger.error(f"Unknown task type: {task_type}")
        return
    
    created_count = updater.update_notion_with_events(events)
    logger.info(f"Task completed. Created {created_count} new events in Notion")

def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="美股市场重大事件自动收集与Notion更新系统")
    parser.add_argument("--run-once", choices=["weekly", "daily"], help="立即运行一次任务 (weekly/daily)")
    parser.add_argument("--daemon", action="store_true", help="以守护进程模式运行定时任务")
    
    args = parser.parse_args()
    
    if args.run_once:
        run_once(args.run_once)
    elif args.daemon:
        logger.info("Starting scheduler in daemon mode")
        scheduler = EventScheduler()
        scheduler.run()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()