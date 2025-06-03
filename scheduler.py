import time
import logging
import schedule
from datetime import datetime
from config import WEEKLY_SCHEDULE_DAY, WEEKLY_SCHEDULE_TIME, PRE_MARKET_TIME, POST_MARKET_TIME
from data_collector import DataCollector
from notion_updater import NotionUpdater

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventScheduler:
    def __init__(self):
        self.collector = DataCollector()
        self.updater = NotionUpdater()
    
    def collect_and_update_weekly(self):
        """收集下周事件并更新到Notion"""
        logger.info("Starting weekly event collection task")
        
        # 收集事件
        events = self.collector.collect_weekly_events()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"Weekly task completed. Created {created_count} new events in Notion")
    
    def collect_and_update_daily(self):
        """收集当天事件并更新到Notion"""
        logger.info("Starting daily event collection task")
        
        # 收集事件
        events = self.collector.collect_daily_events()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"Daily task completed. Created {created_count} new events in Notion")
    
    def collect_and_update_breaking_news(self):
        """收集突发新闻并更新到Notion"""
        logger.info("Starting breaking news collection task")
        
        # 收集事件
        events = self.collector.collect_breaking_news()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"Breaking news task completed. Created {created_count} new events in Notion")
    
    def collect_and_update_earnings(self):
        """收集财报事件并更新到Notion"""
        logger.info("Starting earnings events collection task")
        
        # 收集事件
        events = self.collector.collect_earnings_events()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"Earnings task completed. Created {created_count} new events in Notion")
    
    def collect_and_update_sentiment(self):
        """收集市场情绪并更新到Notion"""
        logger.info("Starting market sentiment collection task")
        
        # 收集事件
        events = self.collector.collect_market_sentiment()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"Market sentiment task completed. Created {created_count} new events in Notion")
    
    def schedule_tasks(self):
        """设置定时任务"""
        logger.info("Setting up scheduled tasks")
        
        # 每周收集下周事件
        schedule.every().sunday.at(WEEKLY_SCHEDULE_TIME).do(self.collect_and_update_weekly)
        logger.info(f"Scheduled weekly task for every {WEEKLY_SCHEDULE_DAY} at {WEEKLY_SCHEDULE_TIME}")
        
        # 每个交易日盘前收集当天事件
        schedule.every().monday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().tuesday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().wednesday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().thursday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().friday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        logger.info(f"Scheduled pre-market task for every trading day at {PRE_MARKET_TIME}")
        
        # 每个交易日盘后收集当天事件
        schedule.every().monday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().tuesday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().wednesday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().thursday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().friday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        logger.info(f"Scheduled post-market task for every trading day at {POST_MARKET_TIME}")
        
        # 每2小时收集一次突发新闻
        schedule.every(2).hours.do(self.collect_and_update_breaking_news)
        logger.info("Scheduled breaking news task every 2 hours")
        
        # 每天收集一次财报事件
        schedule.every().day.at("07:00").do(self.collect_and_update_earnings)
        logger.info("Scheduled earnings events task daily at 07:00")
        
        # 每个交易日开盘前和收盘后收集市场情绪
        schedule.every().monday.at("08:30").do(self.collect_and_update_sentiment)
        schedule.every().monday.at("16:30").do(self.collect_and_update_sentiment)
        schedule.every().tuesday.at("08:30").do(self.collect_and_update_sentiment)
        schedule.every().tuesday.at("16:30").do(self.collect_and_update_sentiment)
        schedule.every().wednesday.at("08:30").do(self.collect_and_update_sentiment)
        schedule.every().wednesday.at("16:30").do(self.collect_and_update_sentiment)
        schedule.every().thursday.at("08:30").do(self.collect_and_update_sentiment)
        schedule.every().thursday.at("16:30").do(self.collect_and_update_sentiment)
        schedule.every().friday.at("08:30").do(self.collect_and_update_sentiment)
        schedule.every().friday.at("16:30").do(self.collect_and_update_sentiment)
        logger.info("Scheduled market sentiment tasks for trading days")
    
    def run(self):
        """运行定时任务"""
        self.schedule_tasks()
        
        logger.info("Starting scheduler. Press Ctrl+C to exit.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")

# 测试代码
if __name__ == "__main__":
    # 测试立即执行一次任务
    scheduler = EventScheduler()
    print("Testing weekly collection...")
    scheduler.collect_and_update_weekly()
    
    print("Testing daily collection...")
    scheduler.collect_and_update_daily()
    
    # 不启动定时任务，仅用于测试
    # scheduler.run()