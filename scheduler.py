import time
import logging
import schedule
from datetime import datetime
from config import PRE_MARKET_TIME, POST_MARKET_TIME
from data_collector import DataCollector
from notion_updater import NotionUpdater

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventScheduler:
    def __init__(self):
        self.collector = DataCollector()
        self.updater = NotionUpdater()
    
    def collect_and_update_daily(self):
        """收集当天事件并更新到Notion"""
        logger.info("开始收集当日事件")
        
        # 收集事件
        events = self.collector.collect_daily_events()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"当日任务完成，创建了 {created_count} 个事件")
    
    def collect_and_update_breaking_news(self):
        """收集突发新闻并更新到Notion"""
        logger.info("开始收集突发新闻")
        
        # 收集事件
        events = self.collector.collect_breaking_news()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"突发新闻收集完成，创建了 {created_count} 个事件")
    
    def collect_and_update_earnings(self):
        """收集财报事件并更新到Notion"""
        logger.info("开始收集财报事件")
        
        # 收集事件
        events = self.collector.collect_earnings_events()
        
        # 更新Notion
        created_count = self.updater.update_notion_with_events(events)
        
        logger.info(f"财报事件收集完成，创建了 {created_count} 个事件")
    
    def schedule_tasks(self):
        """设置定时任务"""
        logger.info("设置定时任务")
        
        # 每个交易日盘前收集当天事件
        schedule.every().monday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().tuesday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().wednesday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().thursday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().friday.at(PRE_MARKET_TIME).do(self.collect_and_update_daily)
        logger.info(f"已设置盘前任务，时间: {PRE_MARKET_TIME}")
        
        # 每个交易日盘后收集当天事件
        schedule.every().monday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().tuesday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().wednesday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().thursday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        schedule.every().friday.at(POST_MARKET_TIME).do(self.collect_and_update_daily)
        logger.info(f"已设置盘后任务，时间: {POST_MARKET_TIME}")
        
        # 每2小时收集一次突发新闻
        schedule.every(2).hours.do(self.collect_and_update_breaking_news)
        logger.info("已设置突发新闻收集任务，每2小时执行一次")
        
        # 每天收集一次财报事件
        schedule.every().day.at("07:00").do(self.collect_and_update_earnings)
        logger.info("已设置财报事件收集任务，每天07:00执行")
    
    def run(self):
        """运行调度器"""
        self.schedule_tasks()
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)
            except Exception as e:
                logger.error(f"调度器运行出错: {str(e)}")
                time.sleep(300)  # 出错后等待5分钟再继续

# 测试代码
if __name__ == "__main__":
    # 测试立即执行一次任务
    scheduler = EventScheduler()
    print("Testing daily collection...")
    scheduler.collect_and_update_daily()
    
    # 不启动定时任务，仅用于测试
    # scheduler.run()