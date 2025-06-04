from data_collector import DataCollector
from notion_updater import NotionUpdater
import logging
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='收集市场事件并更新到Notion')
    parser.add_argument('--daily', action='store_true', help='收集每日事件')
    parser.add_argument('--earnings', action='store_true', help='收集财报事件')
    parser.add_argument('--force', action='store_true', help='强制收集财报事件（即使不是周日）')
    args = parser.parse_args()
    
    # 如果没有指定任何参数，默认收集每日事件
    if not args.daily and not args.earnings:
        args.daily = True
    
    try:
        logger.info("开始数据收集...")
        
        # 初始化收集器和更新器
        collector = DataCollector()
        updater = NotionUpdater()
        
        daily_events = []
        earnings_events = []
        
        # 收集每日事件
        if args.daily:
            logger.info("收集每日事件...")
            daily_events = collector.collect_daily_events()
            logger.info(f"收集到 {len(daily_events)} 个每日事件")
        
        # 收集财报事件
        if args.earnings:
            logger.info("收集财报事件...")
            earnings_events = collector.collect_earnings_events(force=args.force)
            logger.info(f"收集到 {len(earnings_events)} 个财报事件")
        
        # 合并事件并更新Notion
        if daily_events or earnings_events:
            all_events = daily_events + earnings_events
            logger.info(f"开始更新 Notion，共 {len(all_events)} 个事件...")
            updated_count = updater.update_notion_with_events(all_events)
            logger.info(f"成功更新 {updated_count} 个事件到 Notion")
        else:
            logger.info("没有新事件需要更新")
            
    except Exception as e:
        logger.error(f"运行过程中出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 