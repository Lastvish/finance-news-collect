import logging
import json
from datetime import datetime
from notion_client import Client
from config import NOTION_API_KEY, NOTION_DATABASE_ID

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NotionUpdater:
    def __init__(self):
        self.notion_api_key = NOTION_API_KEY
        self.database_id = NOTION_DATABASE_ID
        self.notion = Client(auth=self.notion_api_key)
        
    def _get_existing_events(self):
        """获取Notion数据库中已存在的事件"""
        try:
            logger.info("Fetching existing events from Notion database")
            
            results = self.notion.databases.query(
                database_id=self.database_id
            )
            
            existing_events = []
            for page in results.get("results", []):
                properties = page.get("properties", {})
                
                # 提取事件信息
                event_date = properties.get("日期", {}).get("date", {}).get("start", "")
                event_time = properties.get("时间", {}).get("rich_text", [])
                event_time = event_time[0].get("text", {}).get("content", "") if event_time else ""
                event_description = properties.get("事件描述", {}).get("title", [])
                event_description = event_description[0].get("text", {}).get("content", "") if event_description else ""
                
                existing_events.append({
                    "id": page.get("id"),
                    "date": event_date,
                    "time": event_time,
                    "description": event_description
                })
            
            logger.info(f"Found {len(existing_events)} existing events in Notion")
            return existing_events
            
        except Exception as e:
            logger.error(f"Error fetching existing events: {str(e)}")
            return []
    
    def _is_duplicate_event(self, new_event, existing_events):
        """检查事件是否已存在于Notion数据库中"""
        for event in existing_events:
            # 如果日期和描述相似，则认为是重复事件
            if event["date"] == new_event.get("date", "") and \
               event["description"].lower() == new_event.get("description", "").lower():
                return True
        return False
    
    def _create_event_page(self, event):
        """在Notion数据库中创建新的事件页面"""
        try:
            # 提取事件信息
            date_str = event.get("date", datetime.now().strftime("%Y-%m-%d"))
            time_str = event.get("time", "")
            description = event.get("description", "")
            event_type = event.get("type", "其他")
            market_phase = event.get("market_phase", "其他")
            market_impact = event.get("market_impact", "影响不确定")
            sentiment = event.get("sentiment", "unknown")
            
            # 新增字段
            sector_impact = event.get("sector_impact", "")
            stocks_affected = event.get("stocks_affected", "")
            # 移除历史参考字段
            # historical_reference = event.get("historical_reference", "")
            confidence_level = event.get("confidence_level", "medium")
            source_url = event.get("source_url", "")
            
            # 将sentiment转换为中文显示
            sentiment_display = {
                "bullish": "利好",
                "bearish": "利空",
                "neutral": "中性",
                "unknown": "未知"
            }.get(sentiment.lower(), "未知")
            
            # 将confidence_level转换为中文显示
            confidence_display = {
                "high": "高",
                "medium": "中",
                "low": "低"
            }.get(confidence_level.lower(), "中")
            
            # 打印事件信息，用于调试
            logger.info(f"Creating event: date={date_str}, time={time_str}, description={description}, type={event_type}")
            
            # 创建页面时添加市场影响分析和市场情绪标记
            properties = {
                "事件描述": {
                    "title": [
                        {
                            "text": {
                                "content": description if description else "无描述"
                            }
                        }
                    ]
                },
                "日期": {
                    "date": {
                        "start": date_str
                    }
                },
                "时间": {
                    "rich_text": [
                        {
                            "text": {
                                "content": time_str if time_str else "未指定时间"
                            }
                        }
                    ]
                },
                "事件类型": {
                    "select": {
                        "name": event_type
                    }
                },
                "市场阶段": {
                    "select": {
                        "name": market_phase
                    }
                },
                "市场影响分析": {
                    "rich_text": [
                        {
                            "text": {
                                "content": market_impact
                            }
                        }
                    ]
                },
                "市场情绪": {
                    "select": {
                        "name": sentiment_display
                    }
                },
                "分析确信度": {
                    "select": {
                        "name": confidence_display
                    }
                }
            }
            
            # 添加新字段（如果存在）
            if sector_impact:
                properties["行业影响"] = {
                    "rich_text": [
                        {
                            "text": {
                                "content": str(sector_impact)
                            }
                        }
                    ]
                }
                
            if stocks_affected:
                # 确保stocks_affected是字符串
                if isinstance(stocks_affected, dict):
                    stocks_text = ", ".join([f"{k}: {v}" for k, v in stocks_affected.items()])
                else:
                    stocks_text = str(stocks_affected)
                    
                properties["相关个股"] = {
                    "rich_text": [
                        {
                            "text": {
                                "content": stocks_text
                            }
                        }
                    ]
                }
                
            # 移除历史参考字段的添加
            # if historical_reference:
            #     properties["历史参考"] = {
            #         "rich_text": [
            #             {
            #                 "text": {
            #                     "content": str(historical_reference)
            #                 }
            #             }
            #         ]
            #     }
                
            # 添加信息来源字段
            if source_url:
                properties["信息来源"] = {
                    "rich_text": [
                        {
                            "text": {
                                "content": str(source_url),
                                "link": {
                                    "url": str(source_url) if str(source_url).startswith("http") else None
                                }
                            }
                        }
                    ]
                }
            
            # 创建页面
            new_page = self.notion.pages.create(
                parent={"database_id": self.database_id},
                properties=properties
            )
            
            logger.info(f"Created new event in Notion: {description}")
            return new_page
            
        except Exception as e:
            logger.error(f"Error creating event page: {str(e)}")
            return None
    
    def update_notion_with_events(self, events):
        """将事件更新到Notion数据库"""
        if not events:
            logger.warning("No events to update")
            return 0
        
        logger.info(f"Updating Notion with {len(events)} events")
        
        # 获取已存在的事件
        existing_events = self._get_existing_events()
        
        # 计数器
        created_count = 0
        
        # 添加新事件
        for event in events:
            # 检查是否重复
            if not self._is_duplicate_event(event, existing_events):
                # 创建新事件
                result = self._create_event_page(event)
                if result:
                    created_count += 1
            else:
                logger.info(f"Skipping duplicate event: {event.get('description', '')}")
        
        logger.info(f"Created {created_count} new events in Notion")
        return created_count

# 测试代码
if __name__ == "__main__":
    # 创建测试事件
    test_events = [
        {
            "date": "2023-06-01",
            "time": "14:30",
            "description": "美联储公布利率决议",
            "type": "经济数据"
        },
        {
            "date": "2023-06-02",
            "time": "08:30",
            "description": "美国非农就业数据发布",
            "type": "经济数据"
        }
    ]
    
    # 更新Notion
    updater = NotionUpdater()
    created_count = updater.update_notion_with_events(test_events)
    print(f"Created {created_count} new events in Notion")