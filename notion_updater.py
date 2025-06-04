import logging
import json
from datetime import datetime
from notion_client import Client
from config import NOTION_API_KEY, NOTION_DATABASE_ID, DEEPSEEK_API_KEY
from openai import OpenAI

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NotionUpdater:
    def __init__(self, parent_page_id=None):
        self.notion_api_key = NOTION_API_KEY
        self.database_id = NOTION_DATABASE_ID
        self.parent_page_id = parent_page_id
        self.notion = Client(auth=self.notion_api_key)
        self.client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com"
        )
        
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
    
    def _generate_daily_summary(self, events):
        """生成每日市场事件的总结分析"""
        try:
            if not events:
                return "今日无重要市场事件。"
            
            # 构建分析提示词
            summary_prompt = f"""作为专业的金融分析师，请对以下今日美股市场事件进行全面分析和总结：

事件列表：
{json.dumps(events, ensure_ascii=False, indent=2)}

请提供以下分析：
1. 当日市场主要事件概述
2. 重要经济数据分析
3. 企业财报及重大公告分析
4. 市场情绪评估
5. 潜在市场影响分析
6. 需要重点关注的领域和个股
7. 风险提示

请以精炼报告的形式输出，确保分析深入但简明扼要。"""

            # 调用 DeepSeek API 生成分析
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "你是一个专业的金融分析师，擅长总结和分析市场事件。请提供准确、专业、有见地的分析。"},
                    {"role": "user", "content": summary_prompt}
                ],
                temperature=0.3,
                max_tokens=2000
            )
            
            summary = response.choices[0].message.content
            return summary
            
        except Exception as e:
            logger.error(f"Error generating daily summary: {str(e)}")
            return "生成每日总结时发生错误。"

    def _create_daily_page(self, events):
        """创建每日市场事件页面，包含总结和详细信息"""
        try:
            # 获取当前日期
            today = datetime.now()
            date_str = today.strftime("%Y-%m-%d")
            
            logger.info(f"开始创建每日页面: {date_str}")
            logger.info(f"事件数量: {len(events)}")
            
            # 生成每日总结
            logger.info("开始生成每日总结...")
            daily_summary = self._generate_daily_summary(events)
            logger.info("每日总结生成完成")
            
            # 创建新的页面
            logger.info("开始创建 Notion 页面...")
            new_page = self.notion.pages.create(
                parent={"page_id": self.parent_page_id} if self.parent_page_id else {"database_id": self.database_id},
                properties={
                    "title": {
                        "title": [
                            {
                                "text": {
                                    "content": f"美股市场日报 {date_str}"
                                }
                            }
                        ]
                    }
                },
                children=[
                    {
                        "object": "block",
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": [{"type": "text", "text": {"content": "市场总结"}}]
                        }
                    },
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": daily_summary}}]
                        }
                    },
                    {
                        "object": "block",
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": [{"type": "text", "text": {"content": "详细事件"}}]
                        }
                    },
                    {
                        "object": "block",
                        "type": "table",
                        "table": {
                            "table_width": 7,
                            "has_column_header": True,
                            "has_row_header": False,
                            "children": [
                                {
                                    "type": "table_row",
                                    "table_row": {
                                        "cells": [
                                            [{"type": "text", "text": {"content": "时间"}}],
                                            [{"type": "text", "text": {"content": "事件描述"}}],
                                            [{"type": "text", "text": {"content": "事件类型"}}],
                                            [{"type": "text", "text": {"content": "市场阶段"}}],
                                            [{"type": "text", "text": {"content": "市场影响"}}],
                                            [{"type": "text", "text": {"content": "市场情绪"}}],
                                            [{"type": "text", "text": {"content": "信息来源"}}]
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            )
            logger.info(f"Notion 页面创建成功，ID: {new_page.get('id')}")
            
            # 添加事件到表格
            logger.info("开始添加事件到表格...")
            success_count = 0
            for i, event in enumerate(events, 1):
                try:
                    self.notion.blocks.children.append(
                        block_id=new_page["id"],
                        children=[
                            {
                                "object": "block",
                                "type": "table_row",
                                "table_row": {
                                    "cells": [
                                        [{"type": "text", "text": {"content": event.get("time", "未指定时间")}}],
                                        [{"type": "text", "text": {"content": event.get("description", "无描述")}}],
                                        [{"type": "text", "text": {"content": event.get("type", "其他")}}],
                                        [{"type": "text", "text": {"content": event.get("market_phase", "其他")}}],
                                        [{"type": "text", "text": {"content": event.get("market_impact", "影响不确定")}}],
                                        [{"type": "text", "text": {"content": event.get("sentiment", "未知")}}],
                                        [{"type": "text", "text": {"content": event.get("source_url", "未知来源")}}]
                                    ]
                                }
                            }
                        ]
                    )
                    success_count += 1
                    if i % 5 == 0:  # 每添加5个事件记录一次进度
                        logger.info(f"已成功添加 {i}/{len(events)} 个事件")
                except Exception as e:
                    logger.error(f"添加第 {i} 个事件时出错: {str(e)}")
            
            logger.info(f"表格添加完成，成功添加 {success_count}/{len(events)} 个事件")
            logger.info(f"每日页面创建完成: {date_str}")
            return new_page
            
        except Exception as e:
            logger.error(f"创建每日页面时出错: {str(e)}")
            return None

    def update_notion_with_events(self, events):
        """更新Notion，创建新的每日页面"""
        try:
            if not events:
                logger.info("No events to update")
                return 0
            
            # 创建每日页面
            new_page = self._create_daily_page(events)
            if not new_page:
                logger.error("Failed to create daily page")
                return 0
            
            logger.info(f"Successfully created daily page with {len(events)} events")
            return len(events)
            
        except Exception as e:
            logger.error(f"Error updating Notion: {str(e)}")
            return 0

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