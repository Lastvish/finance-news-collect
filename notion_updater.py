import logging
import json
import time
from datetime import datetime, timedelta
from notion_client import Client
from openai import OpenAI
from config import (
    NOTION_API_KEY,
    NOTION_PARENT_PAGE_ID,
    DEEPSEEK_API_KEY
)
import re

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NotionError(Exception):
    """Notion API相关错误"""
    pass

class NotionUpdater:
    def __init__(self):
        self.notion_api_key = NOTION_API_KEY
        self.parent_page_id = NOTION_PARENT_PAGE_ID
        self.notion = Client(auth=self.notion_api_key)
        self.client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com"
        )
        self.max_retries = 3
        self.retry_delay = 2
        
    def _retry_with_exponential_backoff(self, func, *args, **kwargs):
        """使用指数退避的重试机制"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise e
                wait_time = (2 ** attempt) * self.retry_delay
                logger.warning(f"操作失败，{wait_time}秒后重试: {str(e)}")
                time.sleep(wait_time)
    
    def _validate_notion_content(self, content):
        """验证Notion内容的有效性"""
        if not content:
            raise NotionError("内容不能为空")
        
        # 检查内容长度
        if len(content) > 2000:  # Notion的文本长度限制
            logger.warning(f"内容长度({len(content)})超过限制，将被截断")
            return content[:1997] + "..."
        return content
    
    def _format_table_cell(self, content, max_length=2000):
        """格式化表格单元格内容"""
        if not content:
            return [{"type": "text", "text": {"content": ""}}]
        
        # 处理链接
        if isinstance(content, dict) and "url" in content:
            return [{
                "type": "text",
                "text": {
                    "content": content.get("text", ""),
                    "link": {"url": content["url"]}
                }
            }]
        
        # 处理列表
        if isinstance(content, (list, tuple)):
            text = ", ".join(map(str, content))
        else:
            text = str(content)
            
            # 如果文本看起来像是字符串形式的列表，进行清理
            if text.startswith('[') and text.endswith(']'):
                try:
                    # 尝试解析字符串形式的列表
                    items = eval(text)
                    if isinstance(items, (list, tuple)):
                        text = ", ".join(map(str, items))
                except:
                    # 如果解析失败，直接移除方括号
                    text = text.strip('[]').replace("'", "").replace('"', "")
        
        # 截断过长的文本
        if len(text) > max_length:
            text = text[:max_length-3] + "..."
        
        return [{"type": "text", "text": {"content": text}}]
    
    def _format_source_cell(self, event):
        """格式化来源单元格内容"""
        source_name = event.get("source_name", "未知来源")
        source_url = event.get("source_url", "")
        source_type = event.get("source_type", "")
        
        # 如果有URL，创建链接
        if source_url and source_url.startswith(("http://", "https://")):
            return [{"type": "text", "text": {"content": source_name, "link": {"url": source_url}}}]
        
        # 如果没有URL但有来源类型
        if source_type:
            return [{"type": "text", "text": {"content": f"{source_name} ({source_type})"}}]
        
        # 如果只有来源名称
        return [{"type": "text", "text": {"content": source_name}}]
    
    def _generate_daily_summary(self, events):
        """生成每日市场事件的总结分析"""
        try:
            if not events:
                return "今日无重要市场事件。"
            
            # 构建分析提示词
            summary_prompt = f"""作为专业的金融分析师，请对以下今日美股市场事件进行全面分析和总结。
注意：总结内容必须控制在1500字以内。

事件列表：
{json.dumps(events, ensure_ascii=False, indent=2)}

请提供以下分析：
1. 当日市场主要事件概述（300字以内）
2. 重要经济数据分析（300字以内）
3. 企业财报及重大公告分析（300字以内）
4. 市场情绪评估（200字以内）
5. 潜在市场影响分析（200字以内）
6. 需要重点关注的领域和个股（100字以内）
7. 风险提示（100字以内）

请以精炼报告的形式输出，确保分析深入但简明扼要。严格控制每个部分的字数。"""

            # 调用 DeepSeek API 生成分析
            def _generate_summary():
                response = self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": "你是一个专业的金融分析师，擅长总结和分析市场事件。请提供准确、专业、有见地的分析。注意控制输出长度，确保总字数不超过1500字。"},
                        {"role": "user", "content": summary_prompt}
                    ],
                    temperature=0.3,
                    max_tokens=2000
                )
                return response.choices[0].message.content
            
            summary = self._retry_with_exponential_backoff(_generate_summary)
            
            # 验证并格式化总结内容
            summary = self._validate_notion_content(summary)
            
            return summary
            
        except Exception as e:
            logger.error(f"生成每日总结时出错: {str(e)}")
            return "生成每日总结时发生错误。"

    def _create_daily_page(self, events):
        """创建每日市场事件页面，包含总结和详细信息"""
        try:
            # 获取当前日期
            today = datetime.now()
            date_str = today.strftime("%Y-%m-%d")  # 修改回YYYY-MM-DD格式
            
            logger.info(f"开始创建每日页面: {date_str}")
            logger.info(f"事件数量: {len(events)}")
            
            # 生成每日总结
            logger.info("开始生成每日总结...")
            daily_summary = self._generate_daily_summary(events)
            logger.info("每日总结生成完成")
            
            # 准备表格行
            table_rows = [
                {
                    "type": "table_row",
                    "table_row": {
                        "cells": [
                            [{"type": "text", "text": {"content": "时间"}}],
                            [{"type": "text", "text": {"content": "事件描述"}}],
                            [{"type": "text", "text": {"content": "事件类型"}}],
                            [{"type": "text", "text": {"content": "市场阶段"}}],
                            [{"type": "text", "text": {"content": "市场影响"}}],
                            [{"type": "text", "text": {"content": "行业影响"}}],
                            [{"type": "text", "text": {"content": "相关个股"}}],
                            [{"type": "text", "text": {"content": "市场情绪"}}],
                            [{"type": "text", "text": {"content": "信息来源"}}]
                        ]
                    }
                }
            ]
            
            # 添加事件行
            for event in events:
                try:
                    # 处理市场情绪显示
                    sentiment = event.get("sentiment", "neutral")
                    if isinstance(sentiment, list):
                        sentiment_text = " | ".join(sentiment)
                    else:
                        sentiment_text = sentiment
                    
                    # 创建单元格内容
                    cells = [
                        self._format_table_cell(event.get("time", "未指定时间")),
                        self._format_table_cell(event.get("description", "无描述")),
                        self._format_table_cell(event.get("type", "其他")),
                        self._format_table_cell(event.get("market_phase", "其他")),
                        self._format_table_cell(event.get("market_impact", "影响不确定")),
                        self._format_table_cell(event.get("industry_impact", "暂无行业影响分析")),
                        self._format_table_cell(event.get("related_stocks", "无相关个股")),
                        self._format_table_cell(sentiment_text),
                        self._format_source_cell(event)  # 使用专门的方法处理来源
                    ]
                    
                    table_rows.append({
                        "type": "table_row",
                        "table_row": {"cells": cells}
                    })
                except Exception as e:
                    logger.error(f"处理事件行时出错: {str(e)}")
                    continue
            
            # 创建新的页面
            logger.info("开始创建 Notion 页面...")
            def _create_page():
                return self.notion.pages.create(
                    parent={"page_id": self.parent_page_id},
                    properties={
                        "title": {
                            "title": [
                                {
                                    "text": {
                                        "content": f"美股市场重点事件日报 {date_str}"
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
                                "table_width": 9,
                                "has_column_header": True,
                                "has_row_header": False,
                                "children": table_rows
                            }
                        }
                    ]
                )
            
            new_page = self._retry_with_exponential_backoff(_create_page)
            
            logger.info(f"每日页面创建完成: {date_str}")
            logger.info(f"成功创建每日页面，包含 {len(events)} 个事件")
            return new_page
            
        except Exception as e:
            logger.error(f"创建每日页面时出错: {str(e)}")
            raise NotionError(f"创建Notion页面失败: {str(e)}")

    def update_notion_with_events(self, events):
        """更新Notion，创建每日报告和财报报告"""
        try:
            if not events:
                logger.info("没有事件需要更新")
                return 0
            
            # 分离每日事件和财报事件
            daily_events = [e for e in events if not e.get("is_earnings", False)]
            earnings_events = [e for e in events if e.get("is_earnings", False)]
            
            total_count = 0
            
            # 创建每日事件页面
            if daily_events:
                logger.info(f"创建每日事件页面，包含 {len(daily_events)} 个事件")
                daily_page = self._create_daily_page(daily_events)
                if daily_page:
                    total_count += len(daily_events)
            
            # 创建财报事件页面
            if earnings_events:
                logger.info(f"创建财报事件页面，包含 {len(earnings_events)} 个事件")
                earnings_page = self._create_earnings_page(earnings_events)
                if earnings_page:
                    total_count += len(earnings_events)
            
            logger.info(f"成功创建页面，总共包含 {total_count} 个事件")
            return total_count
            
        except NotionError as e:
            logger.error(f"更新 Notion 时出错: {str(e)}")
            return 0
        except Exception as e:
            logger.error(f"未预期的错误: {str(e)}")
            return 0
            
    def _create_earnings_page(self, events):
        """创建财报事件页面"""
        try:
            # 获取日期范围
            dates = sorted(set(event.get("report_date") for event in events if event.get("report_date")))
            if dates:
                start_date = dates[0]  # 保持YYYY-MM-DD格式
                end_date = dates[-1]
                date_range = f"{start_date} 至 {end_date}"
            else:
                today = datetime.now()
                next_monday = today + timedelta(days=(7 - today.weekday()) % 7)
                next_friday = next_monday + timedelta(days=4)
                date_range = f"{next_monday.strftime('%Y-%m-%d')} 至 {next_friday.strftime('%Y-%m-%d')}"
            
            logger.info(f"开始创建财报页面: {date_range}")
            logger.info(f"事件数量: {len(events)}")
            
            # 生成财报总结
            logger.info("开始生成财报总结...")
            earnings_summary = self._generate_earnings_summary(events)
            logger.info("财报总结生成完成")
            
            # 准备表格行
            table_rows = [
                {
                    "type": "table_row",
                    "table_row": {
                        "cells": [
                            [{"type": "text", "text": {"content": "发布日期"}}],
                            [{"type": "text", "text": {"content": "发布时间"}}],
                            [{"type": "text", "text": {"content": "公司名称"}}],
                            [{"type": "text", "text": {"content": "股票代码"}}],
                            [{"type": "text", "text": {"content": "EPS预期"}}],
                            [{"type": "text", "text": {"content": "营收预期"}}],
                            [{"type": "text", "text": {"content": "上季表现"}}],
                            [{"type": "text", "text": {"content": "关注重点"}}],
                            [{"type": "text", "text": {"content": "市场影响"}}]
                        ]
                    }
                }
            ]
            
            # 添加事件行
            for event in sorted(events, key=lambda x: (x.get("report_date", ""), x.get("time", ""))):
                try:
                    # 提取公司信息
                    description = event.get("description", "")
                    company_info = self._extract_company_info(event)
                    
                    # 创建单元格内容
                    cells = [
                        self._format_table_cell(event.get("report_date", "未指定日期")),
                        self._format_table_cell(event.get("earnings_time", "未指定时间")),
                        self._format_table_cell(company_info.get("company_name", "未知公司")),
                        self._format_table_cell(company_info.get("stock_code", "未知代码")),
                        self._format_table_cell(event.get("eps_forecast", "未知")),
                        self._format_table_cell(event.get("revenue_forecast", "未知")),
                        self._format_table_cell(event.get("last_quarter", "未知")),
                        self._format_table_cell(event.get("focus_points", "无")),
                        self._format_table_cell(event.get("market_impact", "影响不确定"))
                    ]
                    
                    table_rows.append({
                        "type": "table_row",
                        "table_row": {"cells": cells}
                    })
                except Exception as e:
                    logger.error(f"处理财报事件行时出错: {str(e)}")
                    continue
            
            # 创建新的页面
            logger.info("开始创建 Notion 页面...")
            def _create_page():
                return self.notion.pages.create(
                    parent={"page_id": self.parent_page_id},
                    properties={
                        "title": {
                            "title": [
                                {
                                    "text": {
                                        "content": f"美股重点财报时间 {date_range}"
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
                                "rich_text": [{"type": "text", "text": {"content": "财报概览"}}]
                            }
                        },
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [{"type": "text", "text": {"content": earnings_summary}}]
                            }
                        },
                        {
                            "object": "block",
                            "type": "heading_1",
                            "heading_1": {
                                "rich_text": [{"type": "text", "text": {"content": "详细财报信息"}}]
                            }
                        },
                        {
                            "object": "block",
                            "type": "table",
                            "table": {
                                "table_width": 9,
                                "has_column_header": True,
                                "has_row_header": False,
                                "children": table_rows
                            }
                        }
                    ]
                )
            
            new_page = self._retry_with_exponential_backoff(_create_page)
            
            logger.info(f"财报页面创建完成: {date_range}")
            logger.info(f"成功创建财报页面，包含 {len(events)} 个事件")
            return new_page
            
        except Exception as e:
            logger.error(f"创建财报页面时出错: {str(e)}")
            raise NotionError(f"创建Notion财报页面失败: {str(e)}")
            
    def _generate_earnings_summary(self, events):
        """生成财报事件的总结分析"""
        try:
            if not events:
                return "本期无重要财报事件。"
            
            # 构建分析提示词
            summary_prompt = f"""作为专业的金融分析师，请对以下财报事件进行全面分析和总结。
注意：总结内容必须控制在1500字以内。

事件列表：
{json.dumps(events, ensure_ascii=False, indent=2)}

请提供以下分析：
1. 本期财报概览（300字以内）
2. 重点关注公司分析（300字以内）
3. 行业分布分析（200字以内）
4. 市场影响评估（200字以内）
5. 投资机会分析（200字以内）
6. 风险提示（100字以内）

请以精炼报告的形式输出，确保分析深入但简明扼要。严格控制每个部分的字数。"""

            # 调用 DeepSeek API 生成分析
            def _generate_summary():
                response = self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": "你是一个专业的金融分析师，擅长分析财报事件。请提供准确、专业、有见地的分析。注意控制输出长度，确保总字数不超过1500字。"},
                        {"role": "user", "content": summary_prompt}
                    ],
                    temperature=0.3,
                    max_tokens=2000
                )
                return response.choices[0].message.content
            
            summary = self._retry_with_exponential_backoff(_generate_summary)
            
            # 验证并格式化总结内容
            summary = self._validate_notion_content(summary)
            
            return summary
            
        except Exception as e:
            logger.error(f"生成财报总结时出错: {str(e)}")
            return "生成财报总结时发生错误。"
            
    def _extract_company_info(self, event):
        """从事件中提取公司信息"""
        company_name = event.get("company_name", "")
        stock_code = event.get("stock_code", "")
        
        # 如果直接有公司名称和股票代码，直接返回
        if company_name and stock_code:
            return {"company_name": company_name, "stock_code": stock_code}
            
        # 如果没有，尝试从描述中提取
        description = event.get("description", "")
        if not description:
            return {"company_name": "未知", "stock_code": "未知"}
            
        # 尝试从描述中提取公司名称和股票代码
        # 通常格式为 "公司名称(股票代码)" 或 "公司名称（股票代码）"
        match = re.search(r'([^()（）]+)[\(（]([A-Z]+)[\)）]', description)
        if match:
            return {
                "company_name": match.group(1).strip(),
                "stock_code": match.group(2).strip()
            }
            
        return {"company_name": "未知", "stock_code": "未知"}

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