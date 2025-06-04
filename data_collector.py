import os
import json
import logging
import re
import time
from openai import OpenAI  # 导入OpenAI SDK
from datetime import datetime, timedelta
from config import DEEPSEEK_API_KEY, DEEPSEEK_MODEL, WEEKLY_SEARCH_PROMPT, DAILY_SEARCH_PROMPT

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIError(Exception):
    """API调用相关错误"""
    pass

class ParseError(Exception):
    """数据解析相关错误"""
    pass

class DataCollector:
    def __init__(self):
        self.deepseek_api_key = DEEPSEEK_API_KEY
        # 初始化OpenAI客户端，配置为使用DeepSeek API
        self.client = OpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )
        self.model = DEEPSEEK_MODEL
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 2  # 重试延迟（秒）
        
    def _retry_with_exponential_backoff(self, func, *args, **kwargs):
        """使用指数退避的重试机制"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:  # 最后一次尝试
                    raise e
                wait_time = (2 ** attempt) * self.retry_delay
                logger.warning(f"操作失败，{wait_time}秒后重试: {str(e)}")
                time.sleep(wait_time)
        
    def _search_with_deepseek(self, prompt):
        """使用DeepSeek搜索市场事件"""
        try:
            logger.info(f"Searching with prompt: {prompt}")
            
            def _do_search():
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "你是一个专业的金融分析师，擅长收集和整理美股市场事件信息。请提供准确、全面的信息，并按时间顺序排列。"}, 
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=2000
                )
                return response.choices[0].message.content
            
            result = self._retry_with_exponential_backoff(_do_search)
            logger.info("Search completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error searching with DeepSeek: {str(e)}")
            raise APIError(f"DeepSeek API调用失败: {str(e)}")
    
    def _analyze_event(self, event):
        """分析单个事件，添加市场影响、情绪等信息"""
        try:
            # 构建分析提示词
            analysis_prompt = f"""作为专业的金融分析师，请分析以下市场事件：

事件描述：{event.get('description', '')}
事件类型：{event.get('type', '其他')}
发生时间：{event.get('time', '未指定时间')}

请以JSON格式输出以下分析结果：
1. market_phase: 事件发生的市场阶段（如盘前、盘中、盘后等）
2. market_impact: 对整体市场的潜在影响
3. industry_impact: 对相关行业的影响分析
4. related_stocks: 可能受影响的主要个股代码（如AAPL、GOOGL等）
5. sentiment: 市场情绪分析，格式如下：
   - 如果结果确定：使用 "bullish"（利好）、"bearish"（利空）或 "neutral"（中性）
   - 如果有多种可能：使用数组格式，如 ["bullish if 数据好于预期", "bearish if 数据差于预期"]

输出格式示例：
{{
  "market_phase": "盘前",
  "market_impact": "如果数据好于预期，可能推动大盘上涨0.5%；如果差于预期，可能引发回调",
  "industry_impact": "科技行业受影响最大，数据好于预期将带动芯片股走强",
  "related_stocks": "NVDA, AMD, INTC, TSM",
  "sentiment": ["bullish if 数据好于预期", "bearish if 数据差于预期"]
}}

请确保输出是有效的JSON格式。每项分析控制在100字以内。"""

            # 调用 DeepSeek API 进行分析
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "你是一个专业的金融分析师，擅长分析市场事件的影响。请提供准确、专业、简明的分析，并始终以有效的JSON格式输出。"},
                    {"role": "user", "content": analysis_prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            
            # 提取JSON部分
            content = response.choices[0].message.content
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                content = json_match.group()
            
            # 解析分析结果
            analysis = json.loads(content)
            
            # 更新事件信息
            event.update(analysis)
            
            return event
            
        except Exception as e:
            logger.error(f"分析事件时出错: {str(e)}")
            # 返回带有默认值的事件
            event.update({
                "market_phase": "其他",
                "market_impact": "影响不确定",
                "industry_impact": "暂无行业影响分析",
                "related_stocks": "无相关个股",
                "sentiment": "neutral"  # 默认使用中性
            })
            return event

    def _get_event_source(self, event):
        """获取事件的信息来源"""
        try:
            description = event.get('description', '')
            if not description:
                return "未知来源"
            
            # 构建搜索提示词
            source_prompt = f"""请查找以下美股市场事件的信息来源：

事件描述：{description}
事件类型：{event.get('type', '其他')}
发生时间：{event.get('time', '未指定时间')}

请提供该事件的官方来源（如公司官网、SEC文件、政府网站）或权威媒体报道（如Bloomberg、Reuters、CNBC等）。
如果有多个来源，请提供最权威的一个。

输出格式示例：
{{
  "source_name": "Bloomberg",
  "source_url": "https://www.bloomberg.com/news/articles/...",
  "source_type": "官方媒体"  // 可选值：官方网站、官方媒体、行业媒体、其他
}}

请确保输出是有效的JSON格式，必须包含source_url字段。"""

            # 调用 DeepSeek API 获取来源
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "你是一个专业的金融信息检索专家，擅长查找市场事件的原始信息来源。请提供准确、权威的来源信息，并始终以有效的JSON格式输出，确保包含source_url字段。"},
                    {"role": "user", "content": source_prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            # 提取JSON部分
            content = response.choices[0].message.content
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                content = json_match.group()
            
            # 解析来源信息
            source_info = json.loads(content)
            
            # 确保source_url存在
            if not source_info.get("source_url"):
                source_info["source_url"] = ""
                logger.warning(f"事件来源缺少URL: {description[:50]}...")
            
            # 更新事件信息
            event["source_name"] = source_info.get("source_name", "未知来源")
            event["source_url"] = source_info.get("source_url", "")
            event["source_type"] = source_info.get("source_type", "其他")
            
            return event["source_name"]
            
        except Exception as e:
            logger.error(f"获取事件来源时出错: {str(e)}")
            # 确保即使出错也设置基本的来源信息
            event["source_name"] = "未知来源"
            event["source_url"] = ""
            event["source_type"] = "其他"
            return "未知来源"

    def _clean_event_data(self, event):
        """清理和标准化事件数据"""
        # 清理时间格式
        time = event.get("time", "").strip()
        time_mapping = {
            "盘前": "09:00",
            "盘中": "13:30",
            "盘后": "16:00",
            "美股盘前": "09:00",
            "美股盘中": "13:30",
            "美股盘后": "16:00",
            "开盘": "09:30",
            "收盘": "16:00"
        }
        event["time"] = time_mapping.get(time, time)
        
        # 清理描述文本
        description = event.get("description", "").strip()
        description = re.sub(r'\s+', ' ', description)  # 删除多余空白
        event["description"] = description
        
        # 标准化事件类型
        event_type = event.get("type", "").strip().lower()
        type_mapping = {
            "earning": "财报事件",
            "earnings": "财报事件",
            "financial": "财报事件",
            "economic": "经济数据",
            "economy": "经济数据",
            "policy": "政策变动",
            "breaking": "突发新闻",
            "news": "突发新闻"
        }
        event["type"] = type_mapping.get(event_type, event.get("type", "其他"))
        
        # 清理相关个股格式
        stocks = event.get("related_stocks", "")
        if isinstance(stocks, str):
            # 如果是字符串形式的列表，尝试解析
            if stocks.startswith('[') and stocks.endswith(']'):
                try:
                    # 尝试解析JSON
                    stocks_list = json.loads(stocks.replace("'", '"'))
                    if isinstance(stocks_list, list):
                        stocks = ", ".join(stocks_list)
                except:
                    # 如果JSON解析失败，使用正则表达式清理
                    stocks = stocks.strip('[]').replace("'", "").replace('"', "")
            # 标准化分隔符
            stocks = re.sub(r'[,，;；]+', ', ', stocks)
            event["related_stocks"] = stocks.strip()
        elif isinstance(stocks, (list, tuple)):
            # 如果已经是列表形式，直接转换
            event["related_stocks"] = ", ".join(map(str, stocks))
        
        # 清理和标准化情绪标签
        sentiment = event.get("sentiment", "neutral")
        if isinstance(sentiment, str):
            sentiment = sentiment.lower().strip()
            if "利好" in sentiment or "看涨" in sentiment:
                sentiment = "bullish"
            elif "利空" in sentiment or "看跌" in sentiment:
                sentiment = "bearish"
            elif "中性" in sentiment:
                sentiment = "neutral"
        event["sentiment"] = sentiment
        
        return event
        
    def _validate_event(self, event):
        """验证事件数据的完整性和有效性"""
        required_fields = ["time", "description", "type"]
        for field in required_fields:
            if not event.get(field):
                raise ParseError(f"事件缺少必要字段: {field}")
        
        # 验证时间格式
        time = event.get("time", "").strip()
        time_patterns = [
            r'^\d{1,2}:\d{2}$',  # HH:MM
            r'^盘[前中后]$',      # 盘前/盘中/盘后
            r'^美股盘[前中后]$',  # 美股盘前/盘中/盘后
            r'^开盘$',           # 开盘
            r'^收盘$'            # 收盘
        ]
        
        is_valid_time = any(re.match(pattern, time) for pattern in time_patterns)
        if not is_valid_time:
            raise ParseError(f"无效的时间格式: {time}")
        
        # 验证描述长度
        if len(event["description"]) < 10:
            raise ParseError("事件描述过短")
        
        return True
        
    def _parse_events(self, text):
        """解析事件文本，提取事件列表"""
        try:
            # 构建解析提示词
            parse_prompt = f"""请将以下文本解析为结构化的事件列表。

{text}

请以JSON格式输出，每个事件必须包含以下字段：
1. time: 事件发生时间（HH:MM格式）
2. description: 事件描述（至少10个字符）
3. type: 事件类型（如：经济数据、企业财报、政策变动、市场新闻等）

输出格式示例：
[
  {{
    "time": "09:30",
    "description": "示例事件1",
    "type": "经济数据"
  }}
]

请确保输出是有效的JSON格式。"""

            # 解析事件
            events = []
            try:
                # 调用 DeepSeek API 进行解析
                response = self._retry_with_exponential_backoff(
                    lambda: self.client.chat.completions.create(
                        model="deepseek-chat",
                        messages=[
                            {"role": "system", "content": "你是一个专业的文本解析器，擅长将非结构化文本转换为结构化数据。请始终以有效的JSON格式输出。"},
                            {"role": "user", "content": parse_prompt}
                        ],
                        temperature=0.3,
                        max_tokens=2000
                    )
                )
                
                # 提取JSON部分
                content = response.choices[0].message.content
                json_match = re.search(r'\[[\s\S]*\]', content)
                if json_match:
                    content = json_match.group()
                
                # 解析事件列表
                parsed_events = json.loads(content)
                
                # 验证和清理每个事件
                for event in parsed_events:
                    try:
                        if self._validate_event(event):
                            cleaned_event = self._clean_event_data(event)
                            events.append(cleaned_event)
                    except ParseError as e:
                        logger.warning(f"跳过无效事件: {str(e)}")
                        continue
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析错误: {str(e)}")
                raise ParseError(f"无效的JSON格式: {str(e)}")
            
            # 分析和增强事件
            logger.info("开始分析事件...")
            enhanced_events = []
            for event in events:
                try:
                    # 获取事件来源
                    logger.info(f"获取事件来源: {event.get('description', '')[:50]}...")
                    source = self._get_event_source(event)
                    
                    # 分析事件
                    logger.info(f"分析事件: {event.get('description', '')[:50]}...")
                    enhanced_event = self._analyze_event(event)
                    enhanced_events.append(enhanced_event)
                    logger.info(f"完成事件分析: {event.get('description', '')[:50]}...")
                except Exception as e:
                    logger.error(f"处理事件时出错: {str(e)}")
                    # 如果处理失败，添加基本事件信息
                    events.append(event)
            
            logger.info(f"成功解析并增强了 {len(enhanced_events)} 个事件")
            return enhanced_events
            
        except Exception as e:
            logger.error(f"解析事件时出错: {str(e)}")
            return []
    
    def collect_weekly_events(self):
        """收集下周的美股市场重大事件"""
        logger.info("Collecting weekly events")
        
        # 获取下周的日期范围
        today = datetime.now()
        next_monday = today + timedelta(days=(7 - today.weekday()))
        next_sunday = next_monday + timedelta(days=6)
        date_range = f"{next_monday.strftime('%Y-%m-%d')} 至 {next_sunday.strftime('%Y-%m-%d')}"
        
        # 构建搜索提示词
        prompt = f"{WEEKLY_SEARCH_PROMPT}\n日期范围: {date_range}"
        
        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect weekly events")
            return []
        
        # 解析事件
        events = self._parse_events(result_text)
        logger.info(f"Collected {len(events)} weekly events")
        
        return events
    
    def collect_daily_events(self):
        """收集当天的美股市场重大事件"""
        logger.info("Collecting daily events")
        
        # 获取当天日期
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 构建搜索提示词
        prompt = f"{DAILY_SEARCH_PROMPT}\n日期: {today}"
        
        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect daily events")
            return []
        
        # 解析事件
        events = self._parse_events(result_text)
        logger.info(f"Collected {len(events)} daily events")
        
        return events

    def _batch_enhance_events(self, events, batch_size=5):
        """批量增强事件分析，减少API调用次数"""
        if not events:
            return []
            
        # 首先为每个事件添加信息来源
        for event in events:
            description = event.get("description", "")
            if not description:
                continue
                
            try:
                # 添加信息来源查询
                source_prompt = f"请查找以下美股市场事件的信息来源：\n\n事件：{description}\n\n请提供该事件的官方来源网址或新闻报道链接。如果有多个来源，请提供最权威的一个。"
                
                source_response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "你是一个专业的金融信息检索专家，擅长查找市场事件的原始信息来源。请提供准确、权威的来源链接。"}, 
                        {"role": "user", "content": source_prompt}
                    ],
                    temperature=0.3,
                    max_tokens=500
                )
                
                source_text = source_response.choices[0].message.content
                
                # 提取URL
                import re
                url_match = re.search(r'https?://[^\s()<>"\\\[\]]+', source_text)
                if url_match:
                    event["source_url"] = url_match.group(0)
                else:
                    # 如果没有找到URL，使用整个回答作为来源信息
                    event["source_url"] = source_text.strip()
            except Exception as e:
                logger.error(f"Error getting source for event: {str(e)}")
                event["source_url"] = "获取来源失败"
        
        enhanced_events = []
        # 按批次处理事件
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            
            # 构建批量分析提示词
            batch_prompt = "请对以下多个美股市场事件进行批量分析，为每个事件提供市场影响、行业影响、相关个股、确信度评估和市场情绪判断。\n\n"
            for idx, event in enumerate(batch):
                batch_prompt += f"事件{idx+1}: {event.get('description', '')}\n"
            
            batch_prompt += "\n请按照以下格式回答，为每个事件提供分析：\n"
            for idx in range(len(batch)):
                batch_prompt += f"事件{idx+1}分析:\n1. 市场影响: [分析]\n2. 行业影响: [分析]\n3. 相关个股: [分析]\n4. 确信度: [high/medium/low]\n5. 市场情绪: [bullish/bearish/neutral]\n\n"
            
            try:
                # 调用DeepSeek进行批量分析
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "你是一个专业的金融分析师，擅长分析事件对美股市场的影响。请提供简洁、准确的分析，并严格按照指定格式回答。"}, 
                        {"role": "user", "content": batch_prompt}
                    ],
                    temperature=0.3,
                    max_tokens=2000
                )
                
                analysis_text = response.choices[0].message.content
                
                # 解析批量分析结果
                import re
                for idx, event in enumerate(batch):
                    event_pattern = rf"事件{idx+1}分析:(.*?)(?=事件{idx+2}分析:|$)"
                    event_analysis = re.search(event_pattern, analysis_text, re.DOTALL)
                    
                    if event_analysis:
                        analysis = event_analysis.group(1).strip()
                        
                        # 提取各部分分析
                        market_impact = re.search(r'1\.\s*市场影响:?(.*?)(?=\n2\.\s*|$)', analysis, re.DOTALL)
                        sector_impact = re.search(r'2\.\s*行业影响:?(.*?)(?=\n3\.\s*|$)', analysis, re.DOTALL)
                        stocks_affected = re.search(r'3\.\s*相关个股:?(.*?)(?=\n4\.\s*|$)', analysis, re.DOTALL)
                        confidence = re.search(r'4\.\s*确信度:?(.*?)(?=\n5\.\s*|$)', analysis, re.DOTALL)
                        sentiment = re.search(r'5\.\s*市场情绪:?(.*?)(?=\n|$)', analysis, re.DOTALL)
                        
                        # 更新事件信息
                        if market_impact:
                            event["market_impact"] = market_impact.group(1).strip()
                        if sector_impact:
                            event["sector_impact"] = sector_impact.group(1).strip()
                        if stocks_affected:
                            event["stocks_affected"] = stocks_affected.group(1).strip()
                        if confidence:
                            conf_text = confidence.group(1).strip().lower()
                            if "high" in conf_text or "高" in conf_text:
                                event["confidence_level"] = "high"
                            elif "medium" in conf_text or "中" in conf_text:
                                event["confidence_level"] = "medium"
                            else:
                                event["confidence_level"] = "low"
                        else:
                            event["confidence_level"] = "medium"
                            
                        # 添加市场情绪判断
                        if sentiment:
                            sentiment_text = sentiment.group(1).strip().lower()
                            if "bullish" in sentiment_text or "利好" in sentiment_text or "正面" in sentiment_text:
                                event["sentiment"] = "bullish"
                            elif "bearish" in sentiment_text or "利空" in sentiment_text or "负面" in sentiment_text:
                                event["sentiment"] = "bearish"
                            elif "neutral" in sentiment_text or "中性" in sentiment_text:
                                event["sentiment"] = "neutral"
                            else:
                                event["sentiment"] = "unknown"
                        else:
                            event["sentiment"] = "unknown"
                    
                    enhanced_events.append(event)
                    
            except Exception as e:
                logger.error(f"Error in batch enhancing events: {str(e)}")
                # 如果批量处理失败，添加原始事件
                enhanced_events.extend(batch)
                
        return enhanced_events
    
    def _enhance_event_analysis(self, event):
        """增强事件分析，添加更多维度的分析信息"""
        try:
            description = event.get("description", "")
            if not description:
                return event
                
            # 添加信息来源查询
            source_prompt = f"请查找以下美股市场事件的信息来源：\n\n事件：{description}\n\n请提供该事件的官方来源网址或新闻报道链接。如果有多个来源，请提供最权威的一个。"
            
            source_response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个专业的金融信息检索专家，擅长查找市场事件的原始信息来源。请提供准确、权威的来源链接。"}, 
                    {"role": "user", "content": source_prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            source_text = source_response.choices[0].message.content
            
            # 提取URL
            import re
            url_match = re.search(r'https?://[^\s()<>"\\\\[\\]]+', source_text)
            if url_match:
                event["source_url"] = url_match.group(0)
            else:
                # 如果没有找到URL，使用整个回答作为来源信息
                event["source_url"] = source_text.strip()
            
            # 继续原有的分析流程...
            # 调用DeepSeek进行深度分析
            analysis_prompt = f"作为专业金融分析师，请对以下美股市场事件进行深度分析：\n\n事件：{description}\n\n请提供：\n1. 对整体美股市场的影响分析\n2. 对相关行业板块的影响分析\n3. 对主要相关个股的影响分析（请以逗号分隔列出股票代码或名称）\n4. 分析的确信度（high/medium/low）\n5. 市场情绪判断（请明确指出该事件对市场情绪的影响是利好/bullish、利空/bearish、中性/neutral）"
            
            analysis_response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个专业的金融分析师，擅长分析事件对美股市场的影响。请提供详细、准确、有深度的分析，并确保按照指定格式回答。"}, 
                    {"role": "user", "content": analysis_prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            
            analysis_text = analysis_response.choices[0].message.content
            
            # 提取各部分分析，使用更精确的正则表达式
            import re
            market_impact = re.search(r'1\.\s*(?:整体美股市场.*?影响|.*?市场影响)[：:](.*?)(?=\n\s*2\.\s*|$)', analysis_text, re.DOTALL)
            sector_impact = re.search(r'2\.\s*(?:行业板块.*?影响|.*?行业影响)[：:](.*?)(?=\n\s*3\.\s*|$)', analysis_text, re.DOTALL)
            stocks_affected = re.search(r'3\.\s*(?:相关个股.*?影响|.*?个股影响)[：:](.*?)(?=\n\s*4\.\s*|$)', analysis_text, re.DOTALL)
            confidence = re.search(r'4\.\s*(?:确信度|.*?确信度)[：:](.*?)(?=\n\s*5\.\s*|$)', analysis_text, re.DOTALL)
            sentiment = re.search(r'5\.\s*(?:市场情绪|.*?情绪)[：:](.*?)(?=\n|$)', analysis_text, re.DOTALL)
            
            # 更新事件信息
            if market_impact:
                event["market_impact"] = market_impact.group(1).strip()
            if sector_impact:
                event["sector_impact"] = sector_impact.group(1).strip()
            if stocks_affected:
                # 确保stocks_affected是字符串格式
                stocks_text = stocks_affected.group(1).strip()
                # 清理格式，确保是简单的文本列表
                stocks_text = re.sub(r'\*\*|\{|\}|"', '', stocks_text)
                event["stocks_affected"] = stocks_text
            if confidence:
                conf_text = confidence.group(1).strip().lower()
                if "high" in conf_text or "高" in conf_text:
                    event["confidence_level"] = "high"
                elif "medium" in conf_text or "中" in conf_text:
                    event["confidence_level"] = "medium"
                else:
                    event["confidence_level"] = "low"
            else:
                event["confidence_level"] = "medium"
                
            # 添加市场情绪判断
            if sentiment:
                sentiment_text = sentiment.group(1).strip().lower()
                if "bullish" in sentiment_text or "利好" in sentiment_text or "正面" in sentiment_text:
                    event["sentiment"] = "bullish"
                elif "bearish" in sentiment_text or "利空" in sentiment_text or "负面" in sentiment_text:
                    event["sentiment"] = "bearish"
                elif "neutral" in sentiment_text or "中性" in sentiment_text:
                    event["sentiment"] = "neutral"
                else:
                    event["sentiment"] = "unknown"
            else:
                # 如果没有明确的情绪判断，尝试从市场影响分析中推断
                if market_impact:
                    impact_text = market_impact.group(1).strip().lower()
                    if "利好" in impact_text or "正面" in impact_text or "积极" in impact_text or "上涨" in impact_text:
                        event["sentiment"] = "bullish"
                    elif "利空" in impact_text or "负面" in impact_text or "消极" in impact_text or "下跌" in impact_text:
                        event["sentiment"] = "bearish"
                    elif "中性" in impact_text or "有限" in impact_text or "轻微" in impact_text:
                        event["sentiment"] = "neutral"
                    else:
                        event["sentiment"] = "unknown"
                else:
                    event["sentiment"] = "unknown"
                
            return event
        except Exception as e:
            logger.error(f"Error enhancing event analysis: {str(e)}")
            return event
    
    def collect_breaking_news(self):
        """收集突发重要新闻"""
        logger.info("Collecting breaking news")
        
        # 获取当前时间
        now = datetime.now()
        one_hour_ago = (now - timedelta(hours=1)).strftime("%H:%M")
        current_time = now.strftime("%H:%M")
        
        # 构建搜索提示词
        prompt = f"""列出过去一小时（{one_hour_ago} 至 {current_time}）内美股市场的重要突发新闻。

重点关注：
1. 重大公司公告和重要人物讲话
2. 突发事件和黑天鹅事件
3. 市场异常波动
4. 监管政策变动
5. 重要经济数据发布

每条新闻必须包含：
1. 具体发生时间
2. 详细事件描述
3. 信息来源
4. 对市场的潜在影响分析

特别说明：仅收集过去一小时内的新闻，确保时效性。"""
        
        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect breaking news")
            return []
        
        # 解析事件
        events = self._parse_events(result_text)
        logger.info(f"Collected {len(events)} breaking news events")
        
        # 过滤掉一小时前的事件
        filtered_events = []
        for event in events:
            try:
                event_time = datetime.strptime(event.get("time", "00:00"), "%H:%M")
                event_time = event_time.replace(year=now.year, month=now.month, day=now.day)
                if now - timedelta(hours=1) <= event_time <= now:
                    filtered_events.append(event)
            except ValueError:
                logger.warning(f"无法解析事件时间: {event.get('time')}")
                continue
        
        logger.info(f"过滤后保留 {len(filtered_events)} 个最近一小时的事件")
        return filtered_events
        
    def collect_earnings_events(self, force=False):
        """收集下周的财报事件
        
        Args:
            force (bool): 是否强制收集，即使不是周日也收集
        """
        logger.info("Collecting earnings events")
        
        # 检查今天是否是周日，除非强制收集
        today = datetime.now()
        if not force and today.weekday() != 6:  # 0是周一，6是周日
            logger.info("今天不是周日，跳过财报收集")
            return []
        
        # 获取下周的日期范围
        next_monday = today + timedelta(days=(7 - today.weekday()) % 7)  # 获取下周一
        next_friday = next_monday + timedelta(days=4)  # 下周五
        date_range = f"{next_monday.strftime('%Y-%m-%d')} 至 {next_friday.strftime('%Y-%m-%d')}"
        
        # 构建搜索提示词
        prompt = f"""详细列出下周（{date_range}）将发布财报的重要公司。

重点关注：
1. 标普500成分股
2. 大型科技公司
3. 市值超过100亿美元的公司
4. 对市场有重要影响力的公司

对于每家公司，提供以下信息：
1. 公司名称和股票代码
2. 具体财报发布日期和时间（盘前/盘后）
3. 市场预期
   - EPS预期
   - 营收预期
   - 同比增长预期
4. 上一季度业绩回顾
5. 重点关注指标
6. 分析师观点汇总
7. 可能对市场和行业的影响

输出格式示例：
[
  {{
    "report_date": "2025-06-09",
    "time": "盘前",
    "company_name": "Oracle",
    "stock_code": "ORCL",
    "description": "Oracle将于盘前发布2025财年第四季度财报",
    "eps_forecast": "1.32美元",
    "revenue_forecast": "138.2亿美元",
    "last_quarter": "上季度EPS为1.28美元，营收135.1亿美元",
    "focus_points": "云服务收入增长、利润率表现、AI相关业务进展",
    "market_impact": "作为大型企业软件供应商，其业绩可能影响整个科技板块走势",
    "type": "财报事件"
  }}
]

请确保输出是有效的JSON格式，每个事件必须包含上述所有字段。"""

        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect earnings events")
            return []
        
        # 解析事件
        try:
            # 提取JSON部分
            json_match = re.search(r'\[[\s\S]*\]', result_text)
            if json_match:
                content = json_match.group()
                events = json.loads(content)
            else:
                logger.error("无法从响应中提取JSON数据")
                return []
            
            # 为每个财报事件添加额外标记
            for event in events:
                event["type"] = "财报事件"  # 确保事件类型正确
                event["is_earnings"] = True  # 添加财报事件标记
                
                # 标准化时间格式
                if "盘前" in event.get("time", ""):
                    event["earnings_time"] = "盘前"
                    event["time"] = "09:00"
                elif "盘后" in event.get("time", ""):
                    event["earnings_time"] = "盘后"
                    event["time"] = "16:00"
                else:
                    event["earnings_time"] = "未指定"
                    event["time"] = "12:00"  # 默认中午
                
                # 确保所有必要字段都存在
                required_fields = [
                    "report_date", "company_name", "stock_code", "description",
                    "eps_forecast", "revenue_forecast", "last_quarter",
                    "focus_points", "market_impact"
                ]
                for field in required_fields:
                    if not event.get(field):
                        event[field] = "未知"
            
            logger.info(f"成功收集到 {len(events)} 个财报事件")
            return events
            
        except Exception as e:
            logger.error(f"解析财报事件时出错: {str(e)}")
            return []
        
    def collect_market_sentiment(self):
        """收集市场情绪和关注焦点"""
        logger.info("Collecting market sentiment")
        
        # 构建搜索提示词
        prompt = "分析当前美股市场的整体情绪和投资者关注焦点，包括：\n1. 市场主流情绪（贪婪/恐惧/中性）\n2. 当前市场最关注的热点话题和板块\n3. 机构投资者的主要观点和立场\n4. 技术面和基本面的关键指标状态\n5. 可能影响市场的潜在风险因素\n请提供详细分析，并说明依据。"
        
        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect market sentiment")
            return []
        
        # 创建市场情绪事件
        today = datetime.now().strftime("%Y-%m-%d")
        event = {
            "date": today,
            "time": datetime.now().strftime("%H:%M"),
            "description": "市场情绪和关注焦点分析",
            "type": "市场分析",
            "market_phase": "其他",
            "market_impact": result_text,
            "sentiment": "neutral",  # 默认中性
            "confidence_level": "medium"
        }
        
        logger.info("Collected market sentiment analysis")
        return [event]
# 测试代码
if __name__ == "__main__":
    collector = DataCollector()
    weekly_events = collector.collect_weekly_events()
    print(f"Collected {len(weekly_events)} weekly events")
    print(json.dumps(weekly_events[:2], indent=2, ensure_ascii=False))
    
    daily_events = collector.collect_daily_events()
    print(f"Collected {len(daily_events)} daily events")
    print(json.dumps(daily_events[:2], indent=2, ensure_ascii=False))