import os
import json
import logging
from openai import OpenAI  # 导入OpenAI SDK
from datetime import datetime, timedelta
from config import DEEPSEEK_API_KEY, DEEPSEEK_MODEL, WEEKLY_SEARCH_PROMPT, DAILY_SEARCH_PROMPT

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self):
        self.deepseek_api_key = DEEPSEEK_API_KEY
        # 初始化OpenAI客户端，配置为使用DeepSeek API
        self.client = OpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )
        self.model = DEEPSEEK_MODEL
        
    def _search_with_deepseek(self, prompt):
        """使用DeepSeek搜索市场事件"""
        try:
            logger.info(f"Searching with prompt: {prompt}")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个专业的金融分析师，擅长收集和整理美股市场事件信息。请提供准确、全面的信息，并按时间顺序排列。"}, 
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # 降低温度以获得更确定性的回答
                max_tokens=2000
            )
            
            result = response.choices[0].message.content
            logger.info("Search completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error searching with DeepSeek: {str(e)}")
            return None
    
    def _parse_events(self, text):
        """解析DeepSeek返回的事件文本，转换为结构化数据"""
        try:
            logger.info("Parsing events from DeepSeek response")
            
            # 优化提示词，简化解析过程
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个专业的金融分析师和数据解析专家。你的任务是将美股市场事件文本解析为JSON格式的事件列表。每个事件必须作为单独的对象，每个对象必须包含以下字段：\n1. date: 事件日期\n2. time: 事件时间\n3. description: 事件描述\n4. type: 事件类型（经济数据、财报、美联储、政策、公司公告、IPO、地缘政治等）\n5. market_phase: 市场阶段（'盘前'、'盘中'、'盘后'或'其他'）"}, 
                    {"role": "user", "content": f"请将以下文本解析为JSON格式的事件列表，确保每个事件都是单独的一个对象，包含所有必要字段:\n\n{text}"}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            parsed_text = response.choices[0].message.content
            
            # 提取JSON部分
            import re
            json_match = re.search(r'```json\n(.+?)\n```', parsed_text, re.DOTALL)
            if json_match:
                parsed_text = json_match.group(1)
            else:
                # 尝试直接解析，移除可能的markdown格式
                parsed_text = re.sub(r'```json|```', '', parsed_text).strip()
            
            # 确保解析后的数据是列表格式
            events = json.loads(parsed_text)
            if not isinstance(events, list):
                events = [events]  # 如果不是列表，转换为列表
                
            # 确保事件包含所有必要字段
            validated_events = []
            for event in events:
                if isinstance(event, dict):
                    # 确保事件包含所有必要字段
                    if not event.get("description"):
                        event["description"] = "无描述"
                    if not event.get("time"):
                        event["time"] = "未指定时间"
                    if not event.get("market_phase"):
                        # 根据时间判断市场阶段
                        time_str = event.get("time", "")
                        try:
                            # 尝试解析时间
                            if ":" in time_str:
                                hour = int(time_str.split(":")[0])
                                # 美东时间判断，需要根据实际情况调整
                                if 4 <= hour < 9 or (hour == 9 and ":30" not in time_str):
                                    event["market_phase"] = "盘前"
                                elif 9 <= hour < 16 or (hour == 9 and ":30" in time_str):
                                    event["market_phase"] = "盘中"
                                elif 16 <= hour < 20:
                                    event["market_phase"] = "盘后"
                                else:
                                    event["market_phase"] = "其他"
                            else:
                                event["market_phase"] = "其他"
                        except:
                            event["market_phase"] = "其他"
                    
                    # 设置默认值
                    if not event.get("market_impact"):
                        event["market_impact"] = "影响不确定"
                    if not event.get("sentiment"):
                        event["sentiment"] = "unknown"
                        
                    validated_events.append(event)
                else:
                    # 如果事件不是字典，创建一个包含该事件描述的字典
                    validated_events.append({
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "description": str(event) if event else "无描述",
                        "time": "未指定时间",
                        "type": "其他",
                        "market_phase": "其他",
                        "market_impact": "影响不确定",
                        "sentiment": "unknown"
                    })
            
            # 批量增强事件分析，而不是逐个处理
            enhanced_events = self._batch_enhance_events(validated_events)
            
            logger.info(f"Successfully parsed and enhanced {len(enhanced_events)} events")
            return enhanced_events
            
        except Exception as e:
            logger.error(f"Error parsing events: {str(e)}")
            # 如果解析失败，尝试拆分原始文本为多个事件
            try:
                lines = text.split('\n')
                events = []
                for line in lines:
                    if line.strip():
                        events.append({
                            "date": datetime.now().strftime("%Y-%m-%d"), 
                            "description": line.strip(),
                            "time": "未指定时间",
                            "type": "其他",
                            "market_phase": "其他",
                            "market_impact": "影响不确定",
                            "sentiment": "unknown"
                        })
                if events:
                    logger.info(f"Fallback parsing created {len(events)} events")
                    return events
            except:
                pass
                
            # 如果拆分也失败，返回原始文本作为单个事件
            return [{
                "date": datetime.now().strftime("%Y-%m-%d"), 
                "description": text,
                "time": "未指定时间",
                "type": "其他",
                "market_phase": "其他",
                "market_impact": "影响不确定",
                "sentiment": "unknown"
            }]
    
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
        
        # 构建搜索提示词
        prompt = "列出过去6小时内美股市场的重要突发新闻，包括但不限于：重大公司公告、突发事件、重要人物讲话、市场异常波动等。每条新闻必须单独列出，并分析该新闻对美股市场的潜在影响。特别关注可能对市场产生重大影响的黑天鹅事件。"
        
        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect breaking news")
            return []
        
        # 解析事件
        events = self._parse_events(result_text)
        logger.info(f"Collected {len(events)} breaking news events")
        
        return events
        
    def collect_earnings_events(self):
        """收集近期财报事件"""
        logger.info("Collecting earnings events")
        
        # 获取当前日期
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 构建搜索提示词
        prompt = f"详细列出今天和未来一周将发布财报的重要公司，特别关注标普500成分股和大型科技公司。对于每家公司，提供以下信息：\n1. 公司名称和股票代码\n2. 财报发布的具体日期和时间（盘前/盘后）\n3. 市场对该公司财报的预期（EPS和营收预期）\n4. 该公司上一季度的表现\n5. 分析师对该公司的关注点\n6. 该财报可能对整体市场和相关行业的影响"
        
        # 搜索事件
        result_text = self._search_with_deepseek(prompt)
        if not result_text:
            logger.error("Failed to collect earnings events")
            return []
        
        # 解析事件
        events = self._parse_events(result_text)
        logger.info(f"Collected {len(events)} earnings events")
        
        return events
        
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