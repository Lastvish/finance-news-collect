# Configuration Template
# Rename this file to config.py and fill in your API keys

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# DeepSeek API Configuration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")  # Get from environment variable
DEEPSEEK_MODEL = "deepseek-chat"  # Using DeepSeek-V3 model

# Notion API Configuration
NOTION_API_KEY = os.getenv("NOTION_API_KEY")  # Get from environment variable
NOTION_PARENT_PAGE_ID = os.getenv("NOTION_PARENT_PAGE_ID")  # Get from environment variable

# Schedule Configuration
# Weekly event collection every Sunday at 8 PM
WEEKLY_SCHEDULE_DAY = "Sunday"
WEEKLY_SCHEDULE_TIME = "20:00"

# Daily event collection for trading days
# Pre-market (before 9:30 AM ET)
PRE_MARKET_TIME = "08:00"  # Local time, adjust as needed

# Post-market (after 4:00 PM ET)
POST_MARKET_TIME = "17:00"  # Local time, adjust as needed

# Search Configuration - Optimized prompts for detailed but focused analysis
WEEKLY_SEARCH_PROMPT = "详细列出下周美股市场重大事件，包括但不限于：重要经济数据发布（如非农、CPI、PPI、GDP、消费者信心指数、褐皮书经济报告等）、美联储决议及讲话、财报发布（特别关注大型科技公司和重要行业龙头）、IPO、分红除息、重大政策变动、地缘政治事件等。按时间顺序排列，并注明具体日期和时间。每条事件必须单独列出，每行只包含一个事件，不要将多个事件合并在一起。"

DAILY_SEARCH_PROMPT = "详细列出今天美股市场重大事件，包括但不限于：重要经济数据发布（如非农、CPI、PPI、GDP、消费者信心指数、褐皮书经济报告等）、美联储官员讲话、财报发布、IPO、分红除息、重大政策变动、突发新闻、公司重大公告等。按时间顺序排列，并注明具体时间。非常重要：每条事件必须单独列出，每行只包含一个事件，不要将多个事件合并在一起。"

# Logging Configuration
LOG_FILE = "finance_events_collector.log"
LOG_LEVEL = "INFO" 