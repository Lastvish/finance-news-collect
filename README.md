# 美股市场事件收集系统

这是一个自动化的美股市场事件收集和整理系统，可以收集每日重要市场事件和财报信息，并将其整理发布到 Notion 页面中。

## 主要功能

### 1. 每日市场事件收集
- 自动收集美股市场重要事件
- 包括经济数据、政策变动、企业新闻等
- 提供事件分析和市场影响评估
- 支持多种时间格式（盘前、盘中、盘后）
- 自动提取相关个股信息

### 2. 财报信息收集
- 每周自动收集下周的重要财报信息
- 包含详细的财报预期数据
- EPS和营收预期分析
- 上季度业绩回顾
- 重点关注指标提示

### 3. Notion 集成
- 自动创建结构化的 Notion 页面
- 提供市场事件日报
- 生成财报周报
- 支持信息来源链接
- 智能生成市场分析总结

## 使用方法

### 环境配置
1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 配置环境变量：
- 创建 `.env` 文件并设置以下变量：
```
NOTION_API_KEY=your_notion_api_key
NOTION_PARENT_PAGE_ID=your_notion_page_id
DEEPSEEK_API_KEY=your_deepseek_api_key
```

### 运行命令

1. 收集每日事件：
```bash
python run_collection.py --daily
```

2. 收集财报信息：
```bash
python run_collection.py --earnings
```

3. 强制收集财报（非周日）：
```bash
python run_collection.py --earnings --force
```

### 定时任务
使用 scheduler.py 设置自动运行：
```bash
python scheduler.py
```

## 数据格式

### 每日事件页面
- 标题格式：美股市场重点事件日报 YYYY-MM-DD
- 包含市场总结和详细事件表格
- 提供事件时间、描述、类型、影响分析等信息

### 财报信息页面
- 标题格式：美股重点财报时间 YYYY-MM-DD 至 YYYY-MM-DD
- 包含财报概览和详细财报信息
- 提供发布时间、公司信息、预期数据等

## 最近更新

### 2025-06-04
1. 改进时间格式支持
   - 优化盘前、盘中、盘后时间处理
   - 标准化时间显示格式

2. 优化财报信息收集
   - 改进公司信息提取
   - 完善财报数据显示

3. 改进数据展示
   - 修复信息来源链接显示
   - 优化相关个股格式

## 依赖项目
- notion-client
- openai
- python-dotenv
- schedule
- requests

## 注意事项
1. 确保 API 密钥配置正确
2. 建议在美股交易时段运行
3. 注意 API 调用频率限制
4. 定期检查数据准确性

## 许可证
MIT License 