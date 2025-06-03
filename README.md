# 美股市场事件收集器

这是一个自动化工具，用于收集和整理美股市场的重要事件，并将其同步到Notion数据库。

## 功能特点

- 自动收集每日美股市场重要事件
- 自动收集每周美股市场重要事件
- 支持多种事件类型：
  - 经济数据发布
  - 美联储活动
  - 公司财报
  - IPO事件
  - 分红除息
  - 重大政策变动
  - 其他重要市场事件
- 自动同步到Notion数据库
- 支持定时任务调度

## 安装要求

- Python 3.8+
- 依赖包（见 requirements.txt）
- Notion API Token
- DeepSeek API Token

## 安装步骤

1. 克隆仓库：
```bash
git clone [your-repository-url]
cd finance-news-collect
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
创建 `.env` 文件并添加以下配置：
```
NOTION_TOKEN=your_notion_token
NOTION_DATABASE_ID=your_database_id
DEEPSEEK_API_KEY=your_deepseek_api_key
```

## 使用方法

### 单次运行

收集每日事件：
```bash
python main.py --run-once daily
```

收集每周事件：
```bash
python main.py --run-once weekly
```

### 守护进程模式

启动定时任务：
```bash
python main.py --daemon
```

## 项目结构

- `main.py`: 主程序入口
- `data_collector.py`: 数据收集模块
- `notion_updater.py`: Notion同步模块
- `scheduler.py`: 定时任务调度器
- `config.py`: 配置文件

## 许可证

MIT License 