# US Stock Market Events Collector

An automated tool for collecting and organizing important events in the US stock market and synchronizing them to a Notion database.

## Features

- Automated daily US stock market events collection
- Automated weekly US stock market events collection
- Support for multiple event types:
  - Economic data releases (CPI, PPI, NFP, etc.)
  - Federal Reserve activities
  - Company earnings reports
  - IPO events
  - Dividend distributions
  - Major policy changes
  - Geopolitical events
- Automatic synchronization with Notion database
- Scheduled task management
- Real-time breaking news monitoring
- Market sentiment analysis
- Earnings calendar tracking

## Requirements

- Python 3.8+
- Dependencies (see requirements.txt)
- Notion API Token
- DeepSeek API Token

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Lastvish/finance-news-collect.git
cd finance-news-collect
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
Create a `.env` file and add the following configurations:
```
NOTION_TOKEN=your_notion_token
NOTION_DATABASE_ID=your_database_id
DEEPSEEK_API_KEY=your_deepseek_api_key
```

## Usage

### One-time Collection

Collect daily events:
```bash
python main.py --run-once daily
```

Collect weekly events:
```bash
python main.py --run-once weekly
```

### Daemon Mode

Start scheduled tasks:
```bash
python main.py --daemon
```

## Project Structure

- `main.py`: Main program entry point
- `data_collector.py`: Data collection module
  - Collects events from various sources
  - Processes and structures event data
  - Performs sentiment analysis
- `notion_updater.py`: Notion synchronization module
  - Manages Notion database updates
  - Handles duplicate detection
  - Formats data for Notion
- `scheduler.py`: Task scheduler
  - Manages scheduled tasks
  - Handles different collection frequencies
  - Coordinates data flow
- `config.py`: Configuration file
  - API keys and tokens (use .env instead)
  - Schedule settings
  - Search prompts

## Scheduled Tasks

- Weekly events collection: Every Sunday at 20:00
- Daily events collection: 
  - Pre-market: 08:00 ET
  - Post-market: 17:00 ET
- Breaking news monitoring: Every 2 hours
- Earnings events: Daily at 07:00 ET
- Market sentiment analysis: 
  - Pre-market: 08:30 ET
  - Post-market: 16:30 ET

## Data Structure

Events are stored with the following attributes:
- Date
- Time
- Description
- Event Type
- Market Phase
- Market Impact
- Sentiment
- Sector Impact
- Affected Stocks
- Confidence Level
- Source URL

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

MIT License 