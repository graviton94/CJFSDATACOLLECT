# 🍎 CJFSDATACOLLECT - Global Food Safety Intelligence Platform

Real-time food safety intelligence system that aggregates risk data from three primary sources into a unified analytics platform.

## 📋 Overview

This project collects, normalizes, and visualizes food safety data from:
- 🇪🇺 **EU RASFF** (Rapid Alert System for Food and Feed) - via Playwright web scraping
- 🇺🇸 **FDA Import Alerts** (US Food and Drug Administration) - with Country-Count CDC logic
- 🇰🇷 **Korea MFDS** (Ministry of Food and Drug Safety) - via Open API

### Key Features

- **Unified Parquet Schema**: All data normalized to a common format
- **Automated Deduplication**: Unique key-based deduplication prevents duplicate records
- **Daily Scheduled Ingestion**: Automated daily data collection
- **Strict Schema Validation**: Ensures data quality before storage
- **Interactive Dashboard**: Streamlit-based risk analysis visualization
- **Multi-source Integration**: Seamlessly combines data from web scraping, APIs, and structured data

## 🚀 Quick Start

### Prerequisites

- Python 3.10 or higher
- pip package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/graviton94/CJFSDATACOLLECT.git
cd CJFSDATACOLLECT
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install Playwright browsers (required for EU RASFF scraping):
```bash
playwright install chromium
```

4. (Optional) Configure environment variables:
```bash
cp .env.example .env
# Edit .env and add your MFDS API key if available
```

### First Run

1. Collect data from all sources:
```bash
python src/scheduler.py --mode once --days 7
```

2. Launch the dashboard:
```bash
streamlit run app.py
```

3. Open your browser to `http://localhost:8501`

## 📁 Project Structure

```
CJFSDATACOLLECT/
├── app.py                          # Streamlit dashboard application
├── requirements.txt                # Python dependencies
├── .gitignore                      # Git ignore rules
├── .env.example                    # Environment variables template
├── README.md                       # This file
├── config/
│   └── config.yaml                 # Configuration settings
├── src/
│   ├── __init__.py
│   ├── schema.py                   # Unified Parquet schema definition
│   ├── scheduler.py                # Daily ingestion scheduler
│   ├── collectors/
│   │   ├── __init__.py
│   │   ├── rasff_scraper.py       # EU RASFF Playwright scraper
│   │   ├── fda_collector.py       # FDA Import Alerts collector
│   │   └── mfds_collector.py      # Korea MFDS API collector
│   └── utils/
│       ├── __init__.py
│       ├── deduplication.py       # Deduplication utilities
│       └── storage.py             # Parquet storage utilities
├── data/
│   ├── raw/                       # Raw data (not committed)
│   └── processed/                 # Processed Parquet files (not committed)
└── tests/                         # Test files
```

## 🔧 Usage

### Data Collection

#### Run All Collectors Once
```bash
python src/scheduler.py --mode once --days 7
```

#### Schedule Daily Collection
```bash
python src/scheduler.py --mode schedule --time "02:00" --days 1
```

#### Run Individual Collectors
```bash
# EU RASFF
python src/collectors/rasff_scraper.py --days 7

# FDA Import Alerts
python src/collectors/fda_collector.py --days 7

# Korea MFDS
python src/collectors/mfds_collector.py --days 7 --api-key YOUR_KEY
```

### Dashboard

Start the Streamlit dashboard:
```bash
streamlit run app.py
```

Features:
- Real-time metrics and KPIs
- Interactive charts and visualizations
- Filtering by source, risk level, and time range
- Recent alerts table
- Data export (CSV)

## 📊 Data Schema

### Unified Parquet Schema

All data is normalized to the following schema:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| record_id | string | ✅ | Unique identifier (SHA256 hash) |
| source | string | ✅ | Data source (EU_RASFF, FDA_IMPORT_ALERTS, KR_MFDS) |
| source_reference | string | ✅ | Original reference number from source |
| notification_date | datetime | ✅ | Date of notification/alert |
| ingestion_date | datetime | ✅ | Date data was ingested |
| product_name | string | ✅ | Name of the product |
| product_category | string | | Category of product |
| origin_country | string | ✅ | Country of origin |
| destination_country | string | | Destination country |
| hazard_category | string | ✅ | Type of hazard |
| hazard_substance | string | | Specific hazardous substance |
| risk_decision | string | ✅ | Type of decision (alert, recall, etc.) |
| risk_level | string | | Risk level (serious, high, moderate, low) |
| action_taken | string | | Actions taken |
| description | string | | Detailed description |
| data_quality_score | float | ✅ | Quality score (0-1) |

### Deduplication Strategy

Records are deduplicated using a unique key generated from:
- `source`: The data source identifier
- `source_reference`: The original reference number

Key generation:
```python
key = SHA256(f"{source}::{source_reference}")[:16]
```

## 🔄 Data Pipeline

```
┌─────────────────┐
│   EU RASFF      │
│  (Playwright)   │
└────────┬────────┘
         │
         ├─────────────────────┐
         │                     │
┌────────▼────────┐   ┌────────▼─────────┐
│  FDA Import     │   │   Korea MFDS     │
│    Alerts       │   │   (Open API)     │
│ (Country-CDC)   │   │                  │
└────────┬────────┘   └────────┬─────────┘
         │                     │
         └─────────┬───────────┘
                   │
         ┌─────────▼─────────┐
         │   Normalization   │
         │   to Schema       │
         └─────────┬─────────┘
                   │
         ┌─────────▼─────────┐
         │  Deduplication    │
         └─────────┬─────────┘
                   │
         ┌─────────▼─────────┐
         │ Schema Validation │
         └─────────┬─────────┘
                   │
         ┌─────────▼─────────┐
         │ Parquet Storage   │
         └─────────┬─────────┘
                   │
         ┌─────────▼─────────┐
         │    Streamlit      │
         │    Dashboard      │
         └───────────────────┘
```

## 🛠️ Configuration

Edit `config/config.yaml` to customize:
- Data directories
- Collection schedules
- Source-specific settings
- Logging configuration
- Validation rules

## 🔑 Environment Variables

Create a `.env` file (copy from `.env.example`):

```bash
# Korea MFDS API Key (optional)
MFDS_API_KEY=your_api_key_here

# Logging
LOG_LEVEL=INFO

# Data directories
DATA_DIR=data/processed
RAW_DATA_DIR=data/raw
```

## 📈 Features in Detail

### 1. EU RASFF Scraper
- Uses Playwright for dynamic web scraping
- Handles JavaScript-rendered content
- Extracts notification data, product info, and risk details
- Includes mock data for demonstration when portal is unavailable

### 2. FDA Import Alerts Collector
- Implements Country-Count CDC (Change Data Capture) logic
- Tracks alert frequency by country
- Calculates country-level risk scores
- Enriches data with metadata

### 3. Korea MFDS API Collector
- Integrates with Korea's Open API
- Handles various API response formats
- Supports multiple endpoints (recalls, violations)
- Gracefully falls back to mock data without API key

### 4. Data Quality
- Strict schema validation before storage
- Data quality scoring (0-1 scale)
- Error handling and logging
- Duplicate prevention

### 5. Dashboard Analytics
- Time-series analysis
- Geographic distribution
- Hazard category breakdown
- Risk level analysis
- Product category insights
- Exportable reports

## 🧪 Testing

The system includes mock data generation for testing without requiring:
- Live access to EU RASFF portal
- FDA Import Alerts system
- Korea MFDS API credentials

This allows for immediate testing and demonstration of the full pipeline.

## 📝 Development

### Adding New Data Sources

1. Create a new collector in `src/collectors/`
2. Implement the collector class with `collect_and_store()` method
3. Transform data to match unified schema
4. Add to scheduler in `src/scheduler.py`

### Schema Changes

1. Update `src/schema.py` with new fields
2. Update `UNIFIED_SCHEMA` dictionary
3. Update validation logic
4. Test with existing data

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- EU RASFF for food safety data
- FDA for import alert information
- Korea MFDS for API access
- Streamlit for dashboard framework
- Playwright for web scraping capabilities

## 📞 Support

For issues and questions:
- Open an issue on GitHub
- Check existing documentation
- Review configuration settings

---

**Note**: This system uses mock data for demonstration purposes. For production use, configure actual data sources and API credentials.
