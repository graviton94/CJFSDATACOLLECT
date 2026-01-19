# 🛡️ CJFSDATACOLLECT: Global Food Safety Intelligence

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-ff4b4b.svg)](https://streamlit.io/)
[![Status](https://img.shields.io/badge/Status-Active_Development-green)]()
[![Vibe Coding](https://img.shields.io/badge/Built_with-Vibe_Coding-purple)]()

**CJFSDATACOLLECT** is a real-time food safety intelligence system that automates the tracking, aggregation, and visualization of global risk data. It monitors hazardous food recalls and import refusals from the US, EU, and South Korea to support proactive risk management.

---

## 🌍 Data Sources

We aggregate risk data from three primary intelligence sources:

| Source | Region | Type | Method | Frequency |
|:---:|:---:|:---:|:---|:---:|
| **EU RASFF** | 🇪🇺 Europe | Web (SPA) | `Playwright` Dynamic Scraping | Daily |
| **US FDA** | 🇺🇸 USA | Web (Static) | `Requests` + **CDC (Count Change Detection)** | Daily |
| **KR MFDS** | 🇰🇷 Korea | Open API | `REST API` (JSON) | Daily |

## 🚀 Key Features

- **Smart CDC (Change Data Capture):** Minimizes FDA traffic by tracking country-level alert counts and only scraping details when counts increase.
- **Unified Parquet Schema:** Normalizes diverse data fields (e.g., product name, hazard category) into a single, optimized Parquet database.
- **Automated Deduplication:** Uses `SHA256` hashing of source URLs/IDs to prevent duplicate records.
- **Resilient Ingestion:** Includes **Mock Data** generation for testing pipelines without external network dependencies.
- **Vibe Coding Workflow:** Built using Gemini CLI to accelerate logic implementation and maintain high adaptability.
- **Interactive Dashboard:** Streamlit-based UI for filtering and visualizing risk trends by country, product, and hazard type.

---

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.10+
- Node.js (for Gemini CLI, optional)

### 1. Clone the repository
```bash
git clone [https://github.com/YOUR_ORG/CJFSDATACOLLECT.git](https://github.com/YOUR_ORG/CJFSDATACOLLECT.git)
cd CJFSDATACOLLECT
```

### 2. Environment Setup
```bash
# Create Virtual Environment
python -m venv .venv

# Activate (Mac/Linux)
source .venv/bin/activate
# Activate (Windows)
# .venv\Scripts\activate

# Install Dependencies
pip install -r requirements.txt

# Install Browsers for Playwright (Required for RASFF)
playwright install chromium
```

### 3. Configuration
- Create a .env file in the root directory:
```Ini, TOML
KOREA_FOOD_API_KEY=your_api_key_here
# Optional: Set to 'True' to use mock data for testing
USE_MOCK_DATA=False
```

### 🔧 Usage
- 📊 Run Dashboard
  Launch the analytics interface:
```bash
streamlit run app.py
```

- 🤖 Run Data Collectors
  You can run collectors individually or via the main scheduler.

### Option A: Run All (Scheduler)
```bash
# Run once for the last 7 days of data
python src/main_scheduler.py --mode once --days 7

# Run in schedule mode (Daily loop)
python src/main_scheduler.py --mode schedule --time "09:00"
```

```bash
# Test EU RASFF
python src/collectors/rasff_scraper.py

# Test Korea MFDS
python src/collectors/mfds_collector.py
```

### Project Structure
```plaintext
CJFSDATACOLLECT/
├── .env                     # API Keys & Config
├── data/
│   ├── raw/                 # Temporary raw downloads (JSON/HTML)
│   ├── hub/                 # Final DB (hub_data.parquet)
│   └── state/               # State files (e.g., fda_last_counts.json)
├── src/
│   ├── collectors/          # Ingestion Logic
│   │   ├── rasff_scraper.py # Playwright
│   │   ├── fda_scraper.py   # Requests + CDC
│   │   └── mfds_collector.py# REST API
│   ├── processors/          # ETL Logic
│   │   ├── schema.py        # Parquet Schema Definition
│   │   └── normalizer.py    # Data Cleaning
│   └── utils/               # Logger & Helpers
├── app.py                   # Streamlit Entry Point
└── requirements.txt
```

### 📊 Data Schema
All incoming data is normalized to this unified structure (hub_data.parquet):
Field,Type,Description
record_id,string,Unique Key (SHA256 of Source + Ref_No)
source,string,"EU_RASFF, FDA_IMPORT, KR_MFDS"
date_registered,datetime,Standardized Date (YYYY-MM-DD)
product_name,string,Normalized Product Name
hazard_category,string,"e.g., ""Microbiological"", ""Chemical"""
risk_decision,string,"e.g., ""Recall"", ""Reject"", ""Alert"""
origin_country,string,Standardized Country Name
raw_data,json,Original full data (for backup)

### 🤝 Contributing (Vibe Coding)
This project embraces AI-Assisted Vibe Coding.
  1. Context First: Always provide the ARCHITECTURE.md context when prompting Gemini/Copilot.
  2. Review Logic: AI writes the code, humans verify the logic and security.
  3. Mock First: When adding a new source, build a Mock Class first to ensure pipeline stability.

License: MIT | Maintainer: Food Safety Intelligence Team
