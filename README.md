# 🛡️ CJFSDATACOLLECT: Global Food Safety Intelligence

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-ff4b4b.svg)](https://streamlit.io/)
[![Status](https://img.shields.io/badge/Status-Development-green)]()

**CJFSDATACOLLECT** is an automated pipeline designed to track, aggregate, and visualize global food safety alerts in real-time. It monitors hazardous food recalls and import refusals from the US, EU, and South Korea to support proactive risk management.

## 🌍 Data Sources
| Source | Type | Method | Frequency |
|:---:|:---:|:---|:---:|
| **EU RASFF** | Web (SPA) | `Playwright` Dynamic Scraping | Daily |
| **US FDA** | Web (Static) | `Requests` + CDC (Count Change Detection) | Daily |
| **KR MFDS** | Open API | `REST API` (JSON) | Daily |

## 🚀 Key Features
- **Smart CDC (Change Data Capture):** Minimizes traffic by only scraping FDA details when country-level counts change.
- **Unified Schema:** Normalizes diverse data fields (e.g., product name, hazard category) into a single standard format.
- **AI-Powered:** Utilizes Gemini CLI for rapid development ("Vibe Coding") and potential NLP data cleaning.
- **Interactive Dashboard:** Streamlit-based UI for filtering and visualizing risk trends.

## 🛠️ Installation & Setup

1. **Clone the repo**
   ```bash
   git clone [https://github.com/YOUR_ORG/CJFSDATACOLLECT.git](https://github.com/YOUR_ORG/CJFSDATACOLLECT.git)
   cd CJFSDATACOLLECT

2. **Environment Setup**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   playwright install

3. **Configuration Create a .env file:**
   ```Ini, TOML
   KOREA_FOOD_API_KEY=your_api_key_here

4. **Run**
   ```Bash
   # Run Dashboard
   streamlit run app.py

   # Run Collector Manually
   python src/main_scheduler.py

## 📂 Project Structure
   ```Plaintext
CJFSDATACOLLECT/
├── data/               # Parquet Storage & State files
├── src/
│   ├── collectors/     # Scrapers & API Clients
│   ├── processors/     # Normalization & Dedup Logic
│   └── utils/          # Logger & Helpers
├── app.py              # Streamlit Entry Point
└── requirements.txt
