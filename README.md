# 🛡️ Global Food Safety Intelligence Platform (CJFSDATACOLLECT)

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-ff4b4b.svg)](https://streamlit.io/)
[![Status](https://img.shields.io/badge/Status-Active_Development-green)]()
[![Vibe Coding](https://img.shields.io/badge/Built_with-Vibe_Coding-purple)]()

**CJFSDATACOLLECT**는 전 세계 식품 위해 정보를 실시간으로 수집, 정제, 시각화하는 통합 인텔리전스 플랫폼입니다.
한국(MFDS/ImpFood), 미국(FDA), 유럽(RASFF)의 데이터를 단일 스키마(Unified Schema)로 표준화하여 제공합니다.

---

## **🎯 Project Goals (Roadmap)**

본 프로젝트의 최종 완성을 향한 여정입니다.

1. **Data Ingestion Automation (Current Stage ✅)**  
   * 지정된 모든 입력 방식(API, HTML 크롤링)에 대해 완전한 Raw Data 수집 자동화.  
   * 4대 정보원: MFDS(API), FDA(CDC), RASFF(Playwright), ImpFood(Playwright).  
2. **Schema Normalization & Smart Lookup (Current Stage ✅)**  
   * 수집된 이종 데이터를 13개 표준 컬럼으로 자동 정렬.  
   * 기준정보(Reference Data)를 활용하여 품목 유형 및 위해 분류 자동 매핑.  
3. **Master Data Management (Completed ✅)**  
   * 데이터 정합성을 위해 사람이 직접 기준정보(백서)를 수정/관리할 수 있는 Streamlit Admin 메뉴 구축.  
4. **Advanced Visualization (Next Step 🚧)**  
   * Streamlit에서 다양한 필터링 조건으로 현황을 조회하는 차트/테이블 메뉴 고도화.  
5. **Global Risk Dashboard (Final Goal 🏆)**  
   * 가중치(Weighting) 알고리즘을 적용하여, 현재 글로벌 이슈 식품 유형과 위험 요소를 실시간으로 파악하는 **인텔리전스 대시보드** 완성.

---

## 🚀 Key Features

### 1. Multi-Source Data Ingestion
- **🇰🇷 MFDS (식약처):**
  - `I2620`: 국내식품 부적합 정보
  - `I0490`: 회수판매중지 정보
- **🇰🇷 ImpFood (수입식품정보마루):**
  - 수입식품 부적합 정보 (Playwright DOM Scraping)
- **🇺🇸 FDA (미국):**
  - Import Alerts (국가별 차단 리스트 CDC 수집)
- **🇪🇺 RASFF (유럽연합):**
  - 식품 및 사료 신속 경보 시스템 (Playwright Scraping)

### 2. Intelligent Data Processing
### 2. Intelligent Data Processing
- **Unified Schema:** 모든 소스를 14개 표준 컬럼으로 정규화.
- **Smart Lookup:** 기준정보(Parquet)를 활용하여 품목 유형(Hierarchy) 및 위해 분류(Category) 자동 매핑.
- **Deduplication:** 소스별 고유 ID를 기반으로 중복 데이터 자동 제거.

### 📜 Unified Schema (v2.1)
모든 데이터는 아래 14개 컬럼으로 표준화됩니다.

| Column | Description |
|--------|-------------|
| `registration_date` | 등록일자 (YYYY-MM-DD) |
| `data_source` | 데이터소스 (FDA, RASFF, MFDS, ImpFood) |
| `source_detail` | 상세출처 (API ID, Ref No 등) |
| `product_type` | 품목유형 (원본) |
| `top_level_product_type` | 최상위품목유형 (Lookup) |
| `upper_product_type` | 상위품목유형 (Lookup) |
| `product_name` | 제품명 |
| `origin_country` | 원산지 |
| `notifying_country` | 통보국 |
| `hazard_class_l` | **시험분류(대분류)** (New) |
| `hazard_class_m` | **시험분류(중분류)** (New) |
| `hazard_item` | 시험항목 (위해정보 원본) |
| `full_text` | 전문 (원본 본문) |
| `analyzable` | 분석가능여부 (Boolean) |
| `interest_item` | 관심항목 (Boolean) |

### 🆕 Patch Notes (v2.1)
- **Schema Update:** 기존 `hazard_category`가 모호하여 `hazard_class_l` (대분류)와 `hazard_class_m` (중분류)로 분리되었습니다.
- **Improved Matching:** FDA, ImpFood 수집 시 전문(Full Text) 기반의 Fuzzy Matching 로직이 강화되었습니다.
- **UI Enhancement:** 분류 체계 변경에 따른 대시보드 필터 및 차트(Pie/Bar)가 세분화되었습니다.

### 3. Interactive Dashboard
- **Streamlit 기반 UI:** 데이터 검색, 필터링, 시각화(Plotly).
- **Master Data Management:** 기준정보 파일(Parquet) 직접 조회 및 수정 기능.
- **Export:** 한글 깨짐 없는(UTF-8-SIG) CSV 다운로드 지원.

## 🛠️ Tech Stack

- **Language:** Python 3.9+
- **Data Collection:** `requests`, `playwright`, `BeautifulSoup`
- **Data Processing:** `pandas`, `pyarrow`
- **Storage:** Parquet (Local File System)
- **Visualization:** `streamlit`, `plotly`
- **Scheduler:** `schedule` (Lightweight Job Scheduling)

---

## 📥 Installation & Usage

### 1. Setup Environment
```bash
# 가상환경 생성 및 활성화
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt

# Playwright 브라우저 설치 (필수)
playwright install
```
### 2. Environment Variables
Create .env file:
```Ini, TOML
MFDS_API_KEY=your_api_key_here
```
### 3. Run Scheduler (Data Collection)
```Bash
# 1회 즉시 실행 (테스트용)
python src/scheduler.py --mode once

# 매일 정해진 시간에 실행
python src/scheduler.py --mode schedule --time 09:00
```
### 4. Run Dashboard
```Bash
streamlit run app.py
```

## 📂 Project Structure
```plaintext
cjfsdatacollect/
├── app.py                  # Streamlit Dashboard Entry Point
├── data/
│   ├── hub/                # Collected Data (hub_data.parquet)
│   └── reference/          # Master Data (Product/Hazard Codes)
├── src/
│   ├── collectors/         # Source-specific Scrapers (MFDS, FDA, RASFF, ImpFood)
│   ├── utils/              # Storage, Deduplication, Reference Loaders
│   ├── schema.py           # Unified Schema Definition & Validation
│   └── scheduler.py        # Central Job Scheduler
└── tests/                  # Unit & Integration Tests
```
