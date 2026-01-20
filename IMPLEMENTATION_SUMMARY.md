# **📋 Implementation Summary & Roadmap**

Last Updated: 2026-01-20  
Current Status: 🚧 Phase 2 In Progress: Ingestion & Schema Alignment  
*현재는 '데이터 자동 수집' 구현 중이며, 수집된 데이터의 스키마 정합성을 맞추는 고난이도 작업을 앞두고 있습니다.*

## **🏆 Project Roadmap (The Path to Final Goal)**

본 프로젝트는 단순한 크롤링 툴이 아니라, \*\*"가중치 기반 글로벌 식품안전 리스크 인텔리전스"\*\*를 지향합니다.

### **1️⃣ Raw Data Ingestion (Current Step 🔄)**

* **목표:** 지정된 모든 소스(API, Web)로부터 완전한 Raw Data를 끊김 없이 가져오는 자동화 파이프라인 구축.  
* **대상:** MFDS(API), FDA(Web), RASFF(Web), ImpFood(Web).  
* **상태:** 파이썬 모듈 구현 완료, 실제 데이터 적재 및 안정성 테스트 진행 중.

### **2️⃣ Schema Alignment & Transformation (The Hardest Part 🔥)**

* **목표:** 자동 수집된 데이터가 13개 표준 스키마(Unified Schema)의 적절한 셀에 정확히 배분되었는지 전수 검증.  
* **핵심 작업:**  
  * 규칙에 맞지 않게 들어온 데이터 식별 (Anomaly Detection).  
  * 비정형 텍스트(예: FDA Reason, ImpFood 위반내용)를 표준 항목으로 변환하는 정제 로직 고도화.  
  * **Note:** 가장 시간이 많이 소요되고 정교함이 요구되는 단계.

### **3️⃣ Master Data Management (Ready ✅)**

* **목표:** 자동 매핑이 실패하거나 새로운 유형이 발생했을 때, 사람이 개입하여 기준정보(백서)를 업데이트하는 체계.  
* **구현:** Streamlit '기준정보 관리' 탭 (CRUD 기능 완료).

### **4️⃣ Advanced Visualization**

* **목표:** 정제된 데이터를 바탕으로 다양한 필터링(기간, 국가, 위해요소 등)을 통해 현황을 조회하는 메뉴 생성.  
* **계획:** 차트 고도화, 동적 테이블, 드릴다운 리포트.

### **5️⃣ Risk Intelligence Dashboard (Final Goal 🏆)**

* **목표:** 단순 통계를 넘어 '위험'을 식별.  
* **기능:** 가중치 알고리즘 적용, 글로벌 이슈 식품유형/위험요소 실시간 랭킹 및 경보 시스템.

## **🚦 Module Status Board**

### **1\. Data Collectors (Ingestion Engine)**

| Source | Type | Status | Note |
| :---- | :---- | :---- | :---- |
| **🇰🇷 MFDS** | API | ✅ **Stable** | I2620, I0490 정규화 완료. |
| **🇺🇸 FDA** | CDC / Web | 🔄 **Implemented** | 변화 감지 로직 적용됨. 스키마 매핑 검증 필요. |
| **🇪🇺 RASFF** | Playwright | 🔄 **Refining** | 타임아웃 이슈 해결 및 HTML 파싱 구조 최적화 필요. |
| **🇰🇷 ImpFood** | Playwright | 🔄 **Implemented** | DOM 구조 파싱 로직 구현됨. 실제 적재 테스트 필요. |

### **2\. Core Logic & Storage**

* \[x\] **Unified Schema:** 13개 표준 컬럼 정의 완료. (src/schema.py)  
* \[x\] **Reference Loader:** 기준정보(품목/시험항목) 자동 생성기 구현 완료.  
* \[x\] **Scheduler:** 4대 정보원 통합 스케줄링 구조 완성.  
* \[ \] **Data Validation:** 수집 후 스키마 정합성 검증 로직 (To Do).

### **3\. User Interface (Streamlit)**

* \[x\] **Basic Dashboard:** 기본 프레임워크 및 데이터 로드 구현.  
* \[x\] **Admin Tab:** 기준정보 파일(Parquet) 수정 기능 구현.  
* \[ \] **Advanced Filter:** 상세 검색 및 복합 필터링 기능 (To Do).

## **📝 Immediate Next Actions**

1. **ImpFood & RASFF 안정화:** 로컬 환경에서 실제 데이터가 hub\_data.parquet에 누락 없이 쌓이는지 확인.  
2. **Schema Audit:** 수집된 데이터(Parquet)를 열어 top\_level\_product\_type이나 hazard\_category가 None이나 엉뚱한 값으로 들어간 케이스 전수 조사.  
3. **Refine Lookup Logic:** 조사 결과를 바탕으로 매핑 알고리즘(이름 기반 매핑) 수정 및 예외 처리 규칙 추가.
