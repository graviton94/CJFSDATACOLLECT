# 🏗️ System Architecture

## 1. Data Pipeline Overview

```mermaid
graph LR
    A[Data Sources] --> B[Collectors]
    B --> C[Normalization & Lookup]
    C --> D[Deduplication]
    D --> E[Storage (Parquet)]
    E --> F[Dashboard (Streamlit)]
```
**A. Data Sources**
- API: MFDS (OpenAPI)
- Web Scraping: FDA (HTML Parsing), RASFF (SPA/Playwright), ImpFood (DOM/Playwright)

**B. Collectors (src/collectors/)**
- Each collector inherits or implements a common interface logic.
- Responsibility: Fetch raw data -> Parse -> Map to UNIFIED_SCHEMA.
- Smart Enrichment:
-- Calls ReferenceLoader to map Product Name -> Hierarchy (Top/Upper).
-- Calls ReferenceLoader to map Hazard Item -> Category.

**C. Normalization (src/schema.py)**
- Strict 13-Column Schema:
    1. registration_date
    2. data_source
    3. source_detail (Unique Key)
    4. product_type
    5. top_level_product_type (Enriched)
    6. upper_product_type (Enriched)
    7. product_name
    8. origin_country
    9. notifying_country
    10. hazard_category (Enriched)
    11. hazard_item
    12. analyzable (Enriched)
    13. interest_item (Enriched)

**D. Storage Strategy (src/utils/storage.py)**
- Format: Apache Parquet (snappy compression).
- Location: data/hub/hub_data.parquet.
- Logic: Append-only with deduplication check based on data_source + source_detail.
---

## 2. Key Components Design

**Reference Data Lookup System**
- File: data/reference/*.parquet
- Logic:
-- Product Lookup: Matches input string against KOR_NM or ENG_NM in master data to retrieve Hierarchy Names.
-- Hazard Lookup: Matches input string against multiple columns (Name, Alias, Abbr) to retrieve Category.

**Scheduler**
- Uses schedule library for lightweight task management.
- Runs collectors sequentially to avoid resource contention (especially for Headless Browsers).
