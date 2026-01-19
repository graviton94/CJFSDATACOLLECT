# 🏗️ System Architecture

## 1. Overview
The system follows a standard **ETL (Extract, Transform, Load)** pattern, optimized for lightweight execution on local servers or cloud containers.

```mermaid
graph LR
    A[Sources: FDA, RASFF, KR-API] -->|Ingest| B(Collectors)
    B -->|Raw Data| C{Processors}
    C -->|Normalize & Dedup| D[(Hub Parquet DB)]
    D -->|Read| E[Streamlit Dashboard]

## 2. Ingestion Strategy (Extract)
🇪🇺 EU RASFF (Complex/Dynamic)
- Challenge: The portal is a Single Page Application (SPA) utilizing heavy JavaScript.
- Solution: Use Playwright to simulate a browser user.
- Logic: Navigate to search page -> Wait for table load -> Parse visible rows -> Pagination.

🇺🇸 US FDA (Hierarchical/Static)
- Challenge: Too many detail pages to scrape daily.
- Solution: Count-Based CDC.
  1. Scrape the "Country List" page first.
  2. Compare current alert counts per country against data/state/fda_counts.json.
  3. Only visit country detail pages where the count has increased.

🇰🇷 KR MFDS (Structured)
- Method: Standard REST API calls.
- Logic: Iterate through service IDs (I2620, I0470, I0490) with pagination (1~1000, 1001~2000) until empty.

## 3. Transformation & Storage
- Schema Mapping: All incoming data is mapped to Target Schema (16 Headers) defined in Project_Milestone.md.
- Deduplication:
  - Key: source_id + reference_number (or url).
  -  New records are appended to hub_data.parquet.
  - Existing records are updated only if critical fields change.

## 4. UI Layer
- Streamlit: Reads directly from Parquet using Pandas.
- Optimization: Uses @st.cache_data to minimize disk I/O during user interaction.
