# 📊 Data Schema Documentation

The central database (`hub_data.parquet`) strictly adheres to the following schema to ensure consistency across different countries and sources.

## Unified Target Schema (14 Columns)

All data collectors MUST return a `pd.DataFrame` with exactly these **14 columns** defined in `src/schema.py`.

| Column Name | Type | Description | Example |
|:---|:---:|:---|:---|
| `registration_date` | `str` | 등록일자 (YYYY-MM-DD) | `2024-05-20` |
| `data_source` | `str` | 데이터소스 | `MFDS`, `FDA`, `RASFF`, `ImpFood` |
| `source_detail` | `str` | 상세출처 (API ID, Ref No 등) | `I2620-1234`, `Import Alert 99-01` |
| `product_type` | `str` | 품목유형 (원본) | `Fishery products`, `냉이` |
| `top_level_product_type` | `str` | 최상위품목유형 (Lookup) | `수산물`, `농산물` |
| `upper_product_type` | `str` | 상위품목유형 (Lookup) | `냉동수산물`, `채소류` |
| `product_name` | `str` | 제품명 | `Frozen Shrimp`, `건조 표고버섯` |
| `origin_country` | `str` | 원산지 (Standardized) | `Vietnam`, `South Korea` |
| `notifying_country` | `str` | 통보국 (Reporting Country) | `South Korea`, `United States`, `EU Member States` |
| `hazard_category` | `str` | 분류(카테고리) (Lookup) | `미생물`, `농약`, `중금속` |
| `hazard_item` | `str` | 시험항목 (위해정보 원본) | `Salmonella`, `펜디메탈린` |
| `full_text` | `str` | 전문 (원본 본문, Nullable) | `Detected 15ppm of Aflatoxin in sample` |
| `analyzable` | `bool` | 분석가능여부 (Lookup) | `True`, `False` |
| `interest_item` | `bool` | 관심항목 (Lookup) | `True`, `False` |

## Mapping Rules

### 1. Date Normalization
- **Input Format:** Various (YYYY.MM.DD, MM/DD/YYYY, YYYYMMDD, YYYY-MM-DD HH:MM:SS)
- **Output Format:** `YYYY-MM-DD` (String)
- **Example:** `2024.03.12` → `2024-03-12`

### 2. Lookup Strategy
- **Product Hierarchy:** `product_type` → `product_code_master.parquet` → `top_level_product_type`, `upper_product_type`
- **Hazard Classification:** `hazard_item` → `hazard_code_master.parquet` → `hazard_category`, `analyzable`, `interest_item`
- **Country Names:** Raw text → `country_master.tsv` → Standardized English name

### 3. Full Text Usage
- **Purpose:** Store raw context/description for AI-powered hazard extraction
- **Use Cases:**
  - MFDS I0490: `RTRVLPRVNS` (회수사유) field
  - MFDS I2810: Combined `DETECT_TITL` + `BDT` fields
  - Other sources: Any unstructured text containing hazard information
- **Migration Rule:** For existing data or sources without full text, use `None` or empty string `""`
- **Best Practice:** Extract precise `hazard_item` from `full_text` using fuzzy matching, do NOT put long sentences in `hazard_item`

### 4. Data Validation
- **Schema Enforcement:** Always call `src.schema.validate_schema(df)` before returning DataFrame
- **Column Order:** Must match `UNIFIED_SCHEMA` order exactly
- **Missing Columns:** Automatically filled with `None` (strings) or `False` (booleans)
- **Extra Columns:** Removed during validation
