# 📊 Data Schema Documentation

The central database (`hub_data.parquet`) strictly adheres to the following schema to ensure consistency across different countries and sources.

## Unified Target Schema (16 Columns)

| Column Name (EN) | Description (KR) | Type | Example |
|:---|:---|:---:|:---|
| `id` | 구분 (Internal ID) | str | `UUID-v4` |
| `ref_no` | 일련번호 (Source ID) | str | `2024.1234` |
| `source` | 출처 | str | `FDA`, `RASFF`, `MFDS` |
| `date_registered` | 등록일자 | datetime | `2024-05-20` |
| `product_type_raw` | 식품유형 (원본) | str | `Fishery products` |
| `product_type` | 식품유형 (표준) | str | `수산물` |
| `category` | 분류 | str | `식품` |
| `product_name` | 제품명 | str | `Frozen Shrimp` |
| `origin_raw` | 원산지 (원본) | str | `Vietnam` |
| `origin` | 원산지 (표준) | str | `베트남` |
| `notifying_country_raw`| 통보국 (원본) | str | `Germany` |
| `notifying_country` | 통보국 (표준) | str | `독일` |
| `hazard_reason` | 시험항목/위해사유 | str | `Salmonella detected` |
| `analyzable` | 분석가능여부 | bool | `True` |
| `hazard_category` | 항목분류 | str | `미생물` |
| `tags` | 관심/누적 태그 | list | `['Shrimp', 'High Risk']` |

## Mapping Rules
- **Date:** All dates must be converted to `YYYY-MM-DD`.
- **Translation:** `_raw` columns keep the original English/Local text. Non-raw columns utilize the Dictionary mappings located in `data/indices/`.
