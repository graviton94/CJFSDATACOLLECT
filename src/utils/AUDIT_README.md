# Data Quality Audit Tool

## Overview
The `audit.py` script provides comprehensive data quality validation for the `hub_data.parquet` file. It ensures data integrity before dashboard deployment and helps identify issues with the data collection pipeline.

## Features

### 1. Schema Validation
- Verifies that all 13 required columns from `UNIFIED_SCHEMA` are present
- Detects extra columns not in the schema
- Checks column ordering

### 2. Missing Value Analysis
- Calculates the percentage of missing values for each column
- Treats `None`, `NaN`, and empty strings (`""`) as missing
- Categorizes issues as:
  - ✅ **Perfect**: 0% missing
  - ⚠️ **Minor**: < 10% missing
  - ⚠️ **Moderate**: 10-50% missing
  - ❌ **Critical**: > 50% missing

### 3. Mapping Failure Detection
Identifies rows where raw data exists but derived (lookup-based) data is missing:

| Raw Column (Input) | Derived Column (Output) |
|-------------------|------------------------|
| `product_type` | `top_level_product_type` |
| `product_type` | `upper_product_type` |
| `hazard_item` | `hazard_category` |

These failures indicate that the lookup/mapping logic in data collectors needs improvement.

## Usage

### Basic Usage
```bash
python src/utils/audit.py
```

This will automatically look for `data/hub_data.parquet` and generate a comprehensive report.

### Programmatic Usage
```python
from pathlib import Path
from utils.audit import DataQualityAuditor

# Default path (data/hub_data.parquet)
auditor = DataQualityAuditor()
auditor.run_full_audit()

# Custom path
auditor = DataQualityAuditor(parquet_path=Path("/path/to/custom.parquet"))
auditor.run_full_audit()
```

## Sample Output

```
============================================================
🔬 CJFSDATACOLLECT - Data Quality Audit Report
============================================================

📂 Loading data from: /path/to/hub_data.parquet
✅ Loaded 1,234 rows

📋 Executive Summary
============================================================
+----------------+-------------------------+
| Total Records  | 1,234                   |
+----------------+-------------------------+
| Schema Columns | 13 (Expected)           |
+----------------+-------------------------+
| Actual Columns | 13                      |
+----------------+-------------------------+
| Data Sources   | FDA, MFDS, RASFF        |
+----------------+-------------------------+

🔍 Schema Validation
============================================================
✅ Schema validation passed: All 13 columns present

📊 Missing Value Analysis
============================================================
+------------------------+-----------------+-------------+--------------+
| Column                 |   Missing Count | Missing %   | Status       |
+========================+=================+=============+==============+
| registration_date      |               0 | 0.00%       | ✅ Perfect    |
| data_source            |               0 | 0.00%       | ✅ Perfect    |
| ...                    |             ... | ...         | ...          |
+------------------------+-----------------+-------------+--------------+

🔎 Mapping Failure Detection
============================================================
+---------------------------------------+------------+-------------+------------+
| Mapping                               |   Failures | Failure %   | Status     |
+=======================================+============+=============+============+
| product_type → top_level_product_type |         45 | 3.65%       | ❌ Critical |
| ...                                   |        ... | ...         | ...        |
+---------------------------------------+------------+-------------+------------+

============================================================
✅ Audit completed successfully
============================================================
```

## When to Run

### Before Dashboard Deployment
Always run the audit before deploying or updating the Streamlit dashboard to ensure data quality.

### After Data Collection
Run after collecting new data from sources (FDA, MFDS, RASFF) to verify the ingestion was successful.

### During Development
Use when developing or updating data collectors to ensure they follow the schema correctly.

### Regular Monitoring
Schedule periodic audits (e.g., daily) to monitor data quality over time.

## Interpreting Results

### ✅ Perfect Status
All data is complete for this column. No action needed.

### ⚠️ Minor/Moderate Issues
Some data is missing. Review if this is expected:
- Some sources may not provide certain fields
- Optional fields may legitimately be empty

### ❌ Critical Issues
**Immediate action required:**
- **Schema violations**: Fix data collectors to output correct columns
- **High missing percentages**: Investigate why so much data is missing
- **Mapping failures**: Update lookup tables or improve fuzzy matching logic

## Dependencies
- pandas >= 2.0.0
- pyarrow >= 12.0.0
- tabulate >= 0.9.0

## Testing

Run the test suite to verify audit functionality:

```bash
# Unit test with sample data
python tests/test_audit.py

# Integration test with realistic data
python tests/test_audit_integration.py
```

## Related Files
- `src/schema.py` - Schema definition (UNIFIED_SCHEMA)
- `src/utils/storage.py` - Data storage utilities
- `data/reference/` - Lookup tables for mapping
- `app.py` - Streamlit dashboard

## Troubleshooting

### FileNotFoundError
```
❌ Parquet file not found: /path/to/hub_data.parquet
Please run data collectors first to generate the file.
```
**Solution**: Run data collectors to create `hub_data.parquet` first.

### ImportError
```
ModuleNotFoundError: No module named 'tabulate'
```
**Solution**: Install dependencies:
```bash
pip install -r requirements.txt
```

## Contributing

When modifying the audit script:
1. Ensure it works with the current `UNIFIED_SCHEMA` (13 columns)
2. Add tests for new validation logic
3. Update this README with new features
4. Run all existing tests to ensure no regression
