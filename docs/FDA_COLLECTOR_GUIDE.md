# FDA Import Alert Precision Block Parsing - Implementation Guide

## Overview

This implementation introduces a sophisticated context-aware block parsing logic for FDA Import Alert data collection, replacing the old country-based CDC (Change Detection Count) approach.

## Key Features

### 1. Regex-Based Date Detection
- **Pattern**: `(\d{2}/\d{2}/\d{4})` matches MM/DD/YYYY format
- **Flexibility**: No word boundaries to handle various surrounding punctuation
- **Skip Summary**: First date occurrence is skipped (summary block)

### 2. DOM Traversal for Context Extraction
```python
# Product Name/Code: Line exactly 1 row above date
product_code = text_lines[date_index - 1]

# Description: Lines following the date
description = " ".join(text_lines[date_index + 1:])

# Country: Nearest preceding <div class="center"><h4> tag
country = _find_nearest_country_header(date_element)
```

### 3. Data Flow Architecture
```
Index Page (iapublishdate.html)
    ↓
Extract Alert Numbers + Detail URLs
    ↓
For Each Detail Page (importalert_XXX.html)
    ↓
Find All Dates (skip first)
    ↓
For Each Date Block
    ├─→ Extract Product Code (line above)
    ├─→ Extract Description (lines below)
    ├─→ Find Country (DOM traversal)
    └─→ Extract Full Text Context
    ↓
Normalize via ReferenceLoader & FuzzyMatcher
    ↓
Validate Against 14-Column Schema
    ↓
Save to data/hub/*.parquet
```

## Schema Mapping

```python
{
    "registration_date": "YYYY-MM-DD",          # Parsed from MM/DD/YYYY
    "data_source": "FDA",                       # Fixed
    "source_detail": "Import Alert {num}",      # Alert number
    "product_type": "{product_code}",           # Line above date
    "top_level_product_type": None,             # Lookup (future)
    "upper_product_type": None,                 # Lookup (future)
    "product_name": "{description}",            # Lines below date
    "origin_country": "{normalized_country}",   # Via ReferenceLoader
    "notifying_country": "United States",       # Fixed
    "hazard_category": "{category}",            # Via FuzzyMatcher
    "hazard_item": "{alert_num}",               # Alert number
    "full_text": "{context}",                   # Full block (excl. date)
    "analyzable": False/True,                   # Via FuzzyMatcher
    "interest_item": False/True                 # Via FuzzyMatcher
}
```

## Configuration

### Testing Mode (Default)
```python
collector = FDACollector()  # Processes first 5 alerts
# or
collector = FDACollector(alert_limit=5)
```

### Production Mode
```python
collector = FDACollector(alert_limit=None)  # Processes ALL alerts
```

### Custom Limit
```python
collector = FDACollector(alert_limit=10)  # Processes first 10 alerts
```

## Usage Examples

### Basic Collection
```python
from src.collectors.fda_collector import FDACollector

collector = FDACollector()
df = collector.collect()
print(f"Collected {len(df)} records")
```

### Production Collection
```python
import os
from pathlib import Path

# Set environment or use configuration
collector = FDACollector(alert_limit=None)  # All alerts
df = collector.collect()

# Data automatically saved to:
# - data/hub/fda_import_alerts_{timestamp}.parquet
# - reports/fda_collect_summary.md
```

### Inspect Output
```python
print(df.columns)  # 14-column unified schema
print(df.head())
print(df['data_source'].unique())  # ['FDA']
print(df['origin_country'].value_counts())  # Country distribution
```

## File Structure

```
src/collectors/fda_collector.py          # Main collector implementation
tests/test_fda_precision_parsing.py      # Comprehensive unit tests
data/hub/                                 # Output directory (Parquet files)
reports/fda_collect_summary.md           # Auto-generated summary
data/reference/country_master.parquet    # Country normalization lookup
data/reference/hazard_code_master.parquet # Hazard classification lookup
```

## Integration Points

### 1. ReferenceLoader (Country Normalization)
```python
# Loaded from data/reference/country_master.parquet
# Columns: country_name_eng, country_name_kor, iso_2, iso_3
self.country_ref = pd.read_parquet(country_file)
```

### 2. FuzzyMatcher (Hazard Classification)
```python
# Loaded from data/reference/hazard_code_master.parquet
# Columns: KOR_NM, ENG_NM, M_KOR_NM, ANALYZABLE, INTEREST_ITEM
hazard_info = self.fuzzy_matcher.match_hazard_category(
    full_text, 
    self.hazard_ref
)
```

### 3. Schema Validator
```python
# Always validate before saving
df = validate_schema(df)
# Ensures:
# - All 14 columns present
# - Correct data types (bool, str, date)
# - Date format: YYYY-MM-DD
```

## Testing

### Run All Tests
```bash
cd /home/runner/work/CJFSDATACOLLECT/CJFSDATACOLLECT
python3 tests/test_fda_precision_parsing.py
```

### Test Coverage
- ✅ Collector initialization (default, custom, production modes)
- ✅ Date regex pattern matching
- ✅ Product/description extraction logic
- ✅ Country name normalization
- ✅ Schema validation
- ✅ Empty DataFrame handling
- ✅ DOM traversal for country headers

## Security

### CodeQL Scan Results
```
✅ No security vulnerabilities found
- Python: 0 alerts
```

### Best Practices Applied
- ✅ Input validation (date parsing, URL construction)
- ✅ Timeout on HTTP requests (15 seconds)
- ✅ Error handling without exposing internals
- ✅ No hardcoded credentials
- ✅ Safe file path handling (pathlib.Path)

## Troubleshooting

### No Alerts Found
```
⚠️ No alerts found or index page unavailable
```
**Solution**: Check network connectivity to `www.accessdata.fda.gov`

### No Dates Found
```
⚠️ No dates found in Alert {num}
```
**Solution**: HTML structure may have changed, verify regex pattern

### Import Errors
```
ModuleNotFoundError: No module named 'src'
```
**Solution**: Ensure working directory is project root, or use relative imports

## Changelog

### Version 2.0 (Current)
- ✅ Regex-based date detection
- ✅ DOM traversal for country extraction
- ✅ Configurable alert processing limit
- ✅ Full schema compliance
- ✅ Comprehensive unit tests
- ✅ Auto-generated summary reports

### Version 1.0 (Deprecated)
- ❌ Country-based CDC (Change Detection Count)
- ❌ Hardcoded parsing logic
- ❌ Limited error handling

## Support

For issues or questions:
1. Check this documentation first
2. Review test cases in `tests/test_fda_precision_parsing.py`
3. Examine schema definition in `src/schema.py`
4. Consult utility implementations in `src/utils/`

## License

Part of the CJFSDATACOLLECT project.
