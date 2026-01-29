# FDA Import Alert Collection Summary

## Latest Collection Run

- **Timestamp**: 2026-01-29 09:14:14
- **Records Collected**: 42989
- **Data Source**: FDA Import Alerts (iapublishdate.html)
- **Method**: Context-Aware Block Parsing with Regex Date Detection

## Collection Details

The collector implements the following workflow:

1. **Index Page Extraction**: Fetches https://www.accessdata.fda.gov/cms_ia/iapublishdate.html
2. **Alert Discovery**: Extracts Alert Numbers and detail page URLs
3. **Detail Page Parsing**:
   - Skips first summary block (Alert #, Published Date, Type)
   - Uses regex pattern `(\d{2}/\d{2}/\d{4})` to find dates
   - Anchors data extraction around each date:
     * Product Code: Line 1 row above date
     * Description: Lines below date
     * Country: Nearest preceding `<div class="center"><h4>` tag
4. **Data Normalization**:
   - Country names normalized via ReferenceLoader
   - Hazard categories mapped via FuzzyMatcher
5. **Schema Validation**: All records validated against 14-column unified schema

## Schema Compliance

✅ All 42989 records conform to the 14-column unified schema defined in `src/schema.py`.

## Notes

- Default alert processing limit is None for testing
- To process all alerts in production, use: `FDACollector(alert_limit=None)`
- Full text context is preserved in the `full_text` column for future AI-based extraction
