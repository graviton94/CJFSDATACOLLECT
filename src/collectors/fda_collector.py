"""
FDA Import Alert Collector with Context-Aware Block Parsing

This module implements a sophisticated parsing logic that:
1. Starts from the FDA Import Alert publish date index page
2. Extracts detail page links for each Import Alert
3. Parses detail pages using regex-based date detection
4. Anchors data blocks around dates to extract product info, country, and hazard details
5. Maps all data to the unified 14-column schema

Key Features:
- Regex-based date pattern matching (MM/DD/YYYY)
- DOM traversal to find nearest country headers
- Lookup integration for country normalization and hazard classification
- Full schema compliance with validation
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime

import requests
import pandas as pd
from bs4 import BeautifulSoup, NavigableString, Tag

from src.schema import UNIFIED_SCHEMA, validate_schema, get_empty_dataframe
from src.utils.reference_loader import ReferenceLoader
from src.utils.fuzzy_matcher import FuzzyMatcher


class FDACollector:
    """
    US FDA Import Alert collector with precision block parsing.
    
    Workflow:
    1. Fetch index page: https://www.accessdata.fda.gov/cms_ia/iapublishdate.html
    2. Extract Alert Numbers and detail page URLs
    3. For each detail page:
       - Skip the first summary block (Alert #, Published Date, Type)
       - Find all dates using regex (MM/DD/YYYY)
       - For each date:
         * Product Name: Line exactly 1 row above the date
         * Country: Nearest preceding <div class="center"><h4> tag
         * Charge/Reason: Extract associated text block
    4. Normalize data using ReferenceLoader and FuzzyMatcher
    5. Save results to data/hub/ in Parquet format
    """
    
    BASE_URL = "https://www.accessdata.fda.gov/cms_ia"
    INDEX_URL = f"{BASE_URL}/iapublishdate.html"
    OUTPUT_DIR = Path("data/hub")
    REPORT_DIR = Path("reports")
    
    # Regex pattern for MM/DD/YYYY date format
    DATE_PATTERN = re.compile(r'\b(\d{2}/\d{2}/\d{4})\b')
    
    def __init__(self):
        """Initialize the collector with necessary directories."""
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.REPORT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Initialize utilities
        self.fuzzy_matcher = FuzzyMatcher()
        
        # Load reference data for country normalization
        self._load_reference_data()
    
    def _load_reference_data(self):
        """Load reference data for lookups."""
        reference_dir = Path("data/reference")
        
        # Load country master data if available
        country_file = reference_dir / "country_master.parquet"
        if country_file.exists():
            self.country_ref = pd.read_parquet(country_file)
        else:
            self.country_ref = pd.DataFrame()
        
        # Load hazard master data if available
        hazard_file = reference_dir / "hazard_code_master.parquet"
        if hazard_file.exists():
            self.hazard_ref = pd.read_parquet(hazard_file)
        else:
            self.hazard_ref = pd.DataFrame()
    
    def fetch_index_page(self) -> List[Dict[str, str]]:
        """
        Fetch the Import Alert index page and extract alert information.
        
        Returns:
            List of dictionaries containing alert_number and detail_url
        """
        alerts = []
        
        try:
            response = requests.get(self.INDEX_URL, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the table containing alert listings
            table = soup.find('table')
            if not table:
                print("⚠️ No table found on index page")
                return alerts
            
            # Extract alert numbers and links
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue
                
                # First column typically contains the alert number with link
                link = cols[0].find('a')
                if not link:
                    continue
                
                alert_number = link.text.strip()
                href = link.get('href', '')
                
                if href:
                    # Construct full URL
                    detail_url = f"{self.BASE_URL}/{href}" if not href.startswith('http') else href
                    alerts.append({
                        'alert_number': alert_number,
                        'detail_url': detail_url
                    })
            
            print(f"✅ Found {len(alerts)} Import Alerts")
            
        except Exception as e:
            print(f"❌ Error fetching index page: {e}")
        
        return alerts
    
    def _find_nearest_country_header(self, element: Tag) -> Optional[str]:
        """
        Find the nearest preceding <div class="center"><h4> tag for country name.
        
        Args:
            element: The starting element (typically near a date)
            
        Returns:
            Country name as string, or None if not found
        """
        current = element
        
        # Traverse up the DOM tree
        while current:
            # Check previous siblings
            for sibling in current.find_previous_siblings():
                if isinstance(sibling, Tag):
                    # Look for <div class="center"> containing <h4>
                    if sibling.name == 'div' and 'center' in sibling.get('class', []):
                        h4 = sibling.find('h4')
                        if h4:
                            return h4.get_text(strip=True)
                    
                    # Also check nested structures
                    center_div = sibling.find('div', class_='center')
                    if center_div:
                        h4 = center_div.find('h4')
                        if h4:
                            return h4.get_text(strip=True)
            
            # Move to parent
            current = current.parent
            
            # Don't go too far up (e.g., past body tag)
            if current and current.name == 'body':
                break
        
        return None
    
    def _extract_product_and_desc(self, text_lines: List[str], date_index: int) -> Tuple[str, str, str]:
        """
        Extract product name, description, and additional context from text lines.
        
        Args:
            text_lines: List of text lines from the block
            date_index: Index of the line containing the date
            
        Returns:
            Tuple of (product_code_line, description, full_text)
        """
        product_code_line = ""
        description = ""
        full_text_parts = []
        
        # Line above date = Product Name/Code (e.g., "03 R - -..")
        if date_index > 0:
            product_code_line = text_lines[date_index - 1].strip()
        
        # Lines below date = Description
        desc_parts = []
        for i in range(date_index + 1, len(text_lines)):
            line = text_lines[i].strip()
            if line:
                desc_parts.append(line)
        
        description = " ".join(desc_parts)
        
        # Full text includes everything except the date line itself
        for i, line in enumerate(text_lines):
            if i != date_index:  # Skip the date line
                stripped = line.strip()
                if stripped:
                    full_text_parts.append(stripped)
        
        full_text = " ".join(full_text_parts)
        
        return product_code_line, description, full_text
    
    def parse_detail_page(self, alert_info: Dict[str, str]) -> List[Dict]:
        """
        Parse a single Import Alert detail page using context-aware block parsing.
        
        Args:
            alert_info: Dictionary with 'alert_number' and 'detail_url'
            
        Returns:
            List of record dictionaries
        """
        alert_num = alert_info['alert_number']
        url = alert_info['detail_url']
        
        print(f"   Parsing Alert {alert_num}...")
        
        records = []
        
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get all text content
            page_text = soup.get_text()
            
            # Find all dates in the page using regex
            date_matches = list(self.DATE_PATTERN.finditer(page_text))
            
            if not date_matches:
                print(f"      ⚠️ No dates found in Alert {alert_num}")
                return records
            
            # Skip the first date (summary block)
            # The first date is usually in the summary section (Alert #, Published Date, Type)
            date_matches_to_process = date_matches[1:] if len(date_matches) > 1 else date_matches
            
            print(f"      Found {len(date_matches_to_process)} data blocks (skipped summary)")
            
            # Process each date occurrence
            for match in date_matches_to_process:
                date_str = match.group(1)
                
                # Parse date
                try:
                    dt = datetime.strptime(date_str, "%m/%d/%Y")
                    reg_date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    # Skip invalid dates
                    continue
                
                # Find the element containing this date in the DOM
                # We'll search for text nodes containing the date
                date_element = None
                for element in soup.find_all(string=re.compile(re.escape(date_str))):
                    if isinstance(element, NavigableString):
                        date_element = element.parent
                        break
                
                if not date_element:
                    continue
                
                # Extract country from nearest header
                country_name = self._find_nearest_country_header(date_element)
                if not country_name:
                    country_name = "Unknown"
                
                # Normalize country name using reference data
                origin_country = self._normalize_country_name(country_name)
                
                # Extract product and description from surrounding context
                # Get the parent block of text
                parent_text = date_element.get_text(separator='\n', strip=True)
                text_lines = parent_text.split('\n')
                
                # Find which line contains the date
                date_line_index = -1
                for i, line in enumerate(text_lines):
                    if date_str in line:
                        date_line_index = i
                        break
                
                if date_line_index >= 0:
                    product_code, desc, full_text = self._extract_product_and_desc(
                        text_lines, date_line_index
                    )
                else:
                    product_code = ""
                    desc = ""
                    full_text = parent_text
                
                # Use fuzzy matcher to extract hazard info from full_text
                hazard_info = self.fuzzy_matcher.match_hazard_category(
                    full_text, 
                    self.hazard_ref
                )
                
                # Build the record according to schema
                record = {
                    "registration_date": reg_date,
                    "data_source": "FDA",
                    "source_detail": f"Import Alert {alert_num}",
                    "product_type": product_code if product_code else "Imported Food",
                    "top_level_product_type": None,  # Lookup not applied yet
                    "upper_product_type": None,      # Lookup not applied yet
                    "product_name": desc if desc else full_text[:100],
                    "origin_country": origin_country,
                    "notifying_country": "United States",
                    "hazard_category": hazard_info.get('category'),
                    "hazard_item": alert_num,  # Use alert number as hazard item identifier
                    "full_text": full_text if full_text else None,
                    "analyzable": hazard_info.get('analyzable', False),
                    "interest_item": hazard_info.get('interest', False)
                }
                
                records.append(record)
            
        except Exception as e:
            print(f"      ❌ Error parsing Alert {alert_num}: {e}")
        
        return records
    
    def _normalize_country_name(self, raw_country: str) -> str:
        """
        Normalize country name using reference data.
        
        Args:
            raw_country: Raw country name from FDA page
            
        Returns:
            Standardized country name
        """
        if not raw_country or self.country_ref.empty:
            return raw_country
        
        # Try exact match first (case-insensitive)
        raw_lower = raw_country.lower().strip()
        
        for col in ['country_name_eng', 'country_name_kor']:
            if col in self.country_ref.columns:
                mask = self.country_ref[col].astype(str).str.lower().str.strip() == raw_lower
                if mask.any():
                    matched = self.country_ref[mask].iloc[0]
                    return matched.get('country_name_eng', raw_country)
        
        # Return as-is if no match found
        return raw_country
    
    def collect(self) -> pd.DataFrame:
        """
        Main collection workflow.
        
        Returns:
            DataFrame with collected data in unified schema format
        """
        print("🚀 [FDA] Starting Import Alert collection (Precision Block Parsing)...")
        
        # Step 1: Fetch index page
        alerts = self.fetch_index_page()
        
        if not alerts:
            print("⚠️ No alerts found or index page unavailable")
            return get_empty_dataframe()
        
        # Step 2: Parse detail pages
        all_records = []
        
        # Limit to first 5 alerts for testing/demo purposes
        # Remove this limit for production
        alerts_to_process = alerts[:5] if len(alerts) > 5 else alerts
        
        for alert_info in alerts_to_process:
            records = self.parse_detail_page(alert_info)
            all_records.extend(records)
        
        if not all_records:
            print("⚠️ No records extracted from detail pages")
            return get_empty_dataframe()
        
        # Step 3: Convert to DataFrame and validate schema
        df = pd.DataFrame(all_records)
        df = validate_schema(df)
        
        print(f"✅ Collected {len(df)} records from {len(alerts_to_process)} alerts")
        
        # Step 4: Save to Parquet
        output_file = self.OUTPUT_DIR / f"fda_import_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
        print(f"💾 Saved to {output_file}")
        
        # Step 5: Update summary report
        self._update_summary_report(len(df))
        
        return df
    
    def _update_summary_report(self, record_count: int):
        """
        Update the FDA collection summary report.
        
        Args:
            record_count: Number of new records collected
        """
        report_file = self.REPORT_DIR / "fda_collect_summary.md"
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report_content = f"""# FDA Import Alert Collection Summary

## Latest Collection Run

- **Timestamp**: {timestamp}
- **Records Collected**: {record_count}
- **Data Source**: FDA Import Alerts (iapublishdate.html)
- **Method**: Context-Aware Block Parsing with Regex Date Detection

## Collection Details

The collector implements the following workflow:

1. **Index Page Extraction**: Fetches https://www.accessdata.fda.gov/cms_ia/iapublishdate.html
2. **Alert Discovery**: Extracts Alert Numbers and detail page URLs
3. **Detail Page Parsing**:
   - Skips first summary block (Alert #, Published Date, Type)
   - Uses regex pattern `(\\d{{2}}/\\d{{2}}/\\d{{4}})` to find dates
   - Anchors data extraction around each date:
     * Product Code: Line 1 row above date
     * Description: Lines below date
     * Country: Nearest preceding `<div class="center"><h4>` tag
4. **Data Normalization**:
   - Country names normalized via ReferenceLoader
   - Hazard categories mapped via FuzzyMatcher
5. **Schema Validation**: All records validated against 14-column unified schema

## Schema Compliance

✅ All {record_count} records conform to the 14-column unified schema defined in `src/schema.py`.

## Notes

- Current run processed the first 5 alerts for testing purposes
- To process all alerts, remove the limit in the `collect()` method
- Full text context is preserved in the `full_text` column for future AI-based extraction
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(f"📝 Updated summary report: {report_file}")


if __name__ == "__main__":
    collector = FDACollector()
    df = collector.collect()
    print(f"\n📊 Collection complete: {len(df)} records")
    print(df.head())