"""
Tests for FDA Import Alert Precision Block Parsing

This module tests the new FDA collector implementation that uses:
- Regex-based date extraction (MM/DD/YYYY)
- DOM traversal for country headers
- Context-aware block parsing
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from collectors.fda_collector import FDACollector
from schema import validate_schema, UNIFIED_SCHEMA


def test_fda_collector_initialization():
    """Test that FDA collector initializes correctly."""
    # Test with default limit
    collector = FDACollector()
    
    # Check directories are created
    assert collector.OUTPUT_DIR.exists(), "Output directory should be created"
    assert collector.REPORT_DIR.exists(), "Report directory should be created"
    
    # Check fuzzy matcher is initialized
    assert collector.fuzzy_matcher is not None, "Fuzzy matcher should be initialized"
    
    # Check default alert limit
    assert collector.alert_limit == FDACollector.DEFAULT_ALERT_LIMIT, "Should use default alert limit"
    
    # Test with custom limit
    collector_custom = FDACollector(alert_limit=10)
    assert collector_custom.alert_limit == 10, "Should use custom alert limit"
    
    # Test with no limit (production mode)
    collector_prod = FDACollector(alert_limit=None)
    assert collector_prod.alert_limit is None, "Should have no limit in production mode"
    
    print("✓ FDA collector initialization test passed")


def test_fda_date_pattern_regex():
    """Test that the date regex pattern works correctly."""
    import re
    
    collector = FDACollector()
    pattern = collector.DATE_PATTERN
    
    # Test valid dates
    test_cases = [
        ("Product info 01/15/2024 Description", ["01/15/2024"]),
        ("Multiple 12/31/2023 dates 06/01/2024 here", ["12/31/2023", "06/01/2024"]),
        ("No dates here", []),
        ("Invalid 13/45/2024 date", ["13/45/2024"]),  # Regex matches, but parsing will fail
    ]
    
    for text, expected_matches in test_cases:
        matches = pattern.findall(text)
        assert matches == expected_matches, f"Expected {expected_matches}, got {matches}"
    
    print("✓ Date pattern regex test passed")


def test_extract_product_and_desc():
    """Test the product and description extraction logic."""
    collector = FDACollector()
    
    # Test case: Normal block with date in the middle
    text_lines = [
        "03 R - Product Code",
        "01/15/2024",
        "Description line 1",
        "Description line 2"
    ]
    
    product_code, desc, full_text = collector._extract_product_and_desc(text_lines, 1)
    
    assert product_code == "03 R - Product Code", f"Expected '03 R - Product Code', got '{product_code}'"
    assert desc == "Description line 1 Description line 2", f"Unexpected description: {desc}"
    assert "01/15/2024" not in full_text, "Date line should be excluded from full_text"
    assert "03 R - Product Code" in full_text, "Product code should be in full_text"
    
    print("✓ Product and description extraction test passed")


def test_normalize_country_name():
    """Test country name normalization."""
    collector = FDACollector()
    
    # Test without reference data
    result = collector._normalize_country_name("China")
    assert result == "China", "Should return original if no reference data"
    
    # Test with empty reference
    collector.country_ref = pd.DataFrame()
    result = collector._normalize_country_name("India")
    assert result == "India", "Should return original with empty reference"
    
    print("✓ Country name normalization test passed")


def test_schema_validation():
    """Test that collector produces valid schema output."""
    # Use a constant for the test date
    TEST_DATE = "2024-01-15"
    TEST_ALERT_NUM = "99-19"
    
    # Create a mock record
    mock_records = [{
        "registration_date": TEST_DATE,
        "data_source": "FDA",
        "source_detail": f"Import Alert {TEST_ALERT_NUM}",
        "product_type": "Imported Food",
        "top_level_product_type": None,
        "upper_product_type": None,
        "product_name": "Test Product",
        "origin_country": "China",
        "notifying_country": "United States",
        "hazard_category": None,
        "hazard_item": TEST_ALERT_NUM,
        "full_text": "Test context",
        "analyzable": False,
        "interest_item": False
    }]
    
    df = pd.DataFrame(mock_records)
    validated_df = validate_schema(df)
    
    # Check all columns are present
    assert list(validated_df.columns) == UNIFIED_SCHEMA, \
        "Validated DataFrame should have all schema columns"
    
    # Check data types
    assert validated_df['analyzable'].dtype == bool, "analyzable should be boolean"
    assert validated_df['interest_item'].dtype == bool, "interest_item should be boolean"
    
    # Check date format
    assert validated_df['registration_date'].iloc[0] == TEST_DATE, \
        "Date should be in YYYY-MM-DD format"
    
    print("✓ Schema validation test passed")


def test_empty_dataframe():
    """Test that empty DataFrame handling works correctly."""
    from schema import get_empty_dataframe
    
    empty_df = get_empty_dataframe()
    
    # Check columns
    assert list(empty_df.columns) == UNIFIED_SCHEMA, \
        "Empty DataFrame should have all schema columns"
    
    # Check it's actually empty
    assert len(empty_df) == 0, "DataFrame should be empty"
    
    print("✓ Empty DataFrame test passed")


def test_fda_collector_mock_parsing():
    """Test FDA collector with mock HTML data."""
    collector = FDACollector()
    
    # Since we can't access the actual FDA website, we'll test the internal methods
    # with mock data structures
    
    # Test with mock BeautifulSoup element
    from bs4 import BeautifulSoup
    
    html = """
    <html>
        <body>
            <div class="center"><h4>China</h4></div>
            <p>Product: 03 R - Frozen Shrimp</p>
            <p>Date: 01/15/2024</p>
            <p>Description: Salmonella contamination detected</p>
        </body>
    </html>
    """
    
    soup = BeautifulSoup(html, 'html.parser')
    p_element = soup.find('p', string=lambda x: x and '01/15/2024' in x)
    
    if p_element:
        country = collector._find_nearest_country_header(p_element)
        assert country == "China", f"Expected 'China', got '{country}'"
    
    print("✓ FDA collector mock parsing test passed")


if __name__ == '__main__':
    print("Running FDA Precision Parsing Tests")
    print("=" * 60)
    
    try:
        test_fda_collector_initialization()
        test_fda_date_pattern_regex()
        test_extract_product_and_desc()
        test_normalize_country_name()
        test_schema_validation()
        test_empty_dataframe()
        test_fda_collector_mock_parsing()
        
        print("=" * 60)
        print("\n✓ All FDA precision parsing tests passed!\n")
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
