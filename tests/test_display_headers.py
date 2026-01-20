"""
Test that DISPLAY_HEADERS mapping is correctly applied in the dashboard.
"""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import UNIFIED_SCHEMA, DISPLAY_HEADERS


def test_display_headers_complete():
    """Test that all UNIFIED_SCHEMA columns have display headers."""
    
    for col in UNIFIED_SCHEMA:
        assert col in DISPLAY_HEADERS, f"Column '{col}' missing from DISPLAY_HEADERS"
    
    print("✓ All UNIFIED_SCHEMA columns have display headers")


def test_display_headers_mapping():
    """Test that display headers can be applied to a dataframe."""
    
    # Create test dataframe with UNIFIED_SCHEMA columns
    test_df = pd.DataFrame({
        'registration_date': ['2024-01-01'],
        'data_source': ['FDA'],
        'source_detail': ['TEST-001'],
        'product_type': ['Test'],
        'top_level_product_type': ['Food'],
        'upper_product_type': ['Processed'],
        'product_name': ['Test Product'],
        'origin_country': ['USA'],
        'notifying_country': ['Germany'],
        'hazard_category': ['Chemical'],
        'hazard_item': ['Test Hazard'],
        'analyzable': [True],
        'interest_item': [False]
    })
    
    # Apply display headers
    display_df = test_df.rename(columns=DISPLAY_HEADERS)
    
    # Verify Korean headers are present
    expected_headers = [
        '등록일자', '데이터소스', '상세출처', '품목유형', 
        '최상위품목유형', '상위품목유형', '제품명', '원산지',
        '통보국', '분류(카테고리)', '시험항목', '분석가능여부', '관심항목'
    ]
    
    for header in expected_headers:
        assert header in display_df.columns, f"Korean header '{header}' not found in display dataframe"
    
    print("✓ Display headers mapping test passed")


def test_reverse_mapping():
    """Test that we can reverse the mapping for saving."""
    
    # Create reverse mapping
    reverse_headers = {v: k for k, v in DISPLAY_HEADERS.items()}
    
    # Verify all Korean headers can be reversed
    for english, korean in DISPLAY_HEADERS.items():
        assert reverse_headers[korean] == english, \
            f"Reverse mapping failed for {english} -> {korean}"
    
    print("✓ Reverse mapping test passed")


if __name__ == '__main__':
    test_display_headers_complete()
    test_display_headers_mapping()
    test_reverse_mapping()
    print("\n✅ All display header tests passed!")
