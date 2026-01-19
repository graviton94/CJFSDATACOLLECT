"""
Basic tests for the food safety data collection system.
"""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import validate_schema, normalize_dataframe
from utils.deduplication import generate_unique_key, deduplicate_records


def test_unique_key_generation():
    """Test unique key generation."""
    key1 = generate_unique_key("EU_RASFF", "2024.1000")
    key2 = generate_unique_key("EU_RASFF", "2024.1000")
    key3 = generate_unique_key("EU_RASFF", "2024.1001")
    
    assert key1 == key2, "Same inputs should generate same key"
    assert key1 != key3, "Different inputs should generate different keys"
    assert len(key1) == 16, "Key should be 16 characters"
    print("✓ Unique key generation test passed")


def test_schema_validation():
    """Test schema validation."""
    from datetime import datetime
    
    # Valid data
    valid_df = pd.DataFrame([{
        'record_id': 'test123',
        'source': 'EU_RASFF',
        'source_reference': '2024.1000',
        'notification_date': datetime.now(),
        'ingestion_date': datetime.now(),
        'product_name': 'Test Product',
        'product_category': 'Test Category',
        'origin_country': 'Test Country',
        'destination_country': 'Test Destination',
        'hazard_category': 'Test Hazard',
        'hazard_substance': 'Test Substance',
        'risk_decision': 'alert',
        'risk_level': 'serious',
        'action_taken': 'Test Action',
        'description': 'Test Description',
        'data_quality_score': 0.95,
    }])
    
    valid_df = normalize_dataframe(valid_df)
    is_valid, errors = validate_schema(valid_df)
    assert is_valid, f"Valid schema should pass validation: {errors}"
    print("✓ Schema validation test passed")


def test_deduplication():
    """Test deduplication logic."""
    from datetime import datetime
    
    # Create test data with duplicates
    df = pd.DataFrame([
        {
            'source': 'EU_RASFF',
            'source_reference': '2024.1000',
            'notification_date': datetime.now(),
            'ingestion_date': datetime.now(),
            'product_name': 'Product 1',
            'origin_country': 'Country A',
            'hazard_category': 'Hazard 1',
            'risk_decision': 'alert',
        },
        {
            'source': 'EU_RASFF',
            'source_reference': '2024.1000',  # Duplicate
            'notification_date': datetime.now(),
            'ingestion_date': datetime.now(),
            'product_name': 'Product 1',
            'origin_country': 'Country A',
            'hazard_category': 'Hazard 1',
            'risk_decision': 'alert',
        },
        {
            'source': 'EU_RASFF',
            'source_reference': '2024.1001',  # Different
            'notification_date': datetime.now(),
            'ingestion_date': datetime.now(),
            'product_name': 'Product 2',
            'origin_country': 'Country B',
            'hazard_category': 'Hazard 2',
            'risk_decision': 'recall',
        },
    ])
    
    df_dedup = deduplicate_records(df)
    assert len(df_dedup) == 2, f"Should have 2 unique records, got {len(df_dedup)}"
    assert 'record_id' in df_dedup.columns, "Should have record_id column"
    print("✓ Deduplication test passed")


def test_data_pipeline():
    """Test that actual data exists and is valid."""
    data_dir = Path('data/processed')
    
    if not data_dir.exists() or not list(data_dir.glob('*.parquet')):
        print("⚠ No data files found - run collection first")
        return
    
    # Load all data
    dfs = []
    for parquet_file in data_dir.glob('*.parquet'):
        df = pd.read_parquet(parquet_file)
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Validate
    is_valid, errors = validate_schema(combined_df)
    assert is_valid, f"Combined data should be valid: {errors}"
    
    # Check for required sources
    sources = set(combined_df['source'].unique())
    assert 'EU_RASFF' in sources or 'FDA_IMPORT_ALERTS' in sources or 'KR_MFDS' in sources, \
        "Should have at least one data source"
    
    print(f"✓ Data pipeline test passed - {len(combined_df)} total records from {len(sources)} sources")


if __name__ == '__main__':
    print("Running Food Safety Intelligence System Tests\n")
    print("=" * 60)
    
    try:
        test_unique_key_generation()
        test_schema_validation()
        test_deduplication()
        test_data_pipeline()
        
        print("=" * 60)
        print("\n✓ All tests passed!\n")
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n")
        sys.exit(1)
