"""
Basic tests for the food safety data collection system.
Tests for 16 Standard Headers schema validation, Parquet storage, and deduplication.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import validate_data, normalize_dataframe, UNIFIED_SCHEMA
from utils.deduplication import generate_unique_key, deduplicate_records, filter_duplicates
from utils.storage import save_to_parquet, load_parquet


def test_unique_key_generation():
    """Test unique key generation with new schema."""
    key1 = generate_unique_key("FDA", "2024.1000", "2024-05-20")
    key2 = generate_unique_key("FDA", "2024.1000", "2024-05-20")
    key3 = generate_unique_key("FDA", "2024.1001", "2024-05-20")
    
    assert key1 == key2, "Same inputs should generate same key"
    assert key1 != key3, "Different inputs should generate different keys"
    assert len(key1) == 16, "Key should be 16 characters"
    print("✓ Unique key generation test passed")


def test_schema_validation():
    """Test schema validation with 16 Standard Headers."""
    from datetime import datetime
    
    # Valid data with 16 standard headers
    valid_df = pd.DataFrame([{
        'id': 'test123',
        'ref_no': '2024.1000',
        'source': 'FDA',
        'date_registered': datetime.now(),
        'product_type_raw': 'Fishery products',
        'product_type': '수산물',
        'category': '식품',
        'product_name': 'Frozen Shrimp',
        'origin_raw': 'Vietnam',
        'origin': '베트남',
        'notifying_country_raw': 'Germany',
        'notifying_country': '독일',
        'hazard_reason': 'Salmonella detected',
        'analyzable': True,
        'hazard_category': '미생물',
        'tags': ['Shrimp', 'High Risk'],
    }])
    
    valid_df = normalize_dataframe(valid_df)
    is_valid, errors = validate_data(valid_df)
    assert is_valid, f"Valid schema should pass validation: {errors}"
    print("✓ Schema validation test passed")


def test_missing_required_fields():
    """Test that missing required fields are caught."""
    # Missing several required fields
    invalid_df = pd.DataFrame([{
        'id': 'test123',
        'source': 'FDA',
        'product_name': 'Test Product',
    }])
    
    is_valid, errors = validate_data(invalid_df)
    assert not is_valid, "Missing required fields should fail validation"
    assert len(errors) > 0, "Should have error messages"
    print("✓ Missing required fields test passed")


def test_deduplication():
    """Test deduplication logic with new schema."""
    from datetime import datetime
    
    # Create test data with duplicates
    df = pd.DataFrame([
        {
            'id': '',
            'ref_no': '2024.1000',
            'source': 'FDA',
            'date_registered': datetime(2024, 5, 20),
            'product_type_raw': 'Fishery products',
            'product_type': '수산물',
            'category': '식품',
            'product_name': 'Product 1',
            'origin_raw': 'Vietnam',
            'origin': '베트남',
            'notifying_country_raw': 'Germany',
            'notifying_country': '독일',
            'hazard_reason': 'Salmonella',
            'analyzable': True,
            'hazard_category': '미생물',
            'tags': [],
        },
        {
            'id': '',
            'ref_no': '2024.1000',  # Duplicate
            'source': 'FDA',
            'date_registered': datetime(2024, 5, 20),
            'product_type_raw': 'Fishery products',
            'product_type': '수산물',
            'category': '식품',
            'product_name': 'Product 1',
            'origin_raw': 'Vietnam',
            'origin': '베트남',
            'notifying_country_raw': 'Germany',
            'notifying_country': '독일',
            'hazard_reason': 'Salmonella',
            'analyzable': True,
            'hazard_category': '미생물',
            'tags': [],
        },
        {
            'id': '',
            'ref_no': '2024.1001',  # Different
            'source': 'FDA',
            'date_registered': datetime(2024, 5, 21),
            'product_type_raw': 'Meat products',
            'product_type': '육류',
            'category': '식품',
            'product_name': 'Product 2',
            'origin_raw': 'Brazil',
            'origin': '브라질',
            'notifying_country_raw': 'USA',
            'notifying_country': '미국',
            'hazard_reason': 'E. coli',
            'analyzable': True,
            'hazard_category': '미생물',
            'tags': [],
        },
    ])
    
    df_dedup = deduplicate_records(df)
    assert len(df_dedup) == 2, f"Should have 2 unique records, got {len(df_dedup)}"
    assert 'id' in df_dedup.columns, "Should have id column"
    assert all(df_dedup['id'] != ''), "All IDs should be populated"
    print("✓ Deduplication test passed")


def test_parquet_save_load():
    """Test save and load operations with Parquet."""
    from datetime import datetime
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    try:
        parquet_path = str(Path(temp_dir) / 'hub_data.parquet')
        
        # Create test data
        df1 = pd.DataFrame([{
            'id': 'test123',
            'ref_no': '2024.1000',
            'source': 'FDA',
            'date_registered': datetime(2024, 5, 20),
            'product_type_raw': 'Fishery products',
            'product_type': '수산물',
            'category': '식품',
            'product_name': 'Frozen Shrimp',
            'origin_raw': 'Vietnam',
            'origin': '베트남',
            'notifying_country_raw': 'Germany',
            'notifying_country': '독일',
            'hazard_reason': 'Salmonella detected',
            'analyzable': True,
            'hazard_category': '미생물',
            'tags': ['Shrimp', 'High Risk'],
        }])
        
        df1 = normalize_dataframe(df1)
        
        # Save to Parquet
        save_to_parquet(df1, parquet_path)
        
        # Load from Parquet
        df_loaded = load_parquet(parquet_path)
        
        assert len(df_loaded) == 1, "Should load 1 record"
        assert df_loaded['product_name'].iloc[0] == 'Frozen Shrimp', "Product name should match"
        
        # Test append functionality
        df2 = pd.DataFrame([{
            'id': 'test456',
            'ref_no': '2024.1001',
            'source': 'RASFF',
            'date_registered': datetime(2024, 5, 21),
            'product_type_raw': 'Dairy products',
            'product_type': '유제품',
            'category': '식품',
            'product_name': 'Cheese',
            'origin_raw': 'France',
            'origin': '프랑스',
            'notifying_country_raw': 'Italy',
            'notifying_country': '이탈리아',
            'hazard_reason': 'Listeria',
            'analyzable': True,
            'hazard_category': '미생물',
            'tags': ['Dairy'],
        }])
        
        df2 = normalize_dataframe(df2)
        
        # Append to existing file
        save_to_parquet(df2, parquet_path)
        
        # Load again
        df_combined = load_parquet(parquet_path)
        
        assert len(df_combined) == 2, f"Should have 2 records after append, got {len(df_combined)}"
        assert 'Frozen Shrimp' in df_combined['product_name'].values, "Should contain first record"
        assert 'Cheese' in df_combined['product_name'].values, "Should contain second record"
        
        print("✓ Parquet save/load test passed")
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


def test_complete_pipeline():
    """Test complete data validation, deduplication, save, and reload pipeline."""
    from datetime import datetime
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    try:
        parquet_path = str(Path(temp_dir) / 'hub_data.parquet')
        
        # Create dummy data
        df = pd.DataFrame([
            {
                'id': '',
                'ref_no': '2024.1000',
                'source': 'FDA',
                'date_registered': datetime(2024, 5, 20),
                'product_type_raw': 'Fishery products',
                'product_type': '수산물',
                'category': '식품',
                'product_name': 'Frozen Shrimp',
                'origin_raw': 'Vietnam',
                'origin': '베트남',
                'notifying_country_raw': 'Germany',
                'notifying_country': '독일',
                'hazard_reason': 'Salmonella detected',
                'analyzable': True,
                'hazard_category': '미생물',
                'tags': ['Shrimp', 'High Risk'],
            },
            {
                'id': '',
                'ref_no': '2024.1001',
                'source': 'RASFF',
                'date_registered': datetime(2024, 5, 21),
                'product_type_raw': 'Meat products',
                'product_type': '육류',
                'category': '식품',
                'product_name': 'Beef',
                'origin_raw': 'Brazil',
                'origin': '브라질',
                'notifying_country_raw': 'Spain',
                'notifying_country': '스페인',
                'hazard_reason': 'E. coli O157:H7',
                'analyzable': True,
                'hazard_category': '미생물',
                'tags': ['Beef', 'Critical'],
            },
        ])
        
        # Normalize
        df = normalize_dataframe(df)
        
        # Validate
        is_valid, errors = validate_data(df)
        assert is_valid, f"Data should be valid: {errors}"
        
        # Deduplicate
        df = deduplicate_records(df)
        assert len(df) == 2, "Should have 2 unique records"
        
        # Save to Parquet
        save_to_parquet(df, parquet_path)
        
        # Reload
        df_loaded = load_parquet(parquet_path)
        assert len(df_loaded) == 2, "Should reload 2 records"
        
        # Validate reloaded data
        is_valid, errors = validate_data(df_loaded)
        assert is_valid, f"Reloaded data should be valid: {errors}"
        
        print("✓ Complete pipeline test passed")
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


if __name__ == '__main__':
    print("Running Food Safety Intelligence System Tests")
    print("Testing 16 Standard Headers Schema Implementation")
    print("=" * 60)
    
    try:
        test_unique_key_generation()
        test_schema_validation()
        test_missing_required_fields()
        test_deduplication()
        test_parquet_save_load()
        test_complete_pipeline()
        
        print("=" * 60)
        print("\n✓ All tests passed!\n")
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
