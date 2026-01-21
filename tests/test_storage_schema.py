"""
Test storage.py schema enforcement for UNIFIED_SCHEMA.
Validates that obsolete columns are properly filtered out.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import UNIFIED_SCHEMA, validate_schema
from utils.storage import save_to_parquet, load_all_data, save_to_hub


def test_storage_filters_obsolete_columns():
    """Test that storage functions filter out obsolete columns not in UNIFIED_SCHEMA."""
    
    # Create temporary directory for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)
        
        # Create test data with both valid and obsolete columns
        test_data = pd.DataFrame([
            {
                'registration_date': '2024-01-01',
                'data_source': 'FDA',
                'source_detail': 'TEST-001',
                'product_type': 'Test Product',
                'top_level_product_type': 'Food',
                'upper_product_type': 'Processed',
                'product_name': 'Test Item',
                'origin_country': 'USA',
                'notifying_country': 'Germany',
                'hazard_category': 'Chemical',
                'hazard_item': 'Test Hazard',
                'full_text': '',
                'analyzable': True,
                'interest_item': False,
                # Obsolete columns that should be filtered out
                'id': 'old-id-123',
                'ref_no': 'old-ref-456',
                'source': 'OLD_SOURCE',
                'date_registered': '2024-01-01'
            }
        ])
        
        # Save using save_to_parquet
        count = save_to_parquet(test_data, data_dir)
        assert count == 1, "Should save 1 record"
        
        # Load data back
        loaded_data = load_all_data(data_dir)
        
        # Verify only UNIFIED_SCHEMA columns are present
        assert set(loaded_data.columns) == set(UNIFIED_SCHEMA), \
            f"Loaded data should only have UNIFIED_SCHEMA columns. Got: {loaded_data.columns.tolist()}"
        
        # Verify obsolete columns are not present
        obsolete_cols = ['id', 'ref_no', 'source', 'date_registered']
        for col in obsolete_cols:
            assert col not in loaded_data.columns, f"Obsolete column '{col}' should not be in loaded data"
        
        print("✓ Storage filters obsolete columns test passed")


def test_save_to_hub_enforces_schema():
    """Test that save_to_hub enforces UNIFIED_SCHEMA."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)
        
        # Create test data with obsolete columns
        test_data = pd.DataFrame([
            {
                'registration_date': '2024-01-01',
                'data_source': 'MFDS',
                'source_detail': 'I2620',
                'product_type': 'Fish',
                'top_level_product_type': '',
                'upper_product_type': '',
                'product_name': 'Salmon',
                'origin_country': 'Norway',
                'notifying_country': 'Korea',
                'hazard_category': 'Biological',
                'hazard_item': 'Bacteria',
                'full_text': '',
                'analyzable': True,
                'interest_item': True,
                # Obsolete columns
                'id': 'obsolete-id',
                'ref_no': 'obsolete-ref'
            }
        ])
        
        # Save using save_to_hub
        count = save_to_hub(test_data, data_dir)
        assert count == 1, "Should save 1 new record"
        
        # Load and verify
        hub_file = data_dir / 'hub_data.parquet'
        assert hub_file.exists(), "Hub file should exist"
        
        loaded = pd.read_parquet(hub_file)
        
        # Should only have UNIFIED_SCHEMA columns
        for col in UNIFIED_SCHEMA:
            assert col in loaded.columns, f"Column '{col}' should be in loaded data"
        
        # Should not have obsolete columns
        assert 'id' not in loaded.columns, "Obsolete 'id' column should be filtered out"
        assert 'ref_no' not in loaded.columns, "Obsolete 'ref_no' column should be filtered out"
        
        print("✓ save_to_hub enforces schema test passed")


if __name__ == '__main__':
    test_storage_filters_obsolete_columns()
    test_save_to_hub_enforces_schema()
    print("\n✅ All storage schema tests passed!")
