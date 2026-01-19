"""
Tests for MFDS and FDA collectors.
Validates that collectors produce correct 16-column schema output.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from collectors.mfds_collector import MFDSCollector
from collectors.fda_collector import FDACollector
from collectors.rasff_scraper import RASFFScraper
from schema import validate_data, UNIFIED_SCHEMA


def test_mfds_collector_schema():
    """Test that MFDS collector produces valid 16-column schema."""
    collector = MFDSCollector()
    
    # Collect mock data
    df = collector.collect(days_back=2)
    assert len(df) > 0, "Should collect at least one record"
    
    # Transform to schema
    df_transformed = collector.transform_to_schema(df)
    
    # Validate has all 16 columns
    assert list(df_transformed.columns) == list(UNIFIED_SCHEMA.keys()), \
        "Should have exactly 16 standard columns"
    
    # Validate schema
    is_valid, errors = validate_data(df_transformed)
    assert is_valid, f"Schema validation should pass: {errors}"
    
    # Check source is correct
    assert all(df_transformed['source'] == 'MFDS'), "Source should be MFDS"
    
    print("✓ MFDS collector schema test passed")


def test_fda_collector_schema():
    """Test that FDA collector produces valid 16-column schema."""
    collector = FDACollector(use_enforcement_api=False)  # Use mock data
    
    # Collect mock data
    df = collector.collect(days_back=2)
    assert len(df) > 0, "Should collect at least one record"
    
    # Transform to schema
    df_transformed = collector.transform_to_schema(df)
    
    # Validate has all 16 columns
    assert list(df_transformed.columns) == list(UNIFIED_SCHEMA.keys()), \
        "Should have exactly 16 standard columns"
    
    # Validate schema
    is_valid, errors = validate_data(df_transformed)
    assert is_valid, f"Schema validation should pass: {errors}"
    
    # Check source is correct
    assert all(df_transformed['source'] == 'FDA'), "Source should be FDA"
    
    print("✓ FDA collector schema test passed")


def test_mfds_collect_and_store():
    """Test MFDS collector end-to-end pipeline."""
    temp_dir = tempfile.mkdtemp()
    try:
        data_dir = Path(temp_dir)
        collector = MFDSCollector(data_dir=data_dir)
        
        # Collect and store
        count = collector.collect_and_store(days_back=3)
        assert count > 0, "Should store at least one record"
        
        # Verify file exists
        parquet_file = data_dir / 'hub_data.parquet'
        assert parquet_file.exists(), "Parquet file should exist"
        
        # Load and validate
        df = pd.read_parquet(parquet_file)
        assert len(df) == count, f"Should have {count} records"
        
        is_valid, errors = validate_data(df)
        assert is_valid, f"Stored data should be valid: {errors}"
        
        print("✓ MFDS collect_and_store test passed")
    finally:
        shutil.rmtree(temp_dir)


def test_fda_collect_and_store():
    """Test FDA collector end-to-end pipeline."""
    temp_dir = tempfile.mkdtemp()
    try:
        data_dir = Path(temp_dir)
        collector = FDACollector(data_dir=data_dir, use_enforcement_api=False)
        
        # Collect and store
        count = collector.collect_and_store(days_back=3)
        assert count > 0, "Should store at least one record"
        
        # Verify file exists
        parquet_file = data_dir / 'hub_data.parquet'
        assert parquet_file.exists(), "Parquet file should exist"
        
        # Load and validate
        df = pd.read_parquet(parquet_file)
        assert len(df) == count, f"Should have {count} records"
        
        is_valid, errors = validate_data(df)
        assert is_valid, f"Stored data should be valid: {errors}"
        
        print("✓ FDA collect_and_store test passed")
    finally:
        shutil.rmtree(temp_dir)


def test_combined_collectors():
    """Test that both collectors can write to the same database."""
    temp_dir = tempfile.mkdtemp()
    try:
        data_dir = Path(temp_dir)
        
        # Collect from MFDS
        mfds_collector = MFDSCollector(data_dir=data_dir)
        mfds_count = mfds_collector.collect_and_store(days_back=2)
        
        # Collect from FDA
        fda_collector = FDACollector(data_dir=data_dir, use_enforcement_api=False)
        fda_count = fda_collector.collect_and_store(days_back=2)
        
        # Load combined data
        parquet_file = data_dir / 'hub_data.parquet'
        df = pd.read_parquet(parquet_file)
        
        # Verify total count
        expected_total = mfds_count + fda_count
        assert len(df) == expected_total, \
            f"Should have {expected_total} total records"
        
        # Verify sources
        sources = df['source'].value_counts().to_dict()
        assert sources.get('MFDS', 0) == mfds_count, \
            f"Should have {mfds_count} MFDS records"
        assert sources.get('FDA', 0) == fda_count, \
            f"Should have {fda_count} FDA records"
        
        # Validate combined data
        is_valid, errors = validate_data(df)
        assert is_valid, f"Combined data should be valid: {errors}"
        
        print("✓ Combined collectors test passed")
    finally:
        shutil.rmtree(temp_dir)


def test_rasff_scraper_schema():
    """Test that RASFF scraper produces valid 16-column schema."""
    scraper = RASFFScraper()
    
    # Scrape mock data (will use mock since we can't access the actual portal)
    df = scraper.scrape(days_back=2)
    assert len(df) > 0, "Should collect at least one record"
    
    # Transform to schema
    df_transformed = scraper.transform_to_schema(df)
    
    # Validate has all 16 columns
    assert list(df_transformed.columns) == list(UNIFIED_SCHEMA.keys()), \
        "Should have exactly 16 standard columns"
    
    # Validate schema
    is_valid, errors = validate_data(df_transformed)
    assert is_valid, f"Schema validation should pass: {errors}"
    
    # Check source is correct
    assert all(df_transformed['source'] == 'RASFF'), "Source should be RASFF"
    
    print("✓ RASFF scraper schema test passed")


def test_rasff_collect_and_store():
    """Test RASFF scraper end-to-end pipeline."""
    temp_dir = tempfile.mkdtemp()
    try:
        data_dir = Path(temp_dir)
        scraper = RASFFScraper(data_dir=data_dir)
        
        # Collect and store
        count = scraper.collect_and_store(days_back=3)
        assert count > 0, "Should store at least one record"
        
        # Verify file exists
        parquet_file = data_dir / 'hub_data.parquet'
        assert parquet_file.exists(), "Parquet file should exist"
        
        # Load and validate
        df = pd.read_parquet(parquet_file)
        assert len(df) == count, f"Should have {count} records"
        
        is_valid, errors = validate_data(df)
        assert is_valid, f"Stored data should be valid: {errors}"
        
        print("✓ RASFF collect_and_store test passed")
    finally:
        shutil.rmtree(temp_dir)


if __name__ == '__main__':
    print("Running Collector Tests")
    print("=" * 60)
    
    try:
        test_mfds_collector_schema()
        test_fda_collector_schema()
        test_rasff_scraper_schema()
        test_mfds_collect_and_store()
        test_fda_collect_and_store()
        test_rasff_collect_and_store()
        test_combined_collectors()
        
        print("=" * 60)
        print("\n✓ All collector tests passed!\n")
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
