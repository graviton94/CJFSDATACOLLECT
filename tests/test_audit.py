"""
Test script to verify audit.py functionality with sample data.
Creates a temporary hub_data.parquet file and runs the audit.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import UNIFIED_SCHEMA
from utils.audit import DataQualityAuditor


def create_sample_data():
    """Create sample data with intentional issues for testing"""
    
    sample_data = [
        # Perfect record
        {
            'registration_date': '2024-01-01',
            'data_source': 'FDA',
            'source_detail': 'Import Alert 99-01',
            'product_type': 'Snack Food',
            'top_level_product_type': 'Processed Food',
            'upper_product_type': 'Snack',
            'product_name': 'Potato Chips',
            'origin_country': 'USA',
            'notifying_country': 'Germany',
            'hazard_category': 'Chemical',
            'hazard_item': 'Aflatoxin B1',
            'analyzable': True,
            'interest_item': False
        },
        # Missing derived data (mapping failure)
        {
            'registration_date': '2024-01-02',
            'data_source': 'MFDS',
            'source_detail': 'I2620',
            'product_type': 'Fresh Fish',  # Has raw data
            'top_level_product_type': '',  # Missing derived data
            'upper_product_type': '',       # Missing derived data
            'product_name': 'Salmon Fillet',
            'origin_country': 'Norway',
            'notifying_country': 'Korea',
            'hazard_category': '',          # Missing derived data
            'hazard_item': 'Mercury',       # Has raw data
            'analyzable': False,
            'interest_item': True
        },
        # Partially missing data
        {
            'registration_date': '2024-01-03',
            'data_source': 'RASFF',
            'source_detail': '2024.0123',
            'product_type': 'Beverages',
            'top_level_product_type': 'Drinks',
            'upper_product_type': 'Soft Drinks',
            'product_name': '',  # Missing product name
            'origin_country': 'China',
            'notifying_country': '',  # Missing notifying country
            'hazard_category': 'Microbiological',
            'hazard_item': 'E. coli',
            'analyzable': True,
            'interest_item': False
        },
        # Another mapping failure
        {
            'registration_date': '2024-01-04',
            'data_source': 'FDA',
            'source_detail': 'Import Alert 16-120',
            'product_type': 'Spices',
            'top_level_product_type': '',  # Mapping failure
            'upper_product_type': '',
            'product_name': 'Black Pepper',
            'origin_country': 'India',
            'notifying_country': 'USA',
            'hazard_category': 'Chemical',
            'hazard_item': 'Salmonella',
            'analyzable': True,
            'interest_item': True
        },
    ]
    
    df = pd.DataFrame(sample_data)
    
    # Ensure all columns from UNIFIED_SCHEMA are present
    for col in UNIFIED_SCHEMA:
        if col not in df.columns:
            df[col] = ''
    
    # Reorder columns to match UNIFIED_SCHEMA
    df = df[UNIFIED_SCHEMA]
    
    return df


def test_audit_with_sample_data():
    """Test the auditor with sample data"""
    
    print("🧪 Testing Data Quality Auditor with Sample Data")
    print("=" * 60)
    print()
    
    # Create temporary directory and file
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir) / "test_hub_data.parquet"
        
        # Generate and save sample data
        df = create_sample_data()
        df.to_parquet(temp_path, engine='pyarrow', index=False)
        
        print(f"✅ Created sample data: {len(df)} rows")
        print(f"📁 Temporary file: {temp_path}\n")
        
        # Run audit
        auditor = DataQualityAuditor(parquet_path=temp_path)
        auditor.run_full_audit()
        
        print("\n✅ Test completed successfully!")


if __name__ == "__main__":
    test_audit_with_sample_data()
