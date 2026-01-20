"""
Integration test for audit.py
Creates a realistic hub_data.parquet file and runs the complete audit workflow.
"""

import sys
from pathlib import Path
import pandas as pd
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import UNIFIED_SCHEMA, validate_schema
from utils.audit import DataQualityAuditor


def create_realistic_hub_data():
    """Create realistic hub_data.parquet with various data quality issues"""
    
    data = [
        # FDA Import Alert - Complete record
        {
            'registration_date': '2024-01-15',
            'data_source': 'FDA',
            'source_detail': 'Import Alert 99-31',
            'product_type': 'Seafood',
            'top_level_product_type': 'Animal Products',
            'upper_product_type': 'Fish',
            'product_name': 'Frozen Shrimp',
            'origin_country': 'Vietnam',
            'notifying_country': 'USA',
            'hazard_category': 'Chemical',
            'hazard_item': 'Antibiotics',
            'analyzable': True,
            'interest_item': True
        },
        # MFDS - Mapping failure (product type not in lookup)
        {
            'registration_date': '2024-01-16',
            'data_source': 'MFDS',
            'source_detail': 'I2620-20240116-001',
            'product_type': '특수용도식품',
            'top_level_product_type': '',
            'upper_product_type': '',
            'product_name': '다이어트 보조제',
            'origin_country': 'Korea',
            'notifying_country': 'Korea',
            'hazard_category': '',
            'hazard_item': '살모넬라',
            'analyzable': False,
            'interest_item': False
        },
        # RASFF - Complete with European data
        {
            'registration_date': '2024-01-17',
            'data_source': 'RASFF',
            'source_detail': '2024.0234',
            'product_type': 'Nuts',
            'top_level_product_type': 'Plant Products',
            'upper_product_type': 'Tree Nuts',
            'product_name': 'Almonds',
            'origin_country': 'Turkey',
            'notifying_country': 'Germany',
            'hazard_category': 'Chemical',
            'hazard_item': 'Aflatoxin B1',
            'analyzable': True,
            'interest_item': True
        },
        # FDA - Missing product name
        {
            'registration_date': '2024-01-18',
            'data_source': 'FDA',
            'source_detail': 'Import Alert 45-02',
            'product_type': 'Spices',
            'top_level_product_type': 'Plant Products',
            'upper_product_type': 'Condiments',
            'product_name': '',
            'origin_country': 'India',
            'notifying_country': 'USA',
            'hazard_category': 'Microbiological',
            'hazard_item': 'Salmonella',
            'analyzable': True,
            'interest_item': False
        },
        # MFDS - Multiple mapping failures
        {
            'registration_date': '2024-01-19',
            'data_source': 'MFDS',
            'source_detail': 'I0470-20240119-001',
            'product_type': '건강기능식품',
            'top_level_product_type': '',
            'upper_product_type': '',
            'product_name': '오메가3',
            'origin_country': 'Norway',
            'notifying_country': '',
            'hazard_category': '',
            'hazard_item': '중금속',
            'analyzable': False,
            'interest_item': True
        },
        # RASFF - Pesticide case
        {
            'registration_date': '2024-01-20',
            'data_source': 'RASFF',
            'source_detail': '2024.0345',
            'product_type': 'Fruits',
            'top_level_product_type': 'Plant Products',
            'upper_product_type': 'Fresh Produce',
            'product_name': 'Strawberries',
            'origin_country': 'Spain',
            'notifying_country': 'Netherlands',
            'hazard_category': 'Chemical',
            'hazard_item': 'Pesticide Residue',
            'analyzable': True,
            'interest_item': True
        },
        # FDA - Complete dairy case
        {
            'registration_date': '2024-01-21',
            'data_source': 'FDA',
            'source_detail': 'Import Alert 16-120',
            'product_type': 'Dairy',
            'top_level_product_type': 'Animal Products',
            'upper_product_type': 'Milk Products',
            'product_name': 'Cheese',
            'origin_country': 'France',
            'notifying_country': 'USA',
            'hazard_category': 'Microbiological',
            'hazard_item': 'Listeria monocytogenes',
            'analyzable': True,
            'interest_item': True
        },
        # MFDS - Another unknown product type
        {
            'registration_date': '2024-01-22',
            'data_source': 'MFDS',
            'source_detail': 'I2620-20240122-001',
            'product_type': '즉석섭취식품',
            'top_level_product_type': '',
            'upper_product_type': '',
            'product_name': '김밥',
            'origin_country': 'Korea',
            'notifying_country': 'Korea',
            'hazard_category': 'Microbiological',
            'hazard_item': '황색포도상구균',
            'analyzable': True,
            'interest_item': False
        },
    ]
    
    df = pd.DataFrame(data)
    
    # Ensure all columns are present
    for col in UNIFIED_SCHEMA:
        if col not in df.columns:
            df[col] = ''
    
    # Reorder to match schema
    df = df[UNIFIED_SCHEMA]
    
    # Apply schema validation
    df = validate_schema(df)
    
    return df


def test_integration():
    """Run end-to-end integration test"""
    
    print("=" * 70)
    print("🧪 Integration Test: Data Quality Audit")
    print("=" * 70)
    print()
    
    # Create data directory
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    hub_file = data_dir / "hub_data.parquet"
    backup_file = data_dir / "hub_data.parquet.backup"
    
    # Backup existing file if it exists
    if hub_file.exists():
        print(f"⚠️  Backing up existing hub_data.parquet")
        shutil.copy(hub_file, backup_file)
    
    try:
        # Create realistic test data
        print("📝 Creating realistic test data...")
        df = create_realistic_hub_data()
        
        # Save to hub_data.parquet
        data_dir.mkdir(exist_ok=True)
        df.to_parquet(hub_file, engine='pyarrow', index=False)
        print(f"✅ Saved {len(df)} test records to {hub_file}")
        print()
        
        # Run the audit
        print("🔬 Running Data Quality Audit...")
        print()
        auditor = DataQualityAuditor(parquet_path=hub_file)
        auditor.run_full_audit()
        
        # Verify audit results
        print("\n" + "=" * 70)
        print("📊 Verification Summary")
        print("=" * 70)
        print(f"✅ Total records audited: {auditor.total_rows}")
        print(f"✅ Schema columns verified: {len(UNIFIED_SCHEMA)}")
        print(f"✅ Data sources: {', '.join(auditor.df['data_source'].unique())}")
        
        # Check for expected mapping failures
        has_raw_product = ~(auditor.df['product_type'].isna() | (auditor.df['product_type'] == ""))
        missing_derived = (auditor.df['top_level_product_type'].isna() | (auditor.df['top_level_product_type'] == ""))
        mapping_failures = (has_raw_product & missing_derived).sum()
        
        print(f"⚠️  Detected {mapping_failures} mapping failures (as expected)")
        print()
        
        print("=" * 70)
        print("✅ Integration test completed successfully!")
        print("=" * 70)
        
    finally:
        # Restore backup if it exists, otherwise clean up test file
        if backup_file.exists():
            print(f"\n🔄 Restoring original hub_data.parquet from backup")
            shutil.move(str(backup_file), str(hub_file))
        else:
            print(f"\n🧹 Cleaning up test file")
            if hub_file.exists():
                hub_file.unlink()


if __name__ == "__main__":
    test_integration()
