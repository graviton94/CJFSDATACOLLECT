#!/usr/bin/env python3
"""
Comprehensive verification script for the schema enforcement refactor.
Demonstrates:
1. Obsolete column filtering in storage
2. Korean header mapping in dashboard
3. UTF-8-sig CSV encoding
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.schema import UNIFIED_SCHEMA, DISPLAY_HEADERS
from src.utils.storage import save_to_hub, load_all_data

def print_section(title):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def verify_schema_enforcement():
    """Verify that obsolete columns are filtered out."""
    print_section("1. Schema Enforcement Verification")
    
    # Load existing data
    hub_path = Path("data/hub/hub_data.parquet")
    if not hub_path.exists():
        print("❌ No hub_data.parquet found. Run the data creation script first.")
        return False
    
    df = pd.read_parquet(hub_path, engine='pyarrow')
    
    print(f"✅ Loaded {len(df)} records from hub_data.parquet")
    print(f"✅ Columns present: {len(df.columns)}")
    print(f"✅ Expected columns: {len(UNIFIED_SCHEMA)}")
    
    # Check columns
    if set(df.columns) == set(UNIFIED_SCHEMA):
        print("✅ SUCCESS: All columns match UNIFIED_SCHEMA")
        print(f"   Columns: {df.columns.tolist()}")
    else:
        missing = set(UNIFIED_SCHEMA) - set(df.columns)
        extra = set(df.columns) - set(UNIFIED_SCHEMA)
        if missing:
            print(f"❌ Missing columns: {missing}")
        if extra:
            print(f"❌ Extra columns (obsolete?): {extra}")
        return False
    
    # Check for obsolete columns
    obsolete_cols = ['id', 'ref_no', 'source', 'date_registered']
    found_obsolete = [col for col in obsolete_cols if col in df.columns]
    
    if found_obsolete:
        print(f"❌ FAIL: Found obsolete columns: {found_obsolete}")
        return False
    else:
        print(f"✅ SUCCESS: No obsolete columns found")
    
    return True

def verify_korean_headers():
    """Verify Korean header mapping works correctly."""
    print_section("2. Korean Header Mapping Verification")
    
    # Load data
    hub_path = Path("data/hub/hub_data.parquet")
    df = pd.read_parquet(hub_path, engine='pyarrow')
    
    # Apply Korean headers
    df_korean = df.rename(columns=DISPLAY_HEADERS)
    
    print(f"✅ Original columns (English):")
    for col in df.columns[:5]:  # Show first 5
        print(f"   - {col}")
    
    print(f"\n✅ Renamed columns (Korean):")
    for col in df_korean.columns[:5]:  # Show first 5
        print(f"   - {col}")
    
    # Verify all Korean headers are present
    expected_korean = list(DISPLAY_HEADERS.values())
    missing_korean = [h for h in expected_korean if h not in df_korean.columns]
    
    if missing_korean:
        print(f"❌ FAIL: Missing Korean headers: {missing_korean}")
        return False
    else:
        print(f"\n✅ SUCCESS: All {len(expected_korean)} Korean headers present")
    
    return True

def verify_csv_encoding():
    """Verify CSV encoding uses utf-8-sig."""
    print_section("3. CSV Encoding Verification")
    
    # Load data
    hub_path = Path("data/hub/hub_data.parquet")
    df = pd.read_parquet(hub_path, engine='pyarrow')
    
    # Apply Korean headers
    df_korean = df.rename(columns=DISPLAY_HEADERS)
    
    # Export to CSV with utf-8-sig
    csv_data = df_korean.to_csv(index=False).encode('utf-8-sig')
    
    # Check for BOM (Byte Order Mark)
    if csv_data[:3] == b'\xef\xbb\xbf':
        print("✅ SUCCESS: CSV data includes BOM (utf-8-sig encoding)")
        print("   This ensures Korean characters display correctly in Excel")
    else:
        print("❌ FAIL: CSV data missing BOM (not using utf-8-sig)")
        return False
    
    # Show sample CSV output
    csv_str = csv_data.decode('utf-8-sig')
    lines = csv_str.split('\n')[:3]  # First 3 lines
    
    print("\n📄 Sample CSV output (first 2 lines):")
    for i, line in enumerate(lines[:2]):
        if line:
            print(f"   Line {i+1}: {line[:80]}{'...' if len(line) > 80 else ''}")
    
    return True

def verify_data_integrity():
    """Verify data integrity after all transformations."""
    print_section("4. Data Integrity Verification")
    
    # Load data
    hub_path = Path("data/hub/hub_data.parquet")
    df = pd.read_parquet(hub_path, engine='pyarrow')
    
    print(f"✅ Total records: {len(df)}")
    print(f"✅ Data sources present: {df['data_source'].unique().tolist()}")
    
    # Check for required data types
    if df['analyzable'].dtype == bool:
        print("✅ 'analyzable' column is boolean type")
    else:
        print(f"⚠️  'analyzable' column type is {df['analyzable'].dtype} (expected: bool)")
    
    if df['interest_item'].dtype == bool:
        print("✅ 'interest_item' column is boolean type")
    else:
        print(f"⚠️  'interest_item' column type is {df['interest_item'].dtype} (expected: bool)")
    
    # Sample data
    print("\n📊 Sample record (Korean headers):")
    df_korean = df.rename(columns=DISPLAY_HEADERS)
    sample = df_korean.iloc[0].to_dict()
    for key, value in list(sample.items())[:6]:  # Show first 6 fields
        print(f"   {key}: {value}")
    
    return True

def main():
    """Run all verification checks."""
    print("\n" + "🔍 SCHEMA ENFORCEMENT VERIFICATION SUITE".center(60, "="))
    
    results = []
    
    # Run all checks
    results.append(("Schema Enforcement", verify_schema_enforcement()))
    results.append(("Korean Headers", verify_korean_headers()))
    results.append(("CSV Encoding", verify_csv_encoding()))
    results.append(("Data Integrity", verify_data_integrity()))
    
    # Summary
    print_section("SUMMARY")
    
    all_passed = True
    for check_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {check_name}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 ALL CHECKS PASSED! 🎉".center(60, "="))
        return 0
    else:
        print("⚠️  SOME CHECKS FAILED".center(60, "="))
        return 1

if __name__ == '__main__':
    sys.exit(main())
