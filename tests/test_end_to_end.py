"""
End-to-end verification script for schema enforcement and lookup logic.
Tests the complete data flow from collection to storage to UI display.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile

# Add project root and src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

from src.schema import UNIFIED_SCHEMA, DISPLAY_HEADERS, validate_schema
from src.utils.storage import save_to_hub, load_all_data


def test_end_to_end_schema_enforcement():
    """
    Test complete pipeline:
    1. Create data with both valid and obsolete columns
    2. Validate schema enforcement
    3. Save to hub
    4. Load back
    5. Verify only UNIFIED_SCHEMA columns exist
    6. Apply display headers
    7. Verify CSV encoding
    """
    
    print("=" * 70)
    print("End-to-End Schema Enforcement & UI Verification Test")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)
        
        # Step 1: Create mock data with obsolete columns
        print("\n1️⃣ Creating test data with obsolete columns...")
        test_data = pd.DataFrame([
            {
                # Valid UNIFIED_SCHEMA columns
                'registration_date': '2024-01-15',
                'data_source': 'MFDS',
                'source_detail': 'I2620-12345',
                'product_type': '냉이',
                'top_level_product_type': '농산물',
                'upper_product_type': '채소류',
                'product_name': '냉이 신선제품',
                'origin_country': 'South Korea',
                'notifying_country': 'South Korea',
                'hazard_category': '잔류농약',
                'hazard_item': '펜디메탈린',
                'analyzable': True,
                'interest_item': False,
                
                # Obsolete columns (should be filtered out)
                'id': 'obsolete-id-123',
                'ref_no': 'old-ref-no',
                'source': 'OLD_SOURCE',
                'date_registered': '2024-01-15',
                'tags': 'old-tag',
                'legacy_field': 'should-be-removed'
            },
            {
                # Second record
                'registration_date': '2024-01-16',
                'data_source': 'FDA',
                'source_detail': 'IA-99-01',
                'product_type': 'Shrimp',
                'top_level_product_type': 'Seafood',
                'upper_product_type': 'Crustacean',
                'product_name': 'Frozen Shrimp',
                'origin_country': 'Vietnam',
                'notifying_country': 'USA',
                'hazard_category': 'Antibiotic',
                'hazard_item': 'Chloramphenicol',
                'analyzable': True,
                'interest_item': True,
                
                # More obsolete columns
                'id': 'another-obsolete-id',
                'ref_no': 'FDA-OLD-REF'
            }
        ])
        
        print(f"   Original columns: {test_data.columns.tolist()}")
        print(f"   Record count: {len(test_data)}")
        
        # Step 2: Validate schema
        print("\n2️⃣ Validating schema...")
        validated_df = validate_schema(test_data)
        print(f"   After validation columns: {validated_df.columns.tolist()}")
        
        obsolete_cols = set(test_data.columns) - set(UNIFIED_SCHEMA)
        if obsolete_cols:
            assert all(col not in validated_df.columns for col in obsolete_cols), \
                "Obsolete columns should be removed after validation"
            print(f"   ✓ Removed obsolete columns: {obsolete_cols}")
        
        # Step 3: Save to hub
        print("\n3️⃣ Saving to hub with deduplication...")
        count = save_to_hub(validated_df, data_dir)
        print(f"   ✓ Saved {count} records to hub")
        
        # Step 4: Load from hub
        print("\n4️⃣ Loading from hub...")
        loaded_df = load_all_data(data_dir)
        print(f"   Loaded {len(loaded_df)} records")
        print(f"   Columns: {loaded_df.columns.tolist()}")
        
        # Step 5: Verify schema compliance
        print("\n5️⃣ Verifying UNIFIED_SCHEMA compliance...")
        assert set(loaded_df.columns) == set(UNIFIED_SCHEMA), \
            f"Loaded data must have exactly UNIFIED_SCHEMA columns"
        print("   ✓ Schema compliance verified")
        
        # Verify no obsolete columns exist
        for col in ['id', 'ref_no', 'source', 'date_registered', 'tags', 'legacy_field']:
            assert col not in loaded_df.columns, f"Obsolete column '{col}' should not exist"
        print("   ✓ No obsolete columns found")
        
        # Step 6: Apply display headers
        print("\n6️⃣ Applying Korean display headers...")
        display_df = loaded_df.rename(columns=DISPLAY_HEADERS)
        print(f"   Display columns: {display_df.columns.tolist()}")
        
        # Verify all Korean headers are present
        expected_korean_headers = list(DISPLAY_HEADERS.values())
        assert all(header in display_df.columns for header in expected_korean_headers), \
            "All Korean headers should be present"
        print("   ✓ Korean headers applied successfully")
        
        # Step 7: Test CSV encoding
        print("\n7️⃣ Testing CSV encoding with utf-8-sig...")
        csv_data = display_df.to_csv(index=False).encode('utf-8-sig')
        
        # Verify BOM (Byte Order Mark) is present
        assert csv_data[:3] == b'\xef\xbb\xbf', \
            "CSV should start with UTF-8 BOM for Excel compatibility"
        print("   ✓ UTF-8-sig encoding verified (BOM present)")
        
        # Verify Korean characters are preserved
        csv_text = csv_data.decode('utf-8-sig')
        assert '등록일자' in csv_text, "Korean headers should be in CSV"
        assert '냉이' in csv_text, "Korean content should be in CSV"
        print("   ✓ Korean characters preserved in CSV")
        
        # Step 8: Test duplicate prevention
        print("\n8️⃣ Testing deduplication...")
        # Try to add the same data again
        count2 = save_to_hub(validated_df, data_dir)
        assert count2 == 0, "Duplicate records should not be added"
        print("   ✓ Deduplication working correctly")
        
        # Verify record count unchanged
        loaded_df2 = load_all_data(data_dir)
        assert len(loaded_df2) == len(loaded_df), "Record count should not increase with duplicates"
        print("   ✓ Record count unchanged after duplicate attempt")
        
        # Step 9: Test field value verification
        print("\n9️⃣ Verifying data content...")
        first_record = loaded_df.iloc[0]
        assert first_record['data_source'] == 'MFDS'
        assert first_record['product_name'] == '냉이 신선제품'
        assert first_record['hazard_category'] == '잔류농약'
        assert first_record['analyzable'] == True
        print("   ✓ Data content verified")
        
        print("\n" + "=" * 70)
        print("✅ ALL END-TO-END TESTS PASSED!")
        print("=" * 70)
        print("\nSummary:")
        print(f"  • Schema enforcement: ✓ Working")
        print(f"  • Obsolete column filtering: ✓ Working")
        print(f"  • Korean display headers: ✓ Working")
        print(f"  • CSV UTF-8-sig encoding: ✓ Working")
        print(f"  • Deduplication: ✓ Working")
        print(f"  • Data integrity: ✓ Working")


if __name__ == '__main__':
    test_end_to_end_schema_enforcement()
