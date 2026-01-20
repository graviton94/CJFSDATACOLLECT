"""
Test the smart lookup logic in mfds_collector.py
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile
import os

# Add project root and src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

from src.collectors.mfds_collector import MFDSCollector


def test_lookup_product_info_with_mock_data():
    """Test product info lookup with mock reference data."""
    
    # Create temporary reference directory with mock data
    with tempfile.TemporaryDirectory() as tmpdir:
        ref_dir = Path(tmpdir) / "reference"
        ref_dir.mkdir(parents=True)
        
        # Create mock product_code_master.parquet
        product_data = pd.DataFrame([
            {
                'KOR_NM': '냉이',
                'ENG_NM': 'Shepherd\'s Purse',
                'HTRK_PRDLST_CD': '농산물',
                'HRRK_PRDLST_CD': '채소류'
            },
            {
                'KOR_NM': '연어',
                'ENG_NM': 'Salmon',
                'HTRK_PRDLST_CD': '수산물',
                'HRRK_PRDLST_CD': '어류'
            }
        ])
        product_data.to_parquet(ref_dir / "product_code_master.parquet", engine='pyarrow')
        
        # Create mock hazard_code_master.parquet
        hazard_data = pd.DataFrame([
            {
                'KOR_NM': '살모넬라',
                'ENG_NM': 'Salmonella',
                'ABRV': 'SAL',
                'NCKNM': '살모넬라균',
                'TESTITM_NM': '살모넬라',
                'M_KOR_NM': '미생물',
                'ANALYZABLE': True,
                'INTEREST_ITEM': True
            },
            {
                'KOR_NM': '펜디메탈린',
                'ENG_NM': 'Pendimethalin',
                'ABRV': 'PEN',
                'NCKNM': '농약',
                'TESTITM_NM': '펜디메탈린',
                'M_KOR_NM': '잔류농약',
                'ANALYZABLE': True,
                'INTEREST_ITEM': False
            }
        ])
        hazard_data.to_parquet(ref_dir / "hazard_code_master.parquet", engine='pyarrow')
        
        # Create mock country_master.tsv (empty for this test)
        country_tsv = ref_dir / "country_master.tsv"
        with open(country_tsv, 'w', encoding='utf-8') as f:
            f.write("국가명(국문)\t국가명(영문)\tISO(2자리)\tISO(3자리)\n")
        
        # Monkey-patch REF_DIR for testing
        original_ref_dir = MFDSCollector.REF_DIR
        MFDSCollector.REF_DIR = ref_dir
        
        try:
            # Create collector instance (will load our mock data)
            # Note: This will fail if KOREA_FOOD_API_KEY is not set, so we need to handle that
            original_key = os.getenv("KOREA_FOOD_API_KEY")
            os.environ["KOREA_FOOD_API_KEY"] = "test-key-for-unit-test"
            
            collector = MFDSCollector()
            
            # Test 1: Lookup by Korean name
            result = collector._lookup_product_info('냉이')
            assert result['top'] == '농산물', f"Expected '농산물', got {result['top']}"
            assert result['upper'] == '채소류', f"Expected '채소류', got {result['upper']}"
            print("✓ Product lookup by Korean name works")
            
            # Test 2: Lookup by English name
            result = collector._lookup_product_info('Salmon')
            assert result['top'] == '수산물', f"Expected '수산물', got {result['top']}"
            assert result['upper'] == '어류', f"Expected '어류', got {result['upper']}"
            print("✓ Product lookup by English name works")
            
            # Test 3: Lookup non-existent product
            result = collector._lookup_product_info('NonExistent')
            assert result['top'] is None, "Non-existent product should return None"
            assert result['upper'] is None, "Non-existent product should return None"
            print("✓ Product lookup handles missing data correctly")
            
            # Test 4: Hazard lookup by Korean name
            result = collector._lookup_hazard_info('살모넬라')
            assert result['category'] == '미생물', f"Expected '미생물', got {result['category']}"
            assert result['analyzable'] is True, "Expected analyzable=True"
            assert result['interest'] is True, "Expected interest=True"
            print("✓ Hazard lookup by Korean name works")
            
            # Test 5: Hazard lookup by English name
            result = collector._lookup_hazard_info('Pendimethalin')
            assert result['category'] == '잔류농약', f"Expected '잔류농약', got {result['category']}"
            assert result['analyzable'] is True, "Expected analyzable=True"
            assert result['interest'] is False, "Expected interest=False"
            print("✓ Hazard lookup by English name works")
            
            # Test 6: Hazard lookup by abbreviation
            result = collector._lookup_hazard_info('SAL')
            assert result['category'] == '미생물', f"Expected '미생물', got {result['category']}"
            print("✓ Hazard lookup by abbreviation works")
            
            # Test 7: Hazard lookup handles missing data
            result = collector._lookup_hazard_info('UnknownHazard')
            assert result['category'] is None, "Unknown hazard should return None category"
            assert result['analyzable'] is False, "Unknown hazard should return False analyzable"
            assert result['interest'] is False, "Unknown hazard should return False interest"
            print("✓ Hazard lookup handles missing data correctly")
            
            # Restore original environment
            if original_key:
                os.environ["KOREA_FOOD_API_KEY"] = original_key
            else:
                os.environ.pop("KOREA_FOOD_API_KEY", None)
                
        finally:
            # Restore original REF_DIR
            MFDSCollector.REF_DIR = original_ref_dir


if __name__ == '__main__':
    test_lookup_product_info_with_mock_data()
    print("\n✅ All lookup logic tests passed!")
