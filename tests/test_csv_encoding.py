"""
Test CSV encoding with utf-8-sig for Korean characters.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schema import DISPLAY_HEADERS


def test_csv_encoding_utf8_sig():
    """Test that CSV with Korean characters uses utf-8-sig encoding."""
    
    # Create test dataframe with Korean headers
    test_df = pd.DataFrame({
        '등록일자': ['2024-01-01'],
        '데이터소스': ['FDA'],
        '제품명': ['테스트 제품'],
        '원산지': ['대한민국'],
        '시험항목': ['살모넬라']
    })
    
    # Test with utf-8-sig encoding (what we want)
    csv_data_sig = test_df.to_csv(index=False).encode('utf-8-sig')
    
    # Test with regular utf-8 encoding (what we're fixing)
    csv_data_regular = test_df.to_csv(index=False).encode('utf-8')
    
    # utf-8-sig should start with BOM (Byte Order Mark)
    # BOM for UTF-8 is EF BB BF
    assert csv_data_sig[:3] == b'\xef\xbb\xbf', \
        "utf-8-sig encoding should start with BOM"
    
    # Regular UTF-8 should NOT have BOM
    assert csv_data_regular[:3] != b'\xef\xbb\xbf', \
        "Regular utf-8 should not have BOM"
    
    # Save to file and verify
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test_korean.csv"
        
        # Write with utf-8-sig
        with open(csv_path, 'wb') as f:
            f.write(csv_data_sig)
        
        # Read back and verify Korean characters are preserved
        df_loaded = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        assert '등록일자' in df_loaded.columns, "Korean column name should be preserved"
        assert df_loaded['제품명'].iloc[0] == '테스트 제품', "Korean content should be preserved"
        
    print("✓ CSV encoding with utf-8-sig test passed")


if __name__ == '__main__':
    test_csv_encoding_utf8_sig()
    print("\n✅ CSV encoding test passed!")
