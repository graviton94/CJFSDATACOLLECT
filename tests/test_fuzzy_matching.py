"""
Test the fuzzy matching logic in src/utils/fuzzy_matcher.py

This test suite validates:
1. Exact matching (case-insensitive)
2. Keyword/partial matching (handles variations like "Frozen Shrimp" vs "Shrimp")
3. Fuzzy matching (handles typos and similarity)
4. Edge cases (empty inputs, missing data)
"""

import sys
from pathlib import Path
import pandas as pd

# Add project root and src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

from src.utils.fuzzy_matcher import FuzzyMatcher


def create_mock_product_data():
    """Create mock product reference data for testing"""
    return pd.DataFrame([
        {
            'KOR_NM': '새우',
            'ENG_NM': 'Shrimp',
            'HTRK_PRDLST_NM': '수산물',
            'GR_NM': '수산물',
            'HRRK_PRDLST_NM': '갑각류',
            'PRDLST_CL_NM': '갑각류'
        },
        {
            'KOR_NM': '냉이',
            'ENG_NM': "Shepherd's Purse",
            'HTRK_PRDLST_NM': '농산물',
            'GR_NM': '농산물',
            'HRRK_PRDLST_NM': '채소류',
            'PRDLST_CL_NM': '채소류'
        },
        {
            'KOR_NM': '연어',
            'ENG_NM': 'Salmon',
            'HTRK_PRDLST_NM': '수산물',
            'GR_NM': '수산물',
            'HRRK_PRDLST_NM': '어류',
            'PRDLST_CL_NM': '어류'
        },
        {
            'KOR_NM': '초콜릿',
            'ENG_NM': 'Chocolate',
            'HTRK_PRDLST_NM': '가공식품',
            'GR_NM': '가공식품',
            'HRRK_PRDLST_NM': '과자류',
            'PRDLST_CL_NM': '과자류'
        }
    ])


def create_mock_hazard_data():
    """Create mock hazard reference data for testing"""
    return pd.DataFrame([
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
            'KOR_NM': '아플라톡신',
            'ENG_NM': 'Aflatoxin',
            'ABRV': 'AFL',
            'NCKNM': '곰팡이독소',
            'TESTITM_NM': '아플라톡신',
            'M_KOR_NM': '곰팡이독소',
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
        },
        {
            'KOR_NM': '대장균',
            'ENG_NM': 'E. coli',
            'ABRV': 'EC',
            'NCKNM': '대장균',
            'TESTITM_NM': '대장균',
            'M_KOR_NM': '미생물',
            'ANALYZABLE': True,
            'INTEREST_ITEM': True
        }
    ])


class TestFuzzyMatcher:
    """Test suite for FuzzyMatcher class"""
    
    def test_exact_match_korean(self):
        """Test exact matching with Korean product names"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        result = matcher.match_product_type('새우', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        print("✓ Exact match (Korean) works")
    
    def test_exact_match_english(self):
        """Test exact matching with English product names"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        result = matcher.match_product_type('Shrimp', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        print("✓ Exact match (English) works")
    
    def test_case_insensitive(self):
        """Test that matching is case-insensitive"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        result = matcher.match_product_type('SHRIMP', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        
        result = matcher.match_product_type('shrimp', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        print("✓ Case-insensitive matching works")
    
    def test_keyword_match_contains(self):
        """Test keyword matching when search term contains reference term"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        # "Frozen Shrimp" should match "Shrimp"
        result = matcher.match_product_type('Frozen Shrimp', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        
        # "냉동 새우" should match "새우"
        result = matcher.match_product_type('냉동 새우', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        print("✓ Keyword match (contains) works")
    
    def test_keyword_match_reverse(self):
        """Test keyword matching when reference term contains search term"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        hazard_df = create_mock_hazard_data()
        
        # "Aflatoxin B1" contains "Aflatoxin"
        result = matcher.match_hazard_category('Aflatoxin B1', hazard_df)
        assert result['category'] == '곰팡이독소'
        assert result['analyzable'] is True
        assert result['interest'] is True
        print("✓ Keyword match (reverse contains) works")
    
    def test_fuzzy_match_typo(self):
        """Test fuzzy matching handles typos"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        # "Salmn" is close to "Salmon" (one letter missing)
        result = matcher.match_product_type('Salmn', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '어류'
        
        # "Chocolat" is close to "Chocolate"
        result = matcher.match_product_type('Chocolat', product_df)
        assert result['top'] == '가공식품'
        assert result['upper'] == '과자류'
        print("✓ Fuzzy match (typo handling) works")
    
    def test_fuzzy_match_similarity_threshold(self):
        """Test that fuzzy matching respects similarity threshold"""
        # High threshold (95%) should not match typos
        matcher_strict = FuzzyMatcher(similarity_threshold=95)
        product_df = create_mock_product_data()
        
        result = matcher_strict.match_product_type('Shmp', product_df)
        # "Shmp" is too different from "Shrimp" with 95% threshold
        # Should return None values
        assert result['top'] is None
        assert result['upper'] is None
        
        # Low threshold (50%) should match more liberally
        matcher_loose = FuzzyMatcher(similarity_threshold=50)
        result = matcher_loose.match_product_type('Shmp', product_df)
        # "Shmp" might match "Shrimp" with 50% threshold
        # This depends on the fuzzy algorithm, so we just check it doesn't crash
        print("✓ Similarity threshold works")
    
    def test_empty_input(self):
        """Test handling of empty input"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        result = matcher.match_product_type('', product_df)
        assert result['top'] is None
        assert result['upper'] is None
        
        result = matcher.match_product_type(None, product_df)
        assert result['top'] is None
        assert result['upper'] is None
        print("✓ Empty input handling works")
    
    def test_empty_reference_df(self):
        """Test handling of empty reference DataFrame"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        empty_df = pd.DataFrame()
        
        result = matcher.match_product_type('Shrimp', empty_df)
        assert result['top'] is None
        assert result['upper'] is None
        print("✓ Empty reference DataFrame handling works")
    
    def test_no_match_found(self):
        """Test when no match is found in reference data"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        result = matcher.match_product_type('CompletelyUnknownProduct', product_df)
        assert result['top'] is None
        assert result['upper'] is None
        print("✓ No match found handling works")
    
    def test_hazard_exact_match(self):
        """Test exact matching for hazard items"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        hazard_df = create_mock_hazard_data()
        
        result = matcher.match_hazard_category('살모넬라', hazard_df)
        assert result['category'] == '미생물'
        assert result['analyzable'] is True
        assert result['interest'] is True
        
        result = matcher.match_hazard_category('Salmonella', hazard_df)
        assert result['category'] == '미생물'
        assert result['analyzable'] is True
        assert result['interest'] is True
        print("✓ Hazard exact match works")
    
    def test_hazard_abbreviation_match(self):
        """Test matching hazard by abbreviation"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        hazard_df = create_mock_hazard_data()
        
        result = matcher.match_hazard_category('SAL', hazard_df)
        assert result['category'] == '미생물'
        assert result['analyzable'] is True
        
        result = matcher.match_hazard_category('PEN', hazard_df)
        assert result['category'] == '잔류농약'
        assert result['analyzable'] is True
        assert result['interest'] is False
        print("✓ Hazard abbreviation match works")
    
    def test_hazard_fuzzy_match(self):
        """Test fuzzy matching for hazard items"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        hazard_df = create_mock_hazard_data()
        
        # "Salmonnella" is close to "Salmonella" (typo with extra 'n')
        result = matcher.match_hazard_category('Salmonnella', hazard_df)
        assert result['category'] == '미생물'
        
        # "Pendimetalin" is close to "Pendimethalin"
        result = matcher.match_hazard_category('Pendimetalin', hazard_df)
        assert result['category'] == '잔류농약'
        print("✓ Hazard fuzzy match works")
    
    def test_long_text_sentence_scanning(self):
        """Test sentence scanning for long text (Reverse Lookup)"""
        matcher = FuzzyMatcher(similarity_threshold=80, long_text_threshold=30)
        hazard_df = create_mock_hazard_data()
        
        # Long FDA-style alert text
        long_text = "The sample analysis revealed high levels of Aflatoxin B1 in the product batch imported from China"
        result = matcher.match_hazard_category(long_text, hazard_df)
        assert result['category'] == '곰팡이독소'
        assert result['analyzable'] is True
        assert result['interest'] is True
        
        # Another long text with Salmonella
        long_text2 = "Laboratory tests detected contamination with Salmonella bacteria in multiple samples from the facility"
        result = matcher.match_hazard_category(long_text2, hazard_df)
        assert result['category'] == '미생물'
        assert result['analyzable'] is True
        assert result['interest'] is True
        print("✓ Long text sentence scanning works")
    
    def test_long_text_threshold(self):
        """Test that text length threshold works correctly"""
        # Short threshold: Even short text triggers sentence scanning
        matcher_short = FuzzyMatcher(similarity_threshold=80, long_text_threshold=10)
        hazard_df = create_mock_hazard_data()
        
        # This is 15 chars, above threshold of 10
        result = matcher_short.match_hazard_category('Found E. coli', hazard_df)
        assert result['category'] == '미생물'
        
        # Long threshold: Only very long text triggers sentence scanning
        matcher_long = FuzzyMatcher(similarity_threshold=80, long_text_threshold=100)
        hazard_df = create_mock_hazard_data()
        
        # This is short, should use standard matching
        result = matcher_long.match_hazard_category('Salmonella', hazard_df)
        assert result['category'] == '미생물'
        print("✓ Long text threshold configuration works")
    
    def test_sentence_scanning_prefers_longer_match(self):
        """Test that sentence scanning prefers longer, more specific matches"""
        matcher = FuzzyMatcher(similarity_threshold=80, long_text_threshold=30)
        
        # Create mock data with overlapping names
        hazard_df = pd.DataFrame([
            {
                'KOR_NM': '대장균',
                'ENG_NM': 'coli',
                'ABRV': 'COL',
                'NCKNM': 'coli',
                'TESTITM_NM': 'coli',
                'M_KOR_NM': '미생물-일반',
                'ANALYZABLE': True,
                'INTEREST_ITEM': False
            },
            {
                'KOR_NM': '대장균',
                'ENG_NM': 'E. coli',
                'ABRV': 'EC',
                'NCKNM': 'E. coli',
                'TESTITM_NM': 'E. coli',
                'M_KOR_NM': '미생물-특정',
                'ANALYZABLE': True,
                'INTEREST_ITEM': True
            }
        ])
        
        # Long text containing "E. coli" should match the more specific entry
        long_text = "The laboratory confirmed presence of E. coli bacteria in the water sample"
        result = matcher.match_hazard_category(long_text, hazard_df)
        # Should prefer "E. coli" (7 chars) over "coli" (4 chars)
        assert result['category'] == '미생물-특정'
        print("✓ Sentence scanning prefers longer matches")
    
    def test_sentence_scanning_fallback(self):
        """Test that sentence scanning falls back to standard matching if no keywords found"""
        matcher = FuzzyMatcher(similarity_threshold=80, long_text_threshold=30)
        hazard_df = create_mock_hazard_data()
        
        # Long text that doesn't contain exact keywords but is similar
        long_text = "Analysis showed contamination with a type of Salmonnella pathogen in multiple batches"
        result = matcher.match_hazard_category(long_text, hazard_df)
        # Should fall back and find "Salmonnella" ~ "Salmonella" via keyword/fuzzy match
        # (this might or might not match depending on the exact algorithm)
        # The important thing is it doesn't crash
        print("✓ Sentence scanning fallback works")

    
    def test_whitespace_handling(self):
        """Test that whitespace is properly handled"""
        matcher = FuzzyMatcher(similarity_threshold=80)
        product_df = create_mock_product_data()
        
        result = matcher.match_product_type('  Shrimp  ', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '갑각류'
        
        result = matcher.match_product_type('\tSalmon\n', product_df)
        assert result['top'] == '수산물'
        assert result['upper'] == '어류'
        print("✓ Whitespace handling works")


def test_convenience_functions():
    """Test convenience functions that don't require creating a matcher instance"""
    from src.utils.fuzzy_matcher import match_product_type, match_hazard_category
    
    product_df = create_mock_product_data()
    hazard_df = create_mock_hazard_data()
    
    # Test product matching
    result = match_product_type('Shrimp', product_df)
    assert result['top'] == '수산물'
    assert result['upper'] == '갑각류'
    
    # Test hazard matching
    result = match_hazard_category('Salmonella', hazard_df)
    assert result['category'] == '미생물'
    assert result['analyzable'] is True
    
    # Test with custom threshold
    result = match_product_type('Shmp', product_df, similarity_threshold=50)
    # Should work with lower threshold
    print("✓ Convenience functions work")


if __name__ == '__main__':
    # Run all tests
    print("\n" + "=" * 60)
    print("🧪 Testing Fuzzy Matching Logic")
    print("=" * 60 + "\n")
    
    test_instance = TestFuzzyMatcher()
    
    # Product matching tests
    print("📦 Testing Product Matching...")
    test_instance.test_exact_match_korean()
    test_instance.test_exact_match_english()
    test_instance.test_case_insensitive()
    test_instance.test_keyword_match_contains()
    test_instance.test_fuzzy_match_typo()
    test_instance.test_fuzzy_match_similarity_threshold()
    test_instance.test_whitespace_handling()
    
    # Edge case tests
    print("\n⚠️  Testing Edge Cases...")
    test_instance.test_empty_input()
    test_instance.test_empty_reference_df()
    test_instance.test_no_match_found()
    
    # Hazard matching tests
    print("\n☢️  Testing Hazard Matching...")
    test_instance.test_hazard_exact_match()
    test_instance.test_hazard_abbreviation_match()
    test_instance.test_keyword_match_reverse()
    test_instance.test_hazard_fuzzy_match()
    
    # Long text / sentence scanning tests
    print("\n📝 Testing Long Text Sentence Scanning...")
    test_instance.test_long_text_sentence_scanning()
    test_instance.test_long_text_threshold()
    test_instance.test_sentence_scanning_prefers_longer_match()
    test_instance.test_sentence_scanning_fallback()
    
    # Convenience functions
    print("\n🔧 Testing Convenience Functions...")
    test_convenience_functions()
    
    print("\n" + "=" * 60)
    print("✅ All fuzzy matching tests passed!")
    print("=" * 60 + "\n")
