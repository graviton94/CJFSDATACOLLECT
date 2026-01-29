
import pandas as pd
from src.utils.keyword_mapper import KeywordMapper
from src.utils.fuzzy_matcher import FuzzyMatcher
import logging

def debug_v2():
    # Setup
    text = "미생물(황생포도상구균) 허용기준 위반(초과 검출): 420, 0, 0, 0, 160(기준n=5, c=1, m=10, M=100)"
    
    print(f"Testing Text: {text}")
    
    # 1. Test KeywordMapper
    print("\n--- KeywordMapper Test ---")
    mapper = KeywordMapper()
    res = mapper.map_hazard(text)
    print(f"KeywordMapper Result: {res}")
    
    # Check if '물' matches
    if not mapper.rules.empty:
        water_row = mapper.rules[mapper.rules['keyword'] == '물']
        if not water_row.empty:
            print(f"'물' exists in rules: {water_row.to_dict('records')}")
        else:
            print("'물' NOT in rules.")
            
        # Check '황색포도상구균'
        staph_row = mapper.rules[mapper.rules['keyword'] == '황색포도상구균']
        print(f"'황색포도상구균' found: {not staph_row.empty}")

    # 2. Test FuzzyMatcher (if used)
    # Note: FuzzyMatcher is expensive to init if master is large
    # Let's see if we can init it easily
    print("\n--- FuzzyMatcher Test ---")
    try:
        fuzzy = FuzzyMatcher()
        # fuzzy.match_hazard(text) ? 
        # Checking fuzzy_matcher.py content might be needed, but assuming standard usage
        # Usually it has find_best_match or similar
        # Let's inspect available methods via dir
        print(f"FuzzyMatcher methods: {[m for m in dir(fuzzy) if not m.startswith('_')]}")
        # Assuming extract_hazard or similar
    except Exception as e:
        print(f"FuzzyMatcher init failed: {e}")

if __name__ == "__main__":
    debug_v2()
