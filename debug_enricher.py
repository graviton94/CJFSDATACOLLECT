
import pandas as pd
from src.utils.reference_enricher import ReferenceEnricher

def debug_extraction():
    enricher = ReferenceEnricher()
    
    # Problematic text provided by user
    # Note: User wrote "황생포도상구균" (Typo?) usually "황색포도상구균".
    # But even with typo, it should not extract "물".
    text = "미생물(황생포도상구균) 허용기준 위반(초과 검출): 420, 0, 0, 0, 160(기준n=5, c=1, m=10, M=100)"
    
    print(f"Input Text: {text}")
    
    # Test enrich_record (which calls extract_hazard)
    # create a dummy record
    record = {
        'product_name': 'Test Product',
        'source_text': text,
        'hazard_item': None # Simulate missing hazard
    }
    
    # We need to simulate single record enrichment or just call internal method if accessible
    # ReferenceEnricher.enrich takes a list of dicts/df.
    
    df = pd.DataFrame([record])
    
    # Enrich
    enriched_df = enricher.enrich(df)
    
    print("--- Result ---")
    print(enriched_df[['source_text', 'hazard_item']])
    
    # Also lets inspect the hazard master to see if "물" is there
    print("\n--- Hazard Master Check for '물' ---")
    if '물' in enricher.hazard_matcher.keywords:
         print("'물' is in keywords.")
    else:
         print("'물' is NOT in keywords.")

    # Check "황색포도상구균"
    print(f"'황색포도상구균' in keywords: {'황색포도상구균' in enricher.hazard_matcher.keywords}")
    print(f"'황생포도상구균' (User Typo) in keywords: {'황생포도상구균' in enricher.hazard_matcher.keywords}")


if __name__ == "__main__":
    debug_extraction()
