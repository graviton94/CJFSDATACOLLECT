import pandas as pd
from pathlib import Path
import sys
import re

# Add src to path
sys.path.insert(0, str(Path.cwd()))
from src.utils.fuzzy_matcher import FuzzyMatcher

def auto_map_overrides():
    # 1. Load Data
    try:
        fda_df = pd.read_parquet("data/reference/fda_list_master.parquet")
        product_ref = pd.read_parquet("data/reference/product_code_master.parquet")
        hazard_ref = pd.read_parquet("data/reference/hazard_code_master.parquet")
        print(f"Loaded: FDA({len(fda_df)}), Product({len(product_ref)}), Hazard({len(hazard_ref)})")
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    matcher = FuzzyMatcher(similarity_threshold=60, long_text_threshold=10) # Lower threshold for discovery, but rely on keyword scanning

    updated_hazard_count = 0
    updated_product_count = 0

    # 2. Iterate and Match
    for idx, row in fda_df.iterrows():
        # Skip if already manually set (unless we want to overwrite? Let's skip valid non-empty)
        # But newly created columns are None, so we target None.
        
        # --- HAZARD MAPPING (Title -> Hazard ENG_NM) ---
        if pd.isna(row.get('Manual_Hazard_Item')):
            title = row.get('Title', '')
            if title and isinstance(title, str):
                # Clean up title (remove "Detention Without Physical Examination of...")
                clean_title = re.sub(r'Detention Without Physical Examination of', '', title, flags=re.IGNORECASE).strip()
                
                # Use extract_hazard_item logic but targeting ENG_NM
                # We can use the matcher's existing logic which checks KOR_NM/ENG_NM
                # But we specifically want to match English Title to English Ref
                
                # Check directly against ENG_NM column using scanning
                # We can iterate hazard_ref's ENG_NM and see if it's in Title
                best_eng = None
                best_len = 0
                
                # Filter valid English names from ref
                valid_eng_refs = hazard_ref[hazard_ref['ENG_NM'].notna() & (hazard_ref['ENG_NM'] != "")]
                
                for _, h_row in valid_eng_refs.iterrows():
                    eng_nm = str(h_row['ENG_NM']).strip()
                    if len(eng_nm) < 3: continue # Skip too short
                    
                    if eng_nm.lower() in clean_title.lower():
                        if len(eng_nm) > best_len:
                            best_eng = h_row
                            best_len = len(eng_nm)
                
                if best_eng is not None:
                    # Found a match! Set Manual Hazard Item
                    # Prefer KOR_NM for consistency if available, else ENG_NM?
                    # User asked to map to ENG_NM? "title을 기준으로 hazard_code_master의 ENG_NM과 하드매핑"
                    # But Manual_Hazard_Item is usually the RAW text used for classification.
                    # If we put "Salmonella" (ENG), our matcher handles it.
                    # Let's put the KOR_NM if available (better for display), or ENG_NM.
                    # Actually, if we put the specific ENG_NM we found, it's safer.
                    fda_df.at[idx, 'Manual_Hazard_Item'] = best_eng['KOR_NM'] if pd.notna(best_eng['KOR_NM']) else best_eng['ENG_NM']
                    updated_hazard_count += 1
                    # print(f"[H] {clean_title[:30]}... -> {fda_df.at[idx, 'Manual_Hazard_Item']}")

        # --- PRODUCT MAPPING (Description -> Product ENG_NM => KOR_NM) ---
        if pd.isna(row.get('Manual_Product_Type')):
            desc = row.get('Product_Description', '')
            if desc and isinstance(desc, str):
                # Similar logic: Scan for valid Product ENG_NM in description
                best_prod = None
                best_len = 0
                
                valid_prod_refs = product_ref[product_ref['ENG_NM'].notna() & (product_ref['ENG_NM'] != "")]
                
                for _, p_row in valid_prod_refs.iterrows():
                    eng_nm = str(p_row['ENG_NM']).strip()
                    if len(eng_nm) < 4: continue # Skip very short words like "Tea", "Oil" might be dangerous?
                    
                    # Pattern check (word boundary) to avoid "Tea" matching "Tear"
                    if re.search(r'\b' + re.escape(eng_nm) + r'\b', desc, re.IGNORECASE):
                        if len(eng_nm) > best_len:
                            best_prod = p_row
                            best_len = len(eng_nm)
                
                if best_prod is not None:
                    # Set Manual Product Type to KOR_NM (User requested: "product description 기준으로 KOR_NM과 하드매핑")
                    if pd.notna(best_prod['KOR_NM']):
                        fda_df.at[idx, 'Manual_Product_Type'] = best_prod['KOR_NM']
                        updated_product_count += 1
                        # print(f"[P] {desc[:30]}... -> {best_prod['KOR_NM']}")

    print(f"Auto-Mapping Complete.")
    print(f"Updated Hazard Items: {updated_hazard_count}")
    print(f"Updated Product Types: {updated_product_count}")
    
    # Save
    fda_df.to_parquet("data/reference/fda_list_master.parquet", engine='pyarrow', compression='snappy')
    print("Saved to data/reference/fda_list_master.parquet")

if __name__ == "__main__":
    auto_map_overrides()
