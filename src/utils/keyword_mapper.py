import pandas as pd
from pathlib import Path
from typing import Optional

class KeywordMapper:
    """
    Handles hard-coded keyword mappings from external reference file.
    Replaces inline if/else logic in collectors.
    """
    
    def __init__(self, ref_path: str = "data/reference/keyword_map_master.parquet"):
        self.ref_path = Path(ref_path)
        self.rules = self._load_rules()
        
    def _load_rules(self) -> pd.DataFrame:
        if not self.ref_path.exists():
            print(f"⚠️ Keyword mapping file not found: {self.ref_path}")
            return pd.DataFrame(columns=['keyword', 'hazard_item', 'source'])
            
        try:
            df = pd.read_parquet(self.ref_path)
            # Ensure columns exist
            required = ['keyword', 'hazard_item']
            if not all(col in df.columns for col in required):
                print(f"⚠️ Keyword map missing columns: {df.columns}")
                return pd.DataFrame()
                
            # Pre-processing: Lowercase keywords for case-insensitive matching? 
            # Or keep original? Let's do case-insensitive match at runtime.
            return df
        except Exception as e:
            print(f"❌ Error loading keyword map: {e}")
            return pd.DataFrame()

    def map_hazard(self, text: str, source: str = None) -> Dict[str, Optional[str]]:
        """
        Scan text for keywords and return mapped hazard info.
        Prioritizes Longest Keyword Match.
        """
        if not text or self.rules.empty:
            return None
            
        # Filter rules by source if provided
        if source:
            mask = (self.rules['source'] == 'ALL') | (self.rules['source'] == source)
            candidate_rules = self.rules[mask]
        else:
            candidate_rules = self.rules
            
        if candidate_rules.empty:
            return None
            
        best_match_row = None
        best_len = 0
        
        text_lower = str(text).lower()
        
        for _, row in candidate_rules.iterrows():
            keyword = str(row['keyword'])
            
            if not keyword: continue
            
            # Case-insensitive check
            if keyword.lower() in text_lower:
                # Prefer longest keyword match
                if len(keyword) > best_len:
                    best_match_row = row
                    best_len = len(keyword)
        
        if best_match_row is not None:
            return {
                "hazard_item": best_match_row.get('hazard_item'),
                "class_m": best_match_row.get('class_m'),
                "class_l": best_match_row.get('class_l')
            }
                    
        return None
