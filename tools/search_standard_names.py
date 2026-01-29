
import pandas as pd
from pathlib import Path

def search_terms():
    path = Path("data/reference/product_code_master.parquet")
    df = pd.read_parquet(path)
    
    # We want to map FDA categories to 'KOR_NM' in Master.
    # FDA Categories:
    # 02: 곡류, 03: 베이커리, 12: 치즈, 16: 수산물...
    
    searches = ["곡류", "빵", "면", "시리얼", "스낵", "우유", "치즈", "아이스크림", "알가공품", "수산물", "육류", "과일", "채소", "음료", "커피", "주류", "초콜릿", "젤라틴", "소스", "영유아", "건강기능식품", "식품첨가물", "화장품", "사료", "의료기기"]
    
    found_map = {}
    
    unique_names = set(df['KOR_NM'].dropna().unique())
    
    for term in searches:
        # Find exact or close matches
        matches = [n for n in unique_names if term in n]
        # Sort by length to find shortest (likely category)
        matches.sort(key=len)
        found_map[term] = matches[:10] # Top 10 shortest
        
    for k, v in found_map.items():
        print(f"--- {k} ---")
        print(v)

if __name__ == "__main__":
    search_terms()
