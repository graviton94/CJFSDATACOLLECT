
import pandas as pd
from pathlib import Path

def check_cats():
    path = Path("data/reference/product_code_master.parquet")
    df = pd.read_parquet(path)
    
    cats = ["과자류", "빵류", "면류", "음료류", "주류", "농산물", "수산물", "축산물", "가공식품", 
            "치즈류", "버터류", "우유류", "유가공품", "식육가공품", "알가공품", "식품첨가물", 
            "건강기능식품", "기구", "용기", "포장", "화장품", "의약외품", "위생용품",
            "과자", "빵", "소금", "설탕", "빙과류", "초콜릿류", "잼류", "두부류", "묵류", "식용유지류"]
    
    unique_names = set(df['KOR_NM'].dropna().unique())
    
    print("--- Exact Matches ---")
    for c in cats:
        if c in unique_names:
            print(f"✅ {c}")
        else:
            # Check for partial
            matches = [n for n in unique_names if c in n]
            if matches:
                 print(f"⚠️ {c} (Not exact, found {len(matches)} partials e.g., {matches[:3]})")
            else:
                 print(f"❌ {c}")

if __name__ == "__main__":
    check_cats()
