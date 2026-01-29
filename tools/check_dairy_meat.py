
import pandas as pd
from pathlib import Path

def check_dm():
    path = Path("data/reference/product_code_master.parquet")
    df = pd.read_parquet(path)
    
    cats = ["유가공품", "알가공품", "식육가공품", "수산가공품", "수산물", "농산물", "임산물", "축산물", 
            "기타가공품", "규격외일반가공식품", "식품첨가물", "기구", "용기", "포장"]
    
    unique_names = set(df['KOR_NM'].dropna().unique())
    
    print("--- Exact Matches ---")
    for c in cats:
        if c in unique_names:
            print(f"✅ {c}")
        else:
             print(f"❌ {c}")
    
    print("\n--- '치즈' contains ---")
    cheese = [n for n in unique_names if "치즈" in n]
    print(cheese[:10])
    
    print("\n--- '우유' contains ---")
    milk = [n for n in unique_names if "우유" in n]
    print(milk[:10])

    print("\n--- '버터' contains ---")
    butter = [n for n in unique_names if "버터" in n]
    print(butter[:10])

if __name__ == "__main__":
    check_dm()
