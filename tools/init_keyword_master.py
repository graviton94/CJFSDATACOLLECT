import pandas as pd
from pathlib import Path

def create_keyword_master():
    data = [
        # MFDS Existing Rules
        {"keyword": "알레르기", "hazard_item": "표시위반(알러젠)", "class_m": "표시기준 위반", "class_l": "표시/기타", "source": "ALL"},
        {"keyword": "알러지", "hazard_item": "표시위반(알러젠)", "class_m": "표시기준 위반", "class_l": "표시/기타", "source": "ALL"},
        {"keyword": "잔류농약", "hazard_item": "잔류농약", "class_m": "잔류농약", "class_l": "농약", "source": "ALL"},
        {"keyword": "수입신고", "hazard_item": "수입신고 위반", "class_m": "기타", "class_l": "표시/기타", "source": "MFDS"}, 
        {"keyword": "무신고", "hazard_item": "수입신고 위반", "class_m": "기타", "class_l": "표시/기타", "source": "MFDS"},
        {"keyword": "영업등록", "hazard_item": "미등록 영업", "class_m": "기타", "class_l": "표시/기타", "source": "MFDS"}, 
        {"keyword": "무등록", "hazard_item": "미등록 영업", "class_m": "기타", "class_l": "표시/기타", "source": "MFDS"},
        # ... Other examples
        {"keyword": "Salmonella", "hazard_item": "Salmonella", "class_m": "식중독균", "class_l": "미생물", "source": "ALL"},
    ]
    
    df = pd.DataFrame(data)
    output_path = Path("data/reference/keyword_map_master.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(output_path, engine='pyarrow', compression='snappy')
    print(f"Created {output_path} with {len(df)} rules.")
    
    # Save CSV for user review
    df.to_csv(output_path.with_suffix('.csv'), index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    create_keyword_master()
