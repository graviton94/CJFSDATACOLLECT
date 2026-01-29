import pandas as pd
from pathlib import Path

def sync_keyword_master():
    k_csv_path = Path("data/reference/keyword_map_master.csv")
    k_pq_path = Path("data/reference/keyword_map_master.parquet")
    
    if not k_csv_path.exists():
        print("CSV file missing.")
        return

    print(f"Loading CSV from {k_csv_path}...")
    # Load CSV (User might have edited this)
    df = pd.read_csv(k_csv_path)
    
    # 1. Standardize column names if they are old
    rename_map = {
        'hazard_item': 'TESTITM_NM',
        'class_m': 'M_KOR_NM',
        'class_l': 'L_KOR_NM'
    }
    df = df.rename(columns=rename_map)
    
    # 2. Ensure TESTITM_CD exists and is up to date (Z-1, Z-2...)
    print("Regenerating Z-prefix codes based on current row order...")
    df['TESTITM_CD'] = [f"Z-{i+1}" for i in range(len(df))]
    
    # 3. Save synchronized versions
    print("Saving synchronized CSV and Parquet...")
    # Save CSV with updated headers/codes for future editing
    df.to_csv(k_csv_path, index=False, encoding='utf-8-sig')
    # Save Parquet for the application to use
    df.to_parquet(k_pq_path, index=False)
    
    print(f"Sync complete. {len(df)} entries processed.")

if __name__ == "__main__":
    sync_keyword_master()
