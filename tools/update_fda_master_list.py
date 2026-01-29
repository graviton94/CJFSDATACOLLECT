
import pandas as pd
from pathlib import Path

def update_master():
    csv_path = Path("data/fda_master.csv")
    pq_path = Path("data/reference/fda_list_master.parquet")
    
    if not csv_path.exists() or not pq_path.exists():
        print("Missing files.")
        return

    # Load data
    print("Loading datasets...")
    csv_df = pd.read_csv(csv_path, encoding='cp949')
    pq_df = pd.read_parquet(pq_path)
    
    # CSV Columns: ['Alert 번호', '수입 경보 제목 (Title)', '위반 사유 코드 (OASIS)', '제품 상세 설명 (Product Description)']
    # Parquet Key: Alert_No
    
    # Prepare mapping
    # Ensure Alert_No is string and cleaned
    csv_df['Alert 번호'] = csv_df['Alert 번호'].astype(str).str.strip()
    pq_df['Alert_No'] = pq_df['Alert_No'].astype(str).str.strip()
    
    # Perform update
    print(f"Original Parquet rows: {len(pq_df)}")
    
    # Build dictionaries for fast lookup
    title_map = csv_df.set_index('Alert 번호')['수입 경보 제목 (Title)'].to_dict()
    oasis_map = csv_df.set_index('Alert 번호')['위반 사유 코드 (OASIS)'].to_dict()
    desc_map = csv_df.set_index('Alert 번호')['제품 상세 설명 (Product Description)'].to_dict()
    
    updated_count = 0
    for idx, row in pq_df.iterrows():
        alert_no = row['Alert_No']
        if alert_no in title_map:
            # Update core columns
            pq_df.at[idx, 'Title'] = title_map[alert_no]
            pq_df.at[idx, 'OASIS_Charge_Code_Line'] = oasis_map[alert_no]
            pq_df.at[idx, 'Product_Description'] = desc_map[alert_no]
            
            updated_count += 1
            
    print(f"✅ Updated {updated_count} records in master index.")
    
    # Save back to parquet
    pq_df.to_parquet(pq_path, index=False)
    print(f"💾 Saved to {pq_path}")

if __name__ == "__main__":
    update_master()
