import pandas as pd
from pathlib import Path

def update_fda_master():
    file_path = Path("data/reference/fda_list_master.parquet")
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    df = pd.read_parquet(file_path)
    print(f"Loaded {len(df)} records.")

    # Target Range: 76-01 to 98-08
    # Logic: We can split the Alert_No into (Industry, Sub) integers for proper comparison
    # Or simplified: Start >= 76 and Start <= 98?
    # User said "76-01부터 98-08".
    # Let's try to be precise.

    start_tuple = (76, 1)
    end_tuple = (98, 8)

    updated_count = 0
    
    for idx, row in df.iterrows():
        alert_no = str(row['Alert_No'])
        parts = alert_no.split('-')
        
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            industry = int(parts[0])
            sub = int(parts[1])
            
            current_tuple = (industry, sub)
            
            # Check if within range
            if start_tuple <= current_tuple <= end_tuple:
                df.at[idx, 'IsCollect'] = False
                updated_count += 1
    
    print(f"Updated {updated_count} records based on numeric range.")
    
    # 2. Logic: Green List Only -> IsCollect = False
    # If Has_Green_List is True AND (Red is False AND Yellow is False)
    green_only_count = 0
    for idx, row in df.iterrows():
        # Already disabled? Skip to avoid double counting or just re-assert
        if row['IsCollect'] == False:
            continue
            
        has_green = bool(row.get('Has_Green_List', False))
        has_red = bool(row.get('Has_Red_List', False))
        has_yellow = bool(row.get('Has_Yellow_List', False))
        
        if has_green and not has_red and not has_yellow:
            df.at[idx, 'IsCollect'] = False
            green_only_count += 1
            
    print(f"Updated {green_only_count} records based on Green List Only.")
    updated_count += green_only_count
    
    # Save back
    df.to_parquet(file_path, engine='pyarrow', compression='snappy')
    print("Saved updated parquet file.")
    
    # Verify
    verification = df[df['IsCollect'] == False]
    print(f"Total False records now: {len(verification)}")
    if not verification.empty:
        print("Sample disabled alerts:", verification['Alert_No'].head(5).tolist())

if __name__ == "__main__":
    update_fda_master()
