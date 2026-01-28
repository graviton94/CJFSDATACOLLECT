import pandas as pd
from pathlib import Path

def clean_overrides():
    file_path = Path("data/reference/fda_list_master.parquet")
    if not file_path.exists():
        print("File not found.")
        return

    df = pd.read_parquet(file_path)
    print(f"Loaded {len(df)} records.")
    
    # Condition: IsCollect == False (or None, treating as skip? schema default is True, but check explicitly)
    # Convert properly to boolean just in case
    df['IsCollect'] = df['IsCollect'].fillna(True).astype(bool)
    
    mask_skip = df['IsCollect'] == False
    count = mask_skip.sum()
    
    print(f"Found {count} records to skip (IsCollect=False). Clearing their manual overrides...")
    
    # Set Manual cols to None where mask is True
    df.loc[mask_skip, 'Manual_Hazard_Item'] = None
    df.loc[mask_skip, 'Manual_Product_Type'] = None
    
    # Save
    df.to_parquet(file_path, engine='pyarrow', compression='snappy')
    print("Cleanup complete and saved.")

if __name__ == "__main__":
    clean_overrides()
