import pandas as pd
from pathlib import Path

def apply_csv_updates():
    parquet_path = Path("data/reference/fda_list_master.parquet")
    csv_path = Path("data/fda_list_master(updated).csv")
    
    if not parquet_path.exists():
        print(f"Error: Parquet file not found at {parquet_path}")
        return
        
    if not csv_path.exists():
        print(f"Error: Updated CSV file not found at {csv_path}")
        return

    # Load both
    df_parquet = pd.read_parquet(parquet_path)
    try:
        df_csv = pd.read_csv(csv_path, dtype=str) # Load as string to preserve formatting, treat numbers carefully
    except Exception as e:
        # Fallback for encoding if needed, though default usually works
        print(f"CSV Load Error: {e}")
        return

    print(f"Loaded Parquet: {len(df_parquet)} rows")
    print(f"Loaded CSV: {len(df_csv)} rows")
    
    # Columns to update
    target_cols = [
        'IsCollect', 
        'Manual_Hazard_Item', 
        'Manual_Product_Type',
        'Manual_Class_M',
        'Manual_Class_L'
    ]
    
    # Verify CSV has Alert_No
    if 'Alert_No' not in df_csv.columns:
        print("Error: CSV missing 'Alert_No' column.")
        return

    updated_count = 0
    
    # Create a map from CSV for faster lookup
    # Need to handle potential duplicates or just take first? Assuming unique Alert_No
    csv_map = df_csv.set_index('Alert_No').to_dict('index')
    
    for idx, row in df_parquet.iterrows():
        alert = str(row['Alert_No'])
        if alert in csv_map:
            changes_made = False
            new_data = csv_map[alert]
            
            # Update specific columns
            for col in target_cols:
                if col in new_data:
                    new_val = new_data[col]
                    
                    # Handle IsCollect specifically (bool)
                    if col == 'IsCollect':
                        # Convert CSV string/bool to consistent bool
                        if isinstance(new_val, str):
                            val_lower = new_val.lower()
                            bool_val = val_lower in ['true', '1', 't', 'yes']
                        else:
                            bool_val = bool(new_val)
                            
                        if df_parquet.at[idx, col] != bool_val:
                            df_parquet.at[idx, col] = bool_val
                            changes_made = True
                    else:
                        # Handle Strings (Treat 'nan', empty as None)
                        if pd.isna(new_val) or str(new_val).lower() in ['nan', 'none', '']:
                            update_val = None
                        else:
                            update_val = str(new_val).strip()
                            
                        if df_parquet.at[idx, col] != update_val:
                            df_parquet.at[idx, col] = update_val
                            changes_made = True
                            
            if changes_made:
                updated_count += 1
                
    print(f"Successfully updated {updated_count} records from CSV.")
    
    # Save Back
    df_parquet.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
    print("Saved updated parquet file.")
    
    # Verify a few
    print("Sample updated records:")
    print(df_parquet[df_parquet['Alert_No'].isin(list(csv_map.keys())[:5])][['Alert_No'] + target_cols])

if __name__ == "__main__":
    apply_csv_updates()
