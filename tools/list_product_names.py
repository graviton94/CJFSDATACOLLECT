
import pandas as pd
from pathlib import Path

def list_product_names():
    path = Path("data/reference/product_code_master.parquet")
    if not path.exists():
        print("Master file not found.")
        return

    df = pd.read_parquet(path)
    if 'KOR_NM' in df.columns:
        unique_names = sorted(df['KOR_NM'].dropna().unique())
        print(f"Found {len(unique_names)} unique names.")
        # Print all of them to help me map
        for name in unique_names:
            print(name)
    else:
        print("Column KOR_NM not found.")

if __name__ == "__main__":
    list_product_names()
