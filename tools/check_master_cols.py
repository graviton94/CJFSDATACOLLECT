
import pandas as pd
from pathlib import Path

def check_cols():
    path = Path("data/reference/product_code_master.parquet")
    if path.exists():
        df = pd.read_parquet(path)
        print("Columns:", df.columns.tolist())
        print("Sample Row:", df.iloc[0].to_dict())
    else:
        print("File not found")

if __name__ == "__main__":
    check_cols()
