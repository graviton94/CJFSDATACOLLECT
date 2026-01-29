import pandas as pd
from pathlib import Path

def verify():
    path = Path("data/reference/fda_list_master.parquet")
    if not path.exists():
        print("Error: Parquet file not found.")
        return

    df = pd.read_parquet(path)
    print(f"Total rows (Parquet): {len(df)}")
    
    # Also load CSV
    csv_path = Path("data/reference/fda_list_master.csv")
    if csv_path.exists():
        df_csv = pd.read_csv(csv_path)
        print(f"Total rows (CSV): {len(df_csv)}")
    else:
        print("Error: CSV file not found.")
        return

    print("-" * 30)
    
    # Check 98-01 in Parquet
    alert_9801 = df[df['Alert_No'] == '98-01']
    if not alert_9801.empty:
        val = alert_9801['IsCollect'].values[0]
        print(f"[Parquet] Alert 98-01 IsCollect: {val} (Type: {type(val)})")
    
    # Check 98-01 in CSV
    alert_9801_csv = df_csv[df_csv['Alert_No'] == '98-01']
    if not alert_9801_csv.empty:
        val_csv = alert_9801_csv['IsCollect'].values[0]
        print(f"[CSV]     Alert 98-01 IsCollect: {val_csv} (Type: {type(val_csv)})")

    print("-" * 30)
    # Check 12-10
    alert_1210 = df[df['Alert_No'] == '12-10']
    if not alert_1210.empty:
        print(f"Alert 12-10 Found in Parquet.")
        # Fix column name Date -> Publish_Date
        cols = ['Alert_No', 'Title', 'Publish_Date', 'IsCollect']
        print(alert_1210[[c for c in cols if c in df.columns]].iloc[0])
    else:
        print("Alert 12-10 NOT FOUND")

if __name__ == "__main__":
    verify()
