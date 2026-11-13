from pathlib import Path

import pandas as pd

ROOT = Path("/Users/meemee.maung/olist-star-schema")
CSV_PATH = ROOT / "data" / "raw" / "olist_orders_dataset.csv"
OUT_DIR = ROOT / "data" / "sales_by_month"

df = pd.read_csv(CSV_PATH, parse_dates=["order_purchase_timestamp"])
df["purchase_month"] = df["order_purchase_timestamp"].dt.strftime("%Y-%m")

df.to_parquet(OUT_DIR, partition_cols=["purchase_month"], index=False)
# Read everything back:
all_sales = pd.read_parquet(OUT_DIR)
#print(all_sales["purchase_month"].unique())
 #Read only one month (the main benefit):order_purchase_timestamp starting with 2018-01.
jan = pd.read_parquet(OUT_DIR, filters=[("purchase_month", "==", "2018-01")])
print(len(jan))