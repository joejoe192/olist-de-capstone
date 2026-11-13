import shutil
from pathlib import Path
  
import pandas as pd

ROOT = Path("/Users/meemee.maung/olist-star-schema")
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "lab2"

orders    = pd.read_csv(RAW / "olist_orders_dataset.csv")
items     = pd.read_csv(RAW / "olist_order_items_dataset.csv")
customers = pd.read_csv(RAW / "olist_customers_dataset.csv")

df = orders.merge(items, on="order_id").merge(customers, on="customer_id")
df["purchase_month"] = (pd.to_datetime(df["order_purchase_timestamp"])
                          .dt.strftime("%Y-%m"))

shutil.rmtree(OUT, ignore_errors=True)

for col in ["purchase_month", "customer_state", "customer_city"]:
      path = OUT / f"by_{col}"
      df.to_parquet(path, partition_cols=[col], compression="zstd", max_partitions=5000,index=False)
      files = list(path.rglob("*.parquet"))
      size_mb = sum(f.stat().st_size for f in files) / 1e6
      print(f"{col:<16} files: {len(files):>5}  size: {size_mb:>6.1f} MB")
  
jan = pd.read_parquet(OUT / "by_purchase_month",
                        filters=[("purchase_month", "==", "2018-01")])
print("Read back one month:", len(jan), "rows")