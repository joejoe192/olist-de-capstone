import os
import shutil
import pandas as pd
RAW = "/Users/meemee.maung/olist-star-schema/data/raw/"

orders = pd.read_csv(RAW + "olist_orders_dataset.csv")
items = pd.read_csv(RAW + "olist_order_items_dataset.csv")
customers = pd.read_csv(RAW + "olist_customers_dataset.csv")
df = orders.merge(items, on="order_id").merge(customers, on="customer_id")


def count_files(folder):
      """Return the number of files under ``folder``."""
      return sum(len(files) for _, _, files in os.walk(folder))


shutil.rmtree("by_state", ignore_errors=True)
shutil.rmtree("by_city", ignore_errors=True)
  
df.to_parquet("by_state", partition_cols=["customer_state"], index=False)
df.to_parquet("by_city", partition_cols=["customer_city"], index=False,
                max_partitions=5000)

print("by_state files:", count_files("by_state"))
print("by_city files: ", count_files("by_city"))