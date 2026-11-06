import pandas as pd
from pathlib import Path
import numpy as np

#slide 6 test
orders = pd.read_csv("/Users/meemeemaung/Documents/GitHub/olist-de-capstone/data/raw/olist_orders_dataset.csv", dtype=str)

# print(orders.shape )         # (99441, 8)  rows, columns
# print(orders.head())         # first 5 rows
# print(orders.info())         # columns, dtypes, non-null counts
# print(orders.isna().sum())   # nulls per column
# print(orders["order_status"].value_counts())

#slide 7 test
path = Path("/Users/meemeemaung/Documents/GitHub/olist-de-capstone/data/raw/olist_orders_dataset.csv")
orders = pd.read_csv(path, dtype=str, usecols=["order_id", "order_status","customer_id","order_delivered_customer_date","order_purchase_timestamp"], encoding="utf-8")    
# print(orders.head())

#slide 11 test
d = pd.Series([7, pd.NaT, 12,np.nan,pd.NA,None])   # [7, None, 12]
d.mean()    # 9.5
d.sum()     # 19.0
d.count()   # 2
print(len(d))      # 3
print(d.count())  # 2

#slide 13 test
pd.to_numeric(["58.90", "abc", "13"], errors="coerce")
# [58.9, nan, 13.0]
print(pd.to_numeric(["58.90", "abc", "13"],errors="coerce")) # [58.9, nan, 13.0]

#pd.Series([1, None]).astype(int)      # IntCastingNaNError
pd.Series(["1", None]).astype("Int64") # [1, <NA>]

print(pd.Series(["1", None]).astype("Int64")) # [1, <NA>]

#slide 15 test
for col in ["order_purchase_timestamp", "order_delivered_customer_date"]:
    orders[col] = pd.to_datetime(orders[col], format='%d/%m/%Y %H:%M', errors="coerce")
orders["delivery_days"] = (
    orders["order_delivered_customer_date"]
    - orders["order_purchase_timestamp"] ).dt.days
print(orders["delivery_days"].head)
