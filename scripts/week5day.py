import pandas as pd

orders = pd.read_csv("/Users/meemeemaung/Documents/GitHub/olist-de-capstone/data/raw/olist_orders_dataset.csv", dtype=str)

# print(orders.shape  )        # (99441, 8)  rows, columns
# print(orders.head()  )       # first 5 rows
# print(orders.info() )        # columns, dtypes, non-null counts
# print(orders.isna().sum() )   # nulls per column
# print(orders["order_status"].value_counts() )

print(orders.iloc[0, 1] )                      
