import pandas as pd
 
orders    = pd.read_csv("/Users/meemee.maung/olist-star-schema/data/raw/olist_orders_dataset.csv")
items     = pd.read_csv("/Users/meemee.maung/olist-star-schema/data/raw/olist_order_items_dataset.csv")
customers = pd.read_csv("/Users/meemee.maung/olist-star-schema/data/raw/olist_customers_dataset.csv")
 
df = orders.merge(items, on="order_id").merge(customers, on="customer_id") #join them into one table 
df["purchase_month"] = (pd.to_datetime(df["order_purchase_timestamp"])
                        .dt.strftime("%Y-%m")) #create partition column
df.to_parquet("sales_by_month",
              partition_cols=["purchase_month"],
              compression="zstd", index=False) #Write partitioned Parquet
 
result=pd.read_parquet("sales_by_month",
    filters=[("purchase_month", "==", "2018-03")]) #Read back one month

 
print(df.shape)                       # about 112,000 rows
print(df["purchase_month"].head(3))   # values look like 2017-10
print(result)
