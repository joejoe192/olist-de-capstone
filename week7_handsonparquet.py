from pathlib import Path

import pandas as pd

Path("raw").mkdir(exist_ok=True)#creates a folder named raw, and it doesn't fail if the folder already exists.
  
pd.read_csv("/Users/meemee.maung/olist-star-schema/data/raw/olist_orders_dataset.csv").to_parquet("raw/olist_orders_dataset.parquet", index=False)

df = pd.read_parquet("raw/olist_orders_dataset.parquet")
print(df)
print(df.dtypes)