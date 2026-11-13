import pandas as pd

orders = pd.read_csv(
    "/Users/meemee.maung/olist-star-schema/data/raw/olist_orders_dataset.csv",
    encoding="utf-8",
    dtype={"order_id": "string",
           "customer_id": "string",
           "order_status": "category"},
    parse_dates=["order_purchase_timestamp",
                 "order_approved_at",
                 "order_estimated_delivery_date"],
)

customers = pd.read_csv(
    "/Users/meemee.maung/olist-star-schema/data/raw/olist_customers_dataset.csv",
    dtype={"customer_zip_code_prefix": "string"},
)
orders.info()
