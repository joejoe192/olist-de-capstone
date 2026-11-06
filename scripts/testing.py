"""
testing.py: try each ETL step one at a time and look at the result.

Run (from the project root):  python scripts/testing.py
"""
from datetime import datetime
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# ---------- Config ----------
BASE_DIR = Path(__file__).resolve().parent.parent   # scripts/ -> project root
load_dotenv(BASE_DIR / ".env")

DATA_DIR = BASE_DIR / os.getenv("DATA_DIR", "data/raw")
SOURCE_FILE = "olist_orders_dataset.csv"
ORDERS_FILE = DATA_DIR / SOURCE_FILE

DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

print("DATA_DIR:", DATA_DIR)
print("ORDERS_FILE:", ORDERS_FILE)

# ---------- Logging ----------
(BASE_DIR / "logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "etl.log"),   # saved to a file
        logging.StreamHandler(),                              # and shown on screen
    ],
)
log = logging.getLogger(__name__)


# ---------- Functions ----------
def extract():
    log.info(f"Extract: reading {ORDERS_FILE.name}")
    try:
        orders = pd.read_csv(ORDERS_FILE, dtype=str)
    except FileNotFoundError as e:
        log.error(f"File not found: {e.filename}")
        raise

    log.info(f"Read {len(orders):,} orders")
    return orders


def drop_bad_rows(df, mask, reason):
    """Remove rows where mask is True and log how many."""
    count = int(mask.sum())            # True counts as 1
    if count:
        log.warning(f"Dropped {count} rows: {reason}")
    return df[~mask].copy()            # .copy(): a separate DataFrame, no warning later


# ---------- Step 1: Extract ----------
orders = extract()                     # call the function once, keep the result

print(orders.shape)                    # (99441, 8)
print(orders.head())                   # first 5 rows
orders.info()                          # all text (object); info() prints by itself
print(orders.isna().sum())             # missing per column

# ---------- Step 2: Clean text ----------
for col in orders.columns:
    orders[col] = orders[col].str.strip()                 # remove spaces

orders["order_status"] = orders["order_status"].str.lower()  # lowercase, like ETL.py
print(orders["order_status"].value_counts())

# ---------- Step 3: Look at a mask ----------
mask = orders["order_approved_at"].isna()                # True = no approval date
print("Missing approved_at:", mask.sum())
print(orders[mask].head())             # rows with no approval date
print(orders[~mask].head())            # all other rows  (~ = NOT)

# ---------- Step 4: Text to dates ----------
# errors="coerce": a value that is not a date becomes NaT (null) instead of an error
for col in DATE_COLUMNS:
    orders[col] = pd.to_datetime(orders[col], errors="coerce")
    print(col, orders[col].dtype, "missing:", orders[col].isna().sum())

print(orders[DATE_COLUMNS].head())     # now 5 datetime columns

# ---------- Step 5: Drop bad rows ----------
# Missing approved_at is normal (orders canceled before approval), so we keep them.
# A missing PURCHASE date is a real error: the table needs it (NOT NULL).
orders = drop_bad_rows(orders, orders["order_purchase_timestamp"].isna(),
                       "invalid purchase date")
print(orders.shape)

# ---------- Step 6: New columns ----------
delivered = orders["order_delivered_customer_date"]
orders["delivery_days"] = (delivered - orders["order_purchase_timestamp"]).dt.days

is_late = (delivered > orders["order_estimated_delivery_date"]).astype("boolean")
is_late[delivered.isna()] = pd.NA      # not delivered yet = unknown
orders["is_late"] = is_late

print(orders[["order_id", "order_purchase_timestamp", "order_delivered_customer_date",
              "order_estimated_delivery_date", "delivery_days", "is_late"]].head())
orders.info()                          # compare with Step 1: the dtypes have changed

# ---------- Step 7: Rename columns ----------
RENAME_MAP = {
    "order_purchase_timestamp":      "purchased_at",
    "order_approved_at":             "approved_at",
    "order_delivered_carrier_date":  "shipped_at",
    "order_delivered_customer_date": "delivered_at",
    "order_estimated_delivery_date": "estimated_delivery_at",
}


orders = orders.rename(columns=RENAME_MAP)
print(orders.head())                   # new column names
print(orders.info())                  # compare with Step 6: the column names have changed
#---------- Step 8: Lineage ----------
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")   # e.g. 20261029_090001

orders["source_file"] = SOURCE_FILE #new column
orders["run_id"] = RUN_ID #new column

print(orders[["source_file", "run_id"]].head())  # new columns
print(orders.info())                  # compare with Step 7: the column names have changed

# ---------- Step 9: Merge (= SQL INNER JOIN) ----------
CUSTOMERS_FILE = DATA_DIR / "olist_customers_dataset.csv"
customers = pd.read_csv(CUSTOMERS_FILE, dtype=str)
print("customers:", customers.shape)
df = orders.merge(
    customers[["customer_id", "customer_state"]],
    on="customer_id", how="inner")
# Check 1: row count must not change after an inner join
print("orders before merge:", len(orders))
print("after merge:        ", len(df))
# Check 2: orders that found no customer (state is empty)
print("no matching customer:", df["customer_state"].isna().sum())
# Look at the result
print(df[["order_id", "customer_id", "customer_state"]].head())
print(df["customer_state"].value_counts().head())
#---------- Step 10: Group by ----------
print(orders[["order_id", "customer_id"]].head())
print(df.groupby("customer_state").agg(
    orders=("order_id", "count"),
    avg_days=("delivery_days", "mean")))
