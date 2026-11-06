"""
Week 5 demo: a small ETL pipeline with Python + Pandas.

Extract   -> read the raw Olist orders CSV
Transform -> clean text, convert types, check quality, rename, add columns
Load      -> upsert into Postgres (staging.stg_orders), one row per order_id

Run (from the project root):
    source .venv/bin/activate          (Mac/Linux)  |  .venv\\Scripts\\activate  (Windows)
    pip install -r requirements.txt
    python scripts/ETL_day2.py

Running it again is safe: existing orders are updated, never duplicated.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------- Config ----------
BASE_DIR = Path(__file__).resolve().parent.parent   # scripts/ -> project root
load_dotenv(BASE_DIR / ".env")                      # read .env into environment variables

DATA_DIR = BASE_DIR / os.getenv("DATA_DIR", "data/raw")
SOURCE_FILE = "olist_orders_dataset.csv"
ORDERS_FILE = DATA_DIR / SOURCE_FILE

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_URL = (f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD or '')}"  # quote_plus: @ # in password stay safe
          f"@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# one ID per run, saved on every row we load (lineage)
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

DATE_FORMAT = "%d/%m/%Y %H:%M"   # our CSV, e.g. 24/7/2018 20:41
# original Kaggle file (e.g. 2017-10-02 10:56:33) needs "%Y-%m-%d %H:%M:%S"

DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
VALID_STATUSES = {"created", "approved", "invoiced", "processing",
                  "shipped", "delivered", "canceled", "unavailable"}

# source column names -> our staging names
RENAME_MAP = {
    "order_purchase_timestamp": "purchased_at",
    "order_approved_at": "approved_at",
    "order_delivered_carrier_date": "shipped_at",
    "order_delivered_customer_date": "delivered_at",
    "order_estimated_delivery_date": "estimated_delivery_at",
}

# ---------- SQL ----------
CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id               TEXT PRIMARY KEY,
    customer_id            TEXT        NOT NULL,
    order_status           TEXT        NOT NULL,
    purchased_at           TIMESTAMP   NOT NULL,
    approved_at            TIMESTAMP,
    shipped_at             TIMESTAMP,
    delivered_at           TIMESTAMP,
    estimated_delivery_at  TIMESTAMP,
    delivery_days          INTEGER,
    is_late                BOOLEAN,
    _source_file           TEXT        NOT NULL,
    _run_id                TEXT        NOT NULL,
    _loaded_at             TIMESTAMP   NOT NULL DEFAULT now(),
    _updated_at            TIMESTAMP   NOT NULL DEFAULT now()
);
"""

UPSERT_SQL = """
INSERT INTO staging.stg_orders (
    order_id, customer_id, order_status, purchased_at, approved_at,
    shipped_at, delivered_at, estimated_delivery_at, delivery_days, is_late,
    _source_file, _run_id
)
SELECT
    order_id, customer_id, order_status, purchased_at, approved_at,
    shipped_at, delivered_at, estimated_delivery_at,
    CAST(delivery_days AS INTEGER), CAST(is_late AS BOOLEAN),
    _source_file, _run_id
FROM staging._tmp_orders
ON CONFLICT (order_id) DO UPDATE SET
    customer_id           = EXCLUDED.customer_id,  -- EXCLUDED = the new row from the temp table
    order_status          = EXCLUDED.order_status,
    purchased_at          = EXCLUDED.purchased_at,
    approved_at           = EXCLUDED.approved_at,
    shipped_at            = EXCLUDED.shipped_at,
    delivered_at          = EXCLUDED.delivered_at,
    estimated_delivery_at = EXCLUDED.estimated_delivery_at,
    delivery_days         = EXCLUDED.delivery_days,
    is_late               = EXCLUDED.is_late,
    _source_file          = EXCLUDED._source_file,
    _run_id               = EXCLUDED._run_id,
    _updated_at           = now();
"""

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
log = logging.getLogger(__name__) # logger for this module


# ---------- Extract ----------
def extract():
    log.info(f"Extract: reading {ORDERS_FILE.name}") # method of the Logger object provided by Python's built-in logging module.
    try:
        # read every column as text: we decide the types ourselves in transform
        orders = pd.read_csv(ORDERS_FILE, dtype=str)
    except FileNotFoundError as e:
        log.error(f"File not found: {e.filename}")
        raise

    log.info(f"Read {len(orders):,} orders")
    return orders


# ---------- Transform ----------
def drop_bad_rows(df, mask, reason):
    """Remove rows where mask is True and log how many."""
    count = int(mask.sum()) # True counts as 1
    if count:
        log.warning(f"Dropped {count} rows: {reason}") #
    return df[~mask].copy()                  # .copy(): no SettingWithCopyWarning later


def transform(orders):
    log.info("Transform: cleaning, checking, renaming, adding columns")
    df = orders.copy()

    # 1. Clean text: remove spaces, lowercase the status
    for col in df.columns:
        df[col] = df[col].str.strip()
    df["order_status"] = df["order_status"].str.lower()

    # 2. Text to dates (bad values become NaT instead of crashing)
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format=DATE_FORMAT, errors="coerce")

    # 3. Quality checks: drop rows that break the table rules
    df = drop_bad_rows(df, df["order_id"].isna(), "missing order_id")
    df = drop_bad_rows(df, df["order_id"].duplicated(), "duplicate order_id")
    df = drop_bad_rows(df, df["customer_id"].isna(), "missing customer_id")
    df = drop_bad_rows(df, ~df["order_status"].isin(VALID_STATUSES), "unknown order_status")
    df = drop_bad_rows(df, df["order_purchase_timestamp"].isna(), "invalid purchase date")
    # approved_at may be empty (orders canceled before approval): that is normal, keep them

    # 4. New columns
    delivered = df["order_delivered_customer_date"]
    df["delivery_days"] = (delivered - df["order_purchase_timestamp"]).dt.days

    is_late = (delivered > df["order_estimated_delivery_date"]).astype("boolean")
    is_late[delivered.isna()] = pd.NA          # not delivered yet = unknown, not False
    df["is_late"] = is_late

    # 5. Rename to our naming convention
    df = df.rename(columns=RENAME_MAP)

    # 6. Lineage: where each row came from and which run loaded it
    df["_source_file"] = SOURCE_FILE
    df["_run_id"] = RUN_ID

    df = df[[
        "order_id", "customer_id", "order_status", "purchased_at", "approved_at",
        "shipped_at", "delivered_at", "estimated_delivery_at", "delivery_days",
        "is_late", "_source_file", "_run_id",
    ]] #dataframe with the columns in the order we want

    status_counts = df["order_status"].value_counts().to_dict() #dictionary #Series ကို dictionary ပြောင်းလဲ
    log.info(f"Orders by status: {status_counts}")
    log.info(f"Transform done: {len(df):,} rows")
    return df


# ---------- Load ----------
def load(df):
    log.info("Load: upsert into staging.stg_orders")
    try:
        engine = create_engine(DB_URL)

        # one transaction: everything is saved, or nothing is
        with engine.begin() as conn:
            conn.exec_driver_sql(CREATE_TABLE_SQL)                 # 1. table ready

            df.to_sql("_tmp_orders", conn, schema="staging",       # 2. rows into a temp table
                      if_exists="replace", index=False)

            conn.execute(text(UPSERT_SQL))                         # 3. insert new, update old
            conn.execute(text("DROP TABLE staging._tmp_orders"))   # 4. clean up

            total = conn.execute(text("SELECT COUNT(*) FROM staging.stg_orders")).scalar()
    except Exception as e:
        log.error(f"Load failed: {e}")
        raise

    log.info(f"Upserted {len(df):,} rows (table now has {total:,} rows)")


# ---------- Run the pipeline ----------
def main():
    log.info(f"===== ETL started (run {RUN_ID}) =====")
    try:
        orders = extract()
        result = transform(orders)
        load(result)
    except Exception:
        log.exception("ETL failed")   # logs the full error details
        sys.exit(1)                   # non-zero exit = failure (Airflow uses this later)
    log.info("===== ETL finished successfully =====")


if __name__ == "__main__":
    main()