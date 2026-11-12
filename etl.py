"""ETL: Olist order items (CSV) + exchange rates (API) -> Parquet file + Postgres table.

Extract : exchange rates from the API (with retries) and order items from a CSV file
Transform: add USD and EUR prices, then check the data
Load    : write a Parquet file and upsert into Postgres (safe to run again)
"""
import logging
import random
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from config import DATABASE_URL, EXCHANGE_API_KEY, EXCHANGE_API_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("etl")

ITEMS_CSV = "data/olist_order_items_dataset.csv"
PARQUET_OUT = "order_items_fx.parquet"
TABLE = "order_items_fx"
KEY = ["order_id", "order_item_id"]
RETRY_STATUS = (429, 500, 502, 503, 504)


# ---------------------------------------------------------------- helpers
class PermanentError(RuntimeError):
    """An error that retrying cannot fix (wrong key, bad request, ...)."""


def get_with_retry(url, params=None, headers=None, max_attempts=5):
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
        except (requests.ConnectionError, requests.Timeout):
            r = None                                          # network problem: retry
        if r is not None and r.status_code < 400:
            return r                                          # success
        if r is not None and r.status_code not in RETRY_STATUS:
            try:
                detail = r.json().get("error-type", "")       # the API explains the error
            except ValueError:
                detail = ""
            # Do not put the URL in the message: for this API it contains the key.
            raise PermanentError(f"HTTP {r.status_code} {detail}".strip())
        if attempt == max_attempts:
            raise RuntimeError(f"gave up after {attempt} attempts")
        wait = 2 ** (attempt - 1) + random.uniform(0, 0.5)    # backoff + jitter
        if r is not None and "Retry-After" in r.headers:
            wait = int(r.headers["Retry-After"])              # the server says how long
        log.warning("attempt %d failed, waiting %.1fs", attempt, wait)
        time.sleep(wait)


# ---------------------------------------------------------------- extract
def extract_rates(base="BRL"):
    url = f"{EXCHANGE_API_URL}/{EXCHANGE_API_KEY}/latest/{base}"
    data = get_with_retry(url).json()
    if data["result"] != "success":
        raise RuntimeError(f"exchange API error: {data['error-type']}")
    rates = data["conversion_rates"]
    return {
        "usd_per_brl": rates["USD"],
        "eur_per_brl": rates["EUR"],
        "rate_time_utc": data["time_last_update_utc"],
    }


def extract_items():
    return pd.read_csv(
        ITEMS_CSV,
        dtype={"order_id": "string", "product_id": "string", "seller_id": "string"},
        parse_dates=["shipping_limit_date"],
    )


# ---------------------------------------------------------------- transform
def transform(items, rates):
    df = items.copy()
    df["price_usd"] = (df["price"] * rates["usd_per_brl"]).round(2)
    df["price_eur"] = (df["price"] * rates["eur_per_brl"]).round(2)
    df["freight_usd"] = (df["freight_value"] * rates["usd_per_brl"]).round(2)
    df["freight_eur"] = (df["freight_value"] * rates["eur_per_brl"]).round(2)
    df["usd_per_brl"] = rates["usd_per_brl"]
    df["eur_per_brl"] = rates["eur_per_brl"]
    df["rate_time_utc"] = rates["rate_time_utc"]
    df["loaded_at"] = datetime.now(timezone.utc)
    return df


def validate(df, expected_rows):
    problems = []
    if len(df) != expected_rows:
        problems.append(f"row count changed: {expected_rows} -> {len(df)}")
    if df[KEY].isna().any().any():
        problems.append("missing order_id or order_item_id")
    if df.duplicated(KEY).any():
        problems.append("duplicate (order_id, order_item_id) rows")
    if (df["price"] < 0).any():
        problems.append("negative prices")
    if problems:
        raise ValueError("validation failed: " + "; ".join(problems))


# ---------------------------------------------------------------- load
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    order_id            text         NOT NULL,
    order_item_id       integer      NOT NULL,
    product_id          text,
    seller_id           text,
    shipping_limit_date timestamp,
    price               numeric(12,2),
    freight_value       numeric(12,2),
    price_usd           numeric(12,2),
    price_eur           numeric(12,2),
    freight_usd         numeric(12,2),
    freight_eur         numeric(12,2),
    usd_per_brl         numeric(14,6),
    eur_per_brl         numeric(14,6),
    rate_time_utc       text,
    loaded_at           timestamptz,
    PRIMARY KEY (order_id, order_item_id)
)
"""


def load_parquet(df):
    df.to_parquet(PARQUET_OUT, compression="zstd", index=False)
    log.info("wrote %s", PARQUET_OUT)


def load_postgres(df, engine):
    cols = list(df.columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in KEY)
    stg = f"stg_{TABLE}"
    with engine.begin() as conn:                              # one transaction: all or nothing
        conn.execute(text(DDL))
        df.to_sql(stg, conn, if_exists="replace", index=False, chunksize=1000, method="multi")
        conn.execute(text(f"""
            INSERT INTO {TABLE} ({", ".join(cols)})
            SELECT {", ".join(cols)} FROM {stg}
            ON CONFLICT (order_id, order_item_id) DO UPDATE SET {updates}
        """))
        conn.execute(text(f"DROP TABLE {stg}"))
    log.info("upserted %d rows into %s", len(df), TABLE)


# ---------------------------------------------------------------- run
def main():
    started = time.time()
    rates = extract_rates()                                   # API first: fail before touching anything
    log.info("rates: 1 BRL = %s USD, %s EUR", rates["usd_per_brl"], rates["eur_per_brl"])
    items = extract_items()
    log.info("read %d order items", len(items))
    df = transform(items, rates)
    validate(df, expected_rows=len(items))                    # stop here if the data looks wrong
    load_parquet(df)
    load_postgres(df, create_engine(DATABASE_URL))
    log.info("done in %.1fs", time.time() - started)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("ETL failed: %s", e)
        sys.exit(1)                                           # tells Airflow / cron the run failed
