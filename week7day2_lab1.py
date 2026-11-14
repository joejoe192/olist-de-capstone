"""Lab 1: fetch the BRL to USD rate, add price_usd to Olist order items, save as Parquet."""

from pathlib import Path

import pandas as pd
import requests
import truststore

import config

truststore.inject_into_ssl()

ROOT = Path("/Users/meemee.maung/olist-star-schema")
ITEMS_CSV = ROOT / "data" / "raw" / "olist_order_items_dataset.csv"
OUT_PATH = ROOT / "data" / "order_items_usd.parquet"


def get_brl_to_usd() -> float:
    """Return the current USD value of 1 BRL, raising RuntimeError with the HTTP code on failure."""
    base = config.EXCHANGE_API_URL.strip().rstrip("/")
    url = f"{base}/{config.EXCHANGE_API_KEY}/latest/BRL"
    r = requests.get(url, timeout=10)
    try:
        data = r.json()
    except ValueError:
        raise RuntimeError(f"HTTP {r.status_code} {r.reason} (response was not JSON)") from None
    if data["result"] != "success":
        raise RuntimeError(f"HTTP {r.status_code}: {data['error-type']}")
    return data["conversion_rates"]["USD"]


def main() -> None:
    """Fetch the rate, add price_usd to order items, write Parquet and verify the column."""
    try:
        rate = get_brl_to_usd()
    except RuntimeError as exc:
        print("API error:", exc)
        raise SystemExit(1)
    except requests.RequestException as exc:
        print("Request failed:", type(exc).__name__)
        raise SystemExit(1)

    print("1 BRL =", rate, "USD")

    items = pd.read_csv(ITEMS_CSV)
    items["price_usd"] = (items["price"] * rate).round(2)
    items.to_parquet(OUT_PATH, index=False)

    check = pd.read_parquet(OUT_PATH)
    print("price_usd column present:", "price_usd" in check.columns, check.shape)
    print(check[["price", "price_usd"]].head(3))


if __name__ == "__main__":
    main()
