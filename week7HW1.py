"""Load the Olist reviews CSV safely, save it as JSONL and Parquet, and compare sizes."""

import os
from pathlib import Path

import pandas as pd

ROOT = Path("/Users/meemee.maung/olist-star-schema")
CSV_PATH = ROOT / "data" / "raw" / "olist_order_reviews_dataset.csv"
OUT_DIR = ROOT / "data" / "reviews_out"

DATE_COLS = ["review_creation_date", "review_answer_timestamp"]
ID_COLS = {"review_id": "string", "order_id": "string"}


def mb(path: Path) -> float:
      """Return the file size of ``path`` in megabytes, rounded to two decimals."""
      return round(os.path.getsize(path) / 1e6, 2)


OUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(
      CSV_PATH,
      encoding="utf-8",
      dtype=ID_COLS,
      parse_dates=DATE_COLS,
  )
print(df.shape)
print(df.dtypes)

jsonl_path = OUT_DIR / "reviews.jsonl"
parquet_path = OUT_DIR / "reviews.parquet"
  
df.to_json(jsonl_path, orient="records", lines=True,
             date_format="iso", force_ascii=False)
df.to_parquet(parquet_path, index=False)

back = pd.read_json(jsonl_path, lines=True, dtype=ID_COLS)
for col in DATE_COLS:
      back[col] = pd.to_datetime(back[col])
print(back.shape)
print(back.dtypes)

print("csv     MB:", mb(CSV_PATH))
print("jsonl   MB:", mb(jsonl_path))
print("parquet MB:", mb(parquet_path))

print("same shape:", back.shape == df.shape)
print("same ids:", back["review_id"].equals(df["review_id"]))
print("same dates:", back["review_creation_date"].equals(df["review_creation_date"]))
print("same comments:", back["review_comment_message"].fillna("").equals(
      df["review_comment_message"].fillna("")))