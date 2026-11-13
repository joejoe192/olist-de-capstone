"""Compare reading 2 columns from Parquet against reading the full CSV."""
import time
from pathlib import Path
  
import pandas as pd

ROOT = Path("/Users/meemee.maung/olist-star-schema")
CSV_PATH = ROOT / "data" / "raw" / "olist_order_reviews_dataset.csv"
OUT_DIR = ROOT / "data" / "reviews_out"
PARQUET_PATH = OUT_DIR / "reviews.parquet"

COLUMNS = ["order_id", "review_score"]
REPEATS = 5


def best_time(func) -> float:
      """Run ``func`` REPEATS times and return the fastest time in seconds."""
      times = []
      for _ in range(REPEATS):
          start = time.perf_counter()
          func()
          times.append(time.perf_counter() - start)
      return round(min(times), 4)


if not PARQUET_PATH.exists():
      OUT_DIR.mkdir(parents=True, exist_ok=True)
      pd.read_csv(CSV_PATH).to_parquet(PARQUET_PATH, index=False)

csv_full = best_time(lambda: pd.read_csv(CSV_PATH))
csv_cols = best_time(lambda: pd.read_csv(CSV_PATH, usecols=COLUMNS))
parquet_full = best_time(lambda: pd.read_parquet(PARQUET_PATH))
parquet_cols = best_time(lambda: pd.read_parquet(PARQUET_PATH, columns=COLUMNS))

print(f"CSV full (7 columns)        : {csv_full} s")
print(f"CSV usecols (2 columns)     : {csv_cols} s")
print(f"Parquet full (7 columns)    : {parquet_full} s")
print(f"Parquet columns (2 columns) : {parquet_cols} s")
print(f"Parquet 2 columns is {csv_full / parquet_cols:.1f}x faster than full CSV")

result = pd.read_parquet(PARQUET_PATH, columns=COLUMNS)
print(result.columns.tolist(), result.shape)