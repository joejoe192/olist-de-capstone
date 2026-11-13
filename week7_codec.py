import os, time #gets file information, and time measures how long things take.
import pandas as pd #reads and writes tables of data.
mb = lambda p: round(os.path.getsize(p) / 1e6, 1) #is a small helper. It takes a file path, gets its size in bytes with os.path.getsize, divides by 1,000,000 to getmegabytes, and rounds to 1 decimal.

 
csv = "/Users/meemee.maung/olist-star-schema/data/raw/olist_geolocation_dataset.csv"
df = pd.read_csv(csv)
df.to_csv("geo.csv.gz", index=False)   #Save as compressed CSV       # .gz means gzip
print("csv", mb(csv), "MB | csv.gz", mb("geo.csv.gz"), "MB")
 
for codec in [None, "snappy", "zstd", "gzip"]: #Loop over compression codecs
    path = f"geo_{codec}.parquet" #Each codec gets its own file.
    write_start = time.time(); df.to_parquet(path, compression=codec, index=False)#Time the write
    w = round(time.time() - write_start, 2) #- Subtract the start time from the current time to get seconds spent writing (w).
    read_start = time.time(); pd.read_parquet(path) #Time the read
    r = round(time.time() - read_start, 2)
    print(codec, mb(path), "MB | write", w, "s | read", r, "s") 