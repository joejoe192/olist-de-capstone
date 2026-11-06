import pandas as pd
from datetime import datetime

s = "Delivered"
s.lower()          # returns "delivered" ... and it is lost
s = s.lower()      # keep it: assign the result

t = (1, 2)
#t[0] = 9           # TypeError: tuple does not support item assignment
print(t[0])        # 1
print(s)
print(t)          # (1, 2)
print(t[0])       # 1        — တစ်ခုတည်း ထုတ်
print(t[1])       # 2
print(*t)         # 1 2      — * က tuple ကို ခွဲပြီး ကွင်းမပါ ထုတ်

x = [1, 2]; y = [1, 2]
x == y        # True   same value
x is y        # False  different objects
print(x)
print(x==y)
print(x is y)
value = None
if value is None:      # the standard way
    value = 0.05
    print(value)
# 1. df ကို အရင်ဖန်တီး
df = pd.DataFrame({
    "order_id": ["o1", "o2", None],
    "order_status": ["delivered", "shipped", "deliverd"],
    "order_purchase_timestamp": ["2018-01-01 10:00:00", "2018-01-02 11:00:00", "2018-01-03 12:00:00"],
})
RENAME_MAP = {"order_purchase_timestamp": "purchased_at"}
df = df.rename(columns=RENAME_MAP)        # dict: old -> new

CASTS = {"delivery_days": "INTEGER"}
CASTS["delivery_days"]                    # "INTEGER"
CASTS.get("order_id", None)               # None, no KeyError

VALID = {"delivered", "shipped"}
"delivered" in VALID                      # True, very fast
{"delivered", "deliverd"} - VALID         # {'deliverd'}
print(df)
print(df.rename(columns=RENAME_MAP))
print(VALID)
days = [7, 12, 25, 3] #list ရိုးရိုး
late = [d for d in days if d > 10]          # [12, 25] #list comprehension	ရှိပြီးသား list ကနေ ရွေးပြီး list အသစ် ဆောက်

lower = {c: c.lower() for c in ["A", "B"]}  # {'A': 'a', 'B': 'b'} #dict comprehension list ကနေ key: value တွဲပြီး dict ဆောက်	

print(late)
print(lower)
def calc_delivery_days(purchase, delivered):
    """Return days between purchase and delivery."""
    days = (delivered - purchase).days
    return days

start = datetime(2018, 1, 1)
end = datetime(2018, 1, 8)
result = calc_delivery_days(start, end)
print(result)   # 7
def greet(name, greeting="Hello"):        # greeting = default value
    return f"{greeting}, {name}!"

print(greet("John"))                       # Hello, Hnin!      (positional)
print(greet("John", greeting="Hi"))        # Hi, Hnin!         (keyword)
print(greet(greeting="Mingalaba", name="Aung"))   # Mingalaba, Aung!  (keyword, အစဉ်မလိုက်)

count = 10              # global

def f():
    count = 1           # local: a different variable
    return count

def g():
    return count * 2    # reads the global

f(), count, g()         # (1, 10, 20)
print(f(), count, g())

rejected = []            # module-level

def transform(run):
    rejected.append(run)
    return len(rejected)

transform("run1")        # 1
print(rejected)          # ['run1'] 
transform("run2")        # 2  run1 still there!

print(rejected)          # ['run1', 'run2']
mask = df["order_id"].isna() 
def split_bad(df, mask, reason):
    bad = df[mask].assign(reject_reason=reason)
    good = df[~mask].copy()
    return good, bad          # no hidden state

good, bad = split_bad(df, mask, "missing id")

print(good)
print(bad)
import logging, sys

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)
print("small_test.py: __name__ =", __name__) 
def extract():   log.info("extract"); return [1, 2, 3]
def transform(d): log.info("transform"); return [x * 10 for x in d]
def load(d):     log.info(f"load {len(d)} rows")

def main():
    try:
        load(transform(extract()))
    except Exception:
        log.exception("ETL failed"); sys.exit(1)

if __name__ == "__main__":
    main()

def r(x):
    try:
        v = int(x)
    except ValueError:
        return "bad"
    else:
        return f"ok {v}"
    raise

#print(r("5"))   # prints: finally 5   returns: ok 5
print(r("x"))   # prints: finally x   returns: bad
a = [1]; b = a;print("first",a); b.append(2); print(a)
s = " Delivered "; s.strip(); print(s)

df = pd.DataFrame({
    "order_id":     ["o1",  "o2",  "o3"],
    "approved_at":  ["x",   None,  "y"],
    "delivered_at": ["a",   "b",   None],
})

df.dropna()                          # o1 ပဲ ကျန်
df.dropna(subset=["delivered_at"])   # o1, o2 ကျန်
print(df.dropna())

print(pd.to_numeric(["58.90", "abc", "13"], errors="coerce")   )
# [58.9, nan, 13.0]

pd.Series([1, None]).astype(int)      # IntCastingNaNError
print(pd.Series(["1", None]).astype("Int64")) # [1, <NA>]


