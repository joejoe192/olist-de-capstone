import sys

def main():
    try:
        orders, customers = extract()
        result = transform(orders, customers)
        load(result)
    except Exception:
        log.exception("ETL failed")  # full traceback
        sys.exit(1)       # scheduler sees FAILED
