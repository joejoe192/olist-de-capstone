import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def extract():
    log.info("Extract: start")
    data = [1, 2]          # test: [] or [1, None, 3]
    if not data:
        raise ValueError("Extract returned no data")
    log.info(f"Extract: got {len(data)} rows")
    return data


def transform(data):
    log.info("Transform: start")
    try:
        result = [x * 10 for x in data]
    except TypeError as e:
        log.error(f"Transform failed, bad value in data: {e}")
        raise
    log.info(f"Transform: done, {len(result)} rows")
    return result


def load(data):
    log.info("Load: start")
    try:
        with open("output.txt", "w") as f:
            for x in data:
                f.write(f"{x}\n")
    except OSError as e:
        log.error(f"Load failed, cannot write file: {e}")
        raise
    log.info(f"Load: wrote {len(data)} rows to output.txt")


def main():
    log.info("===== ETL started =====")
    try:
        data = extract()
        result = transform(data)
        load(result)
    except Exception:
        log.exception("ETL failed")
        sys.exit(1)
    log.info("===== ETL finished successfully =====")


if __name__ == "__main__":
    main()