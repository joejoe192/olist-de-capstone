import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),   # to a file
        logging.StreamHandler(),          # to the screen
    ],
)
log = logging.getLogger(__name__)
log.debug("checking values")
log.info("Extract: reading CSV files")
log.warning("Dropped 8 rows with no delivery date")
