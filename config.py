# config.py - reads the .env file once. pip install python-dotenv
import os
from dotenv import load_dotenv

load_dotenv()                                   # reads the .env file

EXCHANGE_API_URL = os.getenv("EXCHANGE_API_URL")
EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

for name in ("EXCHANGE_API_URL", "EXCHANGE_API_KEY", "DATABASE_URL"):
    if not os.getenv(name):                     # fail early with a clear message
        raise RuntimeError(f"Missing {name} in the .env file")
