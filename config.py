"""Load API settings from the .env file so scripts never hard-code secrets."""

import os

from dotenv import load_dotenv

load_dotenv()

EXCHANGE_API_URL = os.getenv("EXCHANGE_API_URL")
EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY")
GITHUB_API_URL = os.getenv("GITHUB_API_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
