"""Check that the exchange-rate API and the GitHub API are reachable with the credentials in .env."""

import os

import requests
import truststore
from dotenv import load_dotenv

truststore.inject_into_ssl()
load_dotenv()

EXCHANGE_API_URL = os.getenv("EXCHANGE_API_URL")
EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY")
GITHUB_API_URL = os.getenv("GITHUB_API_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def check_exchange() -> None:
    """Fetch the BRL to USD rate and print it, raising if the API reports an error."""
    url = f"{EXCHANGE_API_URL}/{EXCHANGE_API_KEY}/latest/BRL"
    r = requests.get(url, timeout=10)
    try:
        data = r.json()
    except ValueError:
        raise RuntimeError(f"HTTP {r.status_code} {r.reason} (response was not JSON)") from None
    if data["result"] != "success":
        raise RuntimeError(f"HTTP {r.status_code}: {data['error-type']}")
    print("Exchange API OK: 1 BRL =", data["conversion_rates"]["USD"], "USD")


def check_github() -> None:
    """Call GitHub's /user endpoint with the token and print the authenticated login."""
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    r = requests.get(f"{GITHUB_API_URL}/user", headers=headers, timeout=10)
    r.raise_for_status()
    print("GitHub API OK: authenticated as", r.json()["login"])


for check in (check_exchange, check_github):
    try:
        check()
    except requests.HTTPError as exc:
        print(f"{check.__name__} FAILED: HTTP {exc.response.status_code} {exc.response.reason}")
    except requests.RequestException as exc:
        print(f"{check.__name__} FAILED: {type(exc).__name__} (details hidden: URL may contain the API key)")
    except Exception as exc:
        print(f"{check.__name__} FAILED: {type(exc).__name__}: {exc}")
