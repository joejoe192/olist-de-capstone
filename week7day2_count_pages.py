"""Report how many pages a GitHub list endpoint has for a given page size."""

from urllib.parse import parse_qs, urlparse

import requests
import truststore

truststore.inject_into_ssl()

URL = "https://api.github.com/orgs/microsoft/repos"
PER_PAGE = 30


def count_pages(url: str, per_page: int) -> int:
    """Return the number of pages for ``url``, using the Link header's rel="last"."""
    r = requests.get(url, params={"per_page": per_page, "page": 1}, timeout=10)
    r.raise_for_status()
    if not r.json():
        return 0
    if "last" not in r.links:
        return 1
    query = parse_qs(urlparse(r.links["last"]["url"]).query)
    return int(query["page"][0])


print("pages:", count_pages(URL, PER_PAGE))
