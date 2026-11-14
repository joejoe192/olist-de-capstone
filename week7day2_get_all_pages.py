"""Fetch several pages of Microsoft's GitHub repos by following the Link header."""

import requests
import truststore

from config import GITHUB_API_URL, GITHUB_TOKEN

truststore.inject_into_ssl()

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}


def get_all_pages(url: str, params: dict | None = None, max_pages: int = 3) -> list[dict]:
    """Fetch up to ``max_pages`` pages from a GitHub list endpoint by following the Link header."""
    rows = []
    for _ in range(max_pages):
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        rows.extend(r.json())
        url = r.links.get("next", {}).get("url")
        params = None
        if url is None:
            break
    return rows


repos = get_all_pages(f"{GITHUB_API_URL}/orgs/microsoft/repos", {"per_page": 100})
print("repos fetched:", len(repos))
print([repo["name"] for repo in repos[:5]])
