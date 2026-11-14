"""Lab 2: pull 3 pages of Microsoft's GitHub repos and save them as Parquet."""

from pathlib import Path

import pandas as pd
import requests
import truststore

from config import GITHUB_API_URL, GITHUB_TOKEN

truststore.inject_into_ssl()

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
COLS = ["id", "name", "language", "stargazers_count"]
OUT_PATH = Path("/Users/meemee.maung/olist-star-schema/data/repos_microsoft.parquet")


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


def main() -> None:
    """Fetch repos, keep four columns, print counts, and write the Parquet file."""
    repos = get_all_pages(
        f"{GITHUB_API_URL}/orgs/microsoft/repos",
        {"per_page": 100},
        max_pages=3,
    )

    df = pd.DataFrame(repos)[COLS]
    print(len(df))
    print(df["language"].value_counts().head())

    df.to_parquet(OUT_PATH, index=False)

    check = pd.read_parquet(OUT_PATH)
    print("saved:", OUT_PATH.name, check.shape, check.columns.tolist())


if __name__ == "__main__":
    main()
