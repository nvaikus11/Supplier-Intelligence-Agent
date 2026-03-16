from pathlib import Path
from urllib.parse import urljoin
import subprocess
import requests
import pandas as pd
from bs4 import BeautifulSoup

from load_vendor_master import load_vendor_master


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "outputs" / "daily_reports" / "browser_candidate_pages.csv"

TEST_MODE = True
TEST_VENDOR_LIMIT_PER_CATEGORY = 1
MAX_PAGES_PER_VENDOR = 8

HEADERS = {
    "User-Agent": "SupplierWatchtower/1.0"
}

COMMON_PATHS = [
    "/news",
    "/press",
    "/newsroom",
    "/media",
    "/investors",
    "/investor-relations",
    "/sustainability",
    "/esg",
    "/about",
]


def select_test_suppliers(suppliers_df: pd.DataFrame) -> pd.DataFrame:
    if not TEST_MODE:
        return suppliers_df

    return (
        suppliers_df.sort_values(["category_id", "supplier_name"])
        .groupby("category_id", as_index=False)
        .head(TEST_VENDOR_LIMIT_PER_CATEGORY)
        .reset_index(drop=True)
    )


def discover_candidate_pages(base_url: str, max_pages: int = MAX_PAGES_PER_VENDOR) -> list[dict]:
    pages = []

    if not isinstance(base_url, str) or not base_url.strip():
        return pages

    # Try vendor home page and common content pages
    candidate_urls = [base_url.rstrip("/")] + [base_url.rstrip("/") + p for p in COMMON_PATHS]

    seen = set()

    for url in candidate_urls:
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                continue

            if url not in seen:
                seen.add(url)
                pages.append(
                    {
                        "page_url": url,
                        "page_title": "",
                        "page_source_type": "seed_page",
                    }
                )

            soup = BeautifulSoup(response.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                title = a.get_text(" ", strip=True)

                if not href:
                    continue

                if href.startswith("/"):
                    href = urljoin(url, href)

                if href in seen:
                    continue

                title_l = title.lower()
                href_l = href.lower()

                if any(
                    keyword in title_l or keyword in href_l
                    for keyword in [
                        "news", "press", "investor", "result", "earnings",
                        "annual", "report", "sustainability", "esg",
                        "production", "plant", "capacity"
                    ]
                ):
                    seen.add(href)
                    pages.append(
                        {
                            "page_url": href,
                            "page_title": title,
                            "page_source_type": "discovered_link",
                        }
                    )

                if len(pages) >= max_pages:
                    return pages
        except requests.RequestException:
            continue

    return pages[:max_pages]


def touch_browser(page_url: str) -> None:
    """
    Opens the page in the OpenClaw browser profile so you can later
    deepen this into true browser-driven extraction.
    """
    try:
        subprocess.run(
            [
                "openclaw",
                "browser",
                "--browser-profile",
                "openclaw",
                "open",
                page_url,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        pass


def build_browser_candidates(suppliers_df: pd.DataFrame) -> pd.DataFrame:
    records = []

    for _, supplier in suppliers_df.iterrows():
        supplier_name = supplier["supplier_name"]
        website = supplier.get("website", "") or ""
        category_id = supplier["category_id"]

        pages = discover_candidate_pages(website, max_pages=MAX_PAGES_PER_VENDOR)

        print(f"{supplier_name} | browser candidate pages: {len(pages)}")

        for page in pages:
            # touch_browser(page["page_url"])

            records.append(
                {
                    "supplier_id": supplier["supplier_id"],
                    "supplier_name": supplier_name,
                    "category_id": category_id,
                    "website": website,
                    "page_url": page["page_url"],
                    "page_title": page["page_title"],
                    "page_source_type": page["page_source_type"],
                }
            )

    return pd.DataFrame(records)


if __name__ == "__main__":
    vendor_data = load_vendor_master()
    suppliers_df = select_test_suppliers(vendor_data["suppliers"])

    candidates_df = build_browser_candidates(suppliers_df)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if candidates_df.empty:
        candidates_df = pd.DataFrame(
            columns=[
                "supplier_id",
                "supplier_name",
                "category_id",
                "website",
                "page_url",
                "page_title",
                "page_source_type",
            ]
        )

    candidates_df.to_csv(OUTPUT_FILE, index=False)

    print("\nBrowser candidate pages collected.\n")
    print(candidates_df.head(20).to_string(index=False))
    print(f"\nSaved to: {OUTPUT_FILE}")