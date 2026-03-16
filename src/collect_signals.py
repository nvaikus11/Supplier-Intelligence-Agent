from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse
import hashlib
import html
import re

import feedparser
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

from load_vendor_master import load_vendor_master
from llm_signal_extractor import extract_signals_for_supplier


HEADERS = {
    "User-Agent": "SupplierWatchtower/1.0 (+https://github.com/your-repo)"
}
TEST_MODE = True
TEST_VENDOR_LIMIT_PER_CATEGORY = 1
MAX_PAGES_PER_VENDOR = 8
REQUEST_TIMEOUT = 15
MAX_ITEMS_PER_SUPPLIER = 6

NEWS_PAGE_CANDIDATES = [
    "/news", "/press", "/newsroom", "/media", "/press-releases",
    "/investors", "/investor-relations"
]

BASE_DIR = Path(__file__).resolve().parent.parent
SEEN_URLS_FILE = BASE_DIR / "outputs" / "daily_reports" / "seen_urls.csv"
CATEGORY_CONFIG_FILE = BASE_DIR / "configs" / "category_keywords.yaml"

def select_test_suppliers(suppliers_df: pd.DataFrame) -> pd.DataFrame:
    if not TEST_MODE:
        return suppliers_df

    test_df = (
        suppliers_df.sort_values(["category_id", "supplier_name"])
        .groupby("category_id", as_index=False)
        .head(TEST_VENDOR_LIMIT_PER_CATEGORY)
        .reset_index(drop=True)
    )

    print("\nTEST MODE ENABLED")
    print(test_df[["supplier_name", "category_id"]].to_string(index=False))
    print()
    return test_df

def load_category_keywords() -> dict:
    if not CATEGORY_CONFIG_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {CATEGORY_CONFIG_FILE}")
    with open(CATEGORY_CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_text(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def get_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        return parsed.netloc.replace("www.", "").strip().lower()
    except Exception:
        return ""


def classify_confidence(source_url: str, supplier_domain: str) -> str:
    domain = get_domain(source_url)
    if supplier_domain and supplier_domain in domain:
        return "High"
    if "news.google.com" in domain:
        return "Medium"
    return "Low"


def stable_event_id(supplier_id: str, title: str, url: str) -> str:
    raw = f"{supplier_id}|{title}|{url}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]
    return f"EVT-{digest.upper()}"


def load_seen_urls() -> set[str]:
    if not SEEN_URLS_FILE.exists():
        return set()
    df = pd.read_csv(SEEN_URLS_FILE)
    return set(df["source_url"].dropna().astype(str).tolist())


def save_seen_urls(urls: set[str]) -> None:
    SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"source_url": sorted(urls)}).to_csv(SEEN_URLS_FILE, index=False)


def fetch_google_news_rss(query: str) -> list[dict]:
    rss_url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    feed = feedparser.parse(rss_url)
    items = []

    for entry in getattr(feed, "entries", []):
        title = normalize_text(getattr(entry, "title", ""))
        summary = normalize_text(getattr(entry, "summary", ""))
        link = getattr(entry, "link", "")
        published = getattr(entry, "published", "") or getattr(entry, "updated", "")

        items.append(
            {
                "source_title": title,
                "source_snippet": summary,
                "source_url": link,
                "event_date": published,
                "source_type": "google_news_rss",
            }
        )
    return items


def fetch_press_page_items(base_url: str, max_items: int = 5) -> list[dict]:
    items = []
    if not isinstance(base_url, str) or not base_url.strip():
        return items

    for suffix in NEWS_PAGE_CANDIDATES:
        try:
            url = base_url.rstrip("/") + suffix
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a", href=True)

            seen = set()
            for a in links:
                title = normalize_text(a.get_text(" ", strip=True))
                href = a["href"].strip()

                if not title or len(title) < 12:
                    continue

                if href.startswith("/"):
                    href = base_url.rstrip("/") + href

                if href in seen:
                    continue
                seen.add(href)

                items.append(
                    {
                        "source_title": title,
                        "source_snippet": "",
                        "source_url": href,
                        "event_date": "",
                        "source_type": "supplier_site",
                    }
                )

                if len(items) >= max_items:
                    return items
        except requests.RequestException:
            continue

    return items


def build_queries(
    supplier_name: str,
    website: str,
    public_company: str,
    category_name: str,
    category_keywords: dict,
) -> list[str]:
    domain = get_domain(website)
    cfg = category_keywords.get(category_name, {})

    search_terms = cfg.get("search_terms", [])
    financial_terms = cfg.get("financial_terms", [])
    logistics_terms = cfg.get("logistics_terms", [])
    esg_terms = cfg.get("esg_terms", [])
    capacity_terms = cfg.get("capacity_terms", [])

    queries = [
        f'"{supplier_name}" {" OR ".join(search_terms[:3])}' if search_terms else f'"{supplier_name}" supplier news',
        f'"{supplier_name}" {" OR ".join(financial_terms[:3])}' if financial_terms else f'"{supplier_name}" earnings OR quarterly results',
        f'"{supplier_name}" {" OR ".join(logistics_terms[:3])}' if logistics_terms else f'"{supplier_name}" logistics OR shipping delay',
        f'"{supplier_name}" {" OR ".join(esg_terms[:3])}' if esg_terms else f'"{supplier_name}" sustainability OR compliance',
        f'"{supplier_name}" {" OR ".join(capacity_terms[:3])}' if capacity_terms else f'"{supplier_name}" capacity OR plant OR production',
    ]

    if str(public_company).strip().lower() == "yes":
        queries.append(f'"{supplier_name}" SEC filing OR 10-K OR 10-Q OR earnings transcript')

    if domain:
        queries.append(f'site:{domain} "{supplier_name}"')

    return queries


def build_prefilter_terms(category_name: str, category_keywords: dict) -> list[str]:
    cfg = category_keywords.get(category_name, {})
    terms = []
    for key in ["search_terms", "financial_terms", "logistics_terms", "esg_terms", "capacity_terms"]:
        terms.extend(cfg.get(key, []))
    terms.extend([
        "earnings", "quarterly", "results", "restructuring", "layoffs", "margin",
        "debt", "liquidity", "bankruptcy", "capacity", "plant", "factory",
        "production", "expansion", "shipment", "logistics", "freight", "port",
        "supply", "compliance", "labor", "safety", "emissions", "sustainability",
        "renewable", "investor", "annual report", "10-k", "10-q"
    ])
    return list({t.lower() for t in terms})


def passes_prefilter(item: dict, supplier_name: str, prefilter_terms: list[str]) -> bool:
    combined = f"{item.get('source_title', '')} {item.get('source_snippet', '')}".lower()

    if supplier_name.lower() in combined:
        return True

    if any(term.lower() in combined for term in prefilter_terms[:10]):
        return True

    return True


def dedupe_items(items: list[dict], seen_urls: set[str]) -> list[dict]:
    deduped = []
    seen_local = set()

    for item in items:
        url = item.get("source_url", "").strip()
        title = item.get("source_title", "").strip().lower()

        if not url:
            continue
        if url in seen_urls:
            continue

        key = (title, url)
        if key in seen_local:
            continue

        seen_local.add(key)
        deduped.append(item)

    return deduped


def collect_supplier_items(
    supplier_row: pd.Series,
    seen_urls: set[str],
    category_name: str,
    category_keywords: dict,
) -> list[dict]:
    supplier_name = supplier_row["supplier_name"]
    website = supplier_row.get("website", "") or ""
    public_company = supplier_row.get("public_company", "No")

    raw_items = []

    for query in build_queries(supplier_name, website, public_company, category_name, category_keywords):
        results = fetch_google_news_rss(query)[:2]
        raw_items.extend(results)

    site_items = fetch_press_page_items(website, max_items=4)
    print(f"{supplier_name} | site items: {len(site_items)}")
    raw_items.extend(site_items)

    print(f"{supplier_name} | raw items before prefilter: {len(raw_items)}")

    prefilter_terms = build_prefilter_terms(category_name, category_keywords)
    prefiltered_items = [item for item in raw_items if passes_prefilter(item, supplier_name, prefilter_terms)]
    print(f"{supplier_name} | items after prefilter: {len(prefiltered_items)}")

    deduped_items = dedupe_items(prefiltered_items, seen_urls)
    print(f"{supplier_name} | items after dedupe: {len(deduped_items)}")

    raw_items = deduped_items
    print(f"{supplier_name} | raw items after dedupe: {len(raw_items)}")

    return raw_items[:MAX_ITEMS_PER_SUPPLIER]


def collect_supplier_events(
    supplier_row: pd.Series,
    category_name: str,
    seen_urls: set[str],
    category_keywords: dict,
) -> tuple[list[dict], set[str]]:
    supplier_id = supplier_row["supplier_id"]
    supplier_name = supplier_row["supplier_name"]
    category_id = supplier_row["category_id"]
    website = supplier_row.get("website", "") or ""
    supplier_domain = get_domain(website)

    items = collect_supplier_items(supplier_row, seen_urls, category_name, category_keywords)
    print(f"{supplier_name} | candidate items: {len(items)}")
    if not items:
        return [], seen_urls

    llm_result = extract_signals_for_supplier(
        
        supplier_name=supplier_name,
        category_id=category_id,
        category_name=category_name,
        category_context=category_keywords.get(category_name, {}),
        items=items,
    )
    print(f"{supplier_name} | llm_result: {llm_result}")
    events = []
    supplier_summary = llm_result.get("supplier_summary", "")

    for signal in llm_result.get("signals", []):
        if (not signal.get("is_relevant", False)) or signal.get("risk_type") == "not_relevant":
            continue
        item_index = signal.get("item_index")
        if not item_index or item_index < 1 or item_index > len(items):
            continue

        item = items[item_index - 1]
        source_url = item.get("source_url", "").strip()
        source_title = normalize_text(item.get("source_title", ""))
        event_date = item.get("event_date", "")
        if not event_date:
            event_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        events.append(
            {
                "event_id": stable_event_id(supplier_id, source_title, source_url),
                "event_date": event_date,
                "supplier_id": supplier_id,
                "supplier_name": supplier_name,
                "category_id": category_id,
                "category_name": category_name,
                "signal_type": signal["risk_type"],
                "signal_text": signal["evidence_summary"],
                "severity": signal["severity"],
                "confidence": classify_confidence(source_url, supplier_domain),
                "source_type": item.get("source_type", "web_signal"),
                "source_url": source_url,
                "source_title": source_title,
                "llm_reason": signal["reason"],
                "supplier_summary": supplier_summary,
                "recommended_action": signal["recommended_action"],
            }
        )

        seen_urls.add(source_url)

    return events, seen_urls


def collect_real_signals(suppliers_df: pd.DataFrame, categories_df: pd.DataFrame) -> pd.DataFrame:
    all_events = []
    seen_urls = load_seen_urls()
    category_keywords = load_category_keywords()

    category_lookup = dict(zip(categories_df["category_id"], categories_df["category_name"]))

    for _, supplier in suppliers_df.iterrows():
        category_name = category_lookup.get(supplier["category_id"], "Unknown")

        try:
            events, seen_urls = collect_supplier_events(
                supplier_row=supplier,
                category_name=category_name,
                seen_urls=seen_urls,
                category_keywords=category_keywords,
            )
            print(f"{supplier['supplier_name']} | events found: {len(events)}")
            all_events.extend(events)
        except Exception as exc:
            all_events.append(
                {
                    "event_id": stable_event_id(
                        supplier["supplier_id"], "collector_error", supplier.get("website", "")
                    ),
                    "event_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "supplier_id": supplier["supplier_id"],
                    "supplier_name": supplier["supplier_name"],
                    "category_id": supplier["category_id"],
                    "category_name": category_name,
                    "signal_type": "general_news",
                    "signal_text": f"Collection error for {supplier['supplier_name']}: {exc}",
                    "severity": "Low",
                    "confidence": "Low",
                    "source_type": "collector_error",
                    "source_url": "",
                    "source_title": "collector_error",
                    "llm_reason": "Signal collection failed for this supplier during this run.",
                    "supplier_summary": "No supplier-level summary generated due to collection failure.",
                    "recommended_action": "Check source availability and retry",
                }
            )

    save_seen_urls(seen_urls)
    return pd.DataFrame(all_events)


def save_signals(signals_df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    expected_columns = [
        "event_id",
        "event_date",
        "supplier_id",
        "supplier_name",
        "category_id",
        "category_name",
        "signal_type",
        "signal_text",
        "severity",
        "confidence",
        "source_type",
        "source_url",
        "source_title",
        "llm_reason",
        "supplier_summary",
        "recommended_action",
    ]

    if signals_df.empty:
        signals_df = pd.DataFrame(columns=expected_columns)

    signals_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    output_file = BASE_DIR / "outputs" / "daily_reports" / "real_signals.csv"

    vendor_data = load_vendor_master()
    suppliers_df = select_test_suppliers(vendor_data["suppliers"])
    categories_df = vendor_data["categories"]

    signals = collect_real_signals(suppliers_df, categories_df)
    save_signals(signals, output_file)

    print("\nReal category-aware batched LLM-classified signals collected successfully.\n")
    print(signals.head(15).to_string(index=False))
    print(f"\nSaved to: {output_file}")