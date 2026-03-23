from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
import hashlib
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from llm_signal_extractor import extract_signals_for_supplier


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "outputs" / "daily_reports" / "browser_candidate_pages.csv"
OUTPUT_FILE = BASE_DIR / "outputs" / "daily_reports" / "real_signals.csv"

HEADERS = {
    "User-Agent": "SupplierWatchtower/1.0"
}

REQUEST_TIMEOUT = 20
MAX_TEXT_CHARS = 6000


def normalize_text(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value).strip()


def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").strip().lower()
    except Exception:
        return ""


def classify_confidence(source_url: str, supplier_domain: str) -> str:
    domain = get_domain(source_url)
    if supplier_domain and supplier_domain in domain:
        return "High"
    return "Medium"


def stable_event_id(supplier_id: str, title: str, url: str) -> str:
    raw = f"{supplier_id}|{title}|{url}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]
    return f"EVT-{digest.upper()}"


def fetch_page_text(url: str) -> tuple[str, str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            return "", ""

        soup = BeautifulSoup(response.text, "html.parser")

        title = ""
        if soup.title and soup.title.string:
            title = normalize_text(soup.title.string)

        for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
            tag.decompose()

        text = normalize_text(soup.get_text(" ", strip=True))
        return title, text[:MAX_TEXT_CHARS]
    except requests.RequestException:
        return "", ""


def build_items_for_supplier(supplier_pages: pd.DataFrame) -> list[dict]:
    items = []

    for _, row in supplier_pages.iterrows():
        page_title, page_text = fetch_page_text(row["page_url"])

        if not page_text:
            continue

        items.append(
            {
                "source_title": page_title or row.get("page_title", "") or row["page_url"],
                "source_snippet": page_text,
                "source_url": row["page_url"],
                "source_type": row.get("page_source_type", "browser_page"),
            }
        )

    return items


def process_supplier_group(supplier_pages: pd.DataFrame) -> list[dict]:
    if supplier_pages.empty:
        return []

    first_row = supplier_pages.iloc[0]
    supplier_id = first_row["supplier_id"]
    supplier_name = first_row["supplier_name"]
    category_id = first_row["category_id"]
    supplier_domain = get_domain(first_row.get("website", "") or "")

    category_name_map = {
        "CAT001": "Camera Lens",
        "CAT004": "Titanium",
        "CAT005": "Chip Modules",
        "CAT006": "Battery Materials",
    }
    category_name = category_name_map.get(category_id, category_id)

    category_context = {
        "category_name": category_name
    }

    items = build_items_for_supplier(supplier_pages)
    print(f"{supplier_name} | fetched full pages: {len(items)}")

    if not items:
        return []

    llm_result = extract_signals_for_supplier(
        supplier_name=supplier_name,
        category_id=category_id,
        category_name=category_name,
        category_context=category_context,
        items=items,
    )

    print(f"{supplier_name} | llm signals: {len(llm_result.get('signals', []))}")

    events = []

    for signal in llm_result.get("signals", []):
        if (not signal.get("is_relevant", False)) or signal.get("risk_type") == "not_relevant":
            continue

        item_index = signal.get("item_index")
        if not item_index or item_index < 1 or item_index > len(items):
            continue

        item = items[item_index - 1]
        source_url = item["source_url"]
        source_title = normalize_text(item["source_title"])

        events.append(
            {
                "event_id": stable_event_id(supplier_id, source_title, source_url),
                "event_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "supplier_id": supplier_id,
                "supplier_name": supplier_name,
                "category_id": category_id,
                "category_name": category_name,
                "signal_type": signal["risk_type"],
                "signal_text": signal["evidence_summary"],
                "severity": signal["severity"],
                "confidence": classify_confidence(source_url, supplier_domain),
                "source_type": item["source_type"],
                "source_url": source_url,
                "source_title": source_title,
                "llm_reason": signal["reason"],
                "supplier_summary": llm_result.get("supplier_summary", ""),
                "recommended_action": signal["recommended_action"],
            }
        )

    return events


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing file: {INPUT_FILE}")

    candidate_pages = pd.read_csv(INPUT_FILE)

    if candidate_pages.empty:
        print("\nNo browser candidate pages found.\n")
        pd.DataFrame(columns=[
            "event_id", "event_date", "supplier_id", "supplier_name",
            "category_id", "category_name", "signal_type", "signal_text",
            "severity", "confidence", "source_type", "source_url",
            "source_title", "llm_reason", "supplier_summary",
            "recommended_action"
        ]).to_csv(OUTPUT_FILE, index=False)
        return

    all_events = []

    for supplier_name, supplier_pages in candidate_pages.groupby("supplier_name"):
        events = process_supplier_group(supplier_pages)
        print(f"{supplier_name} | events found: {len(events)}")
        all_events.extend(events)

    output_df = pd.DataFrame(all_events)

    if output_df.empty:
        output_df = pd.DataFrame(columns=[
            "event_id", "event_date", "supplier_id", "supplier_name",
            "category_id", "category_name", "signal_type", "signal_text",
            "severity", "confidence", "source_type", "source_url",
            "source_title", "llm_reason", "supplier_summary",
            "recommended_action"
        ])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(OUTPUT_FILE, index=False)

    print("\nBrowser-enriched signals collected successfully.\n")
    print(output_df.head(20).to_string(index=False))
    print(f"\nSaved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()