"""
Lemlist Campaign Leads Exporter
================================
Retrieves all campaigns, exports their leads concurrently,
and merges everything into a single JSON file with a union of all columns.

API docs: https://developer.lemlist.com/api-reference
Rate limit: 20 requests per 2 seconds per API key.
Auth: HTTP Basic with empty username, API key as password.
"""

import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

# ──────────────────────────────────────────────
# CONFIGURATION  (edit these as needed)
# ──────────────────────────────────────────────
BASE_URL = "https://api.lemlist.com/api"
EXPORT_STATE = "all"          # "all" | "interested" | comma-separated states
EXPORT_FORMAT = "json"        # "json" | "csv"
CAMPAIGNS_PAGE_LIMIT = 100    # max per page (API cap = 100)
MAX_WORKERS = 5               # concurrent export requests (stay within 20 req/2s)
OUTPUT_DIR = Path("data")     # directory for output files
OUTPUT_FILE = OUTPUT_DIR / f"all_leads_{datetime.now():%Y%m%d_%H%M%S}.json"

# Rate-limit safety: minimum seconds between bursts
RATE_LIMIT_REQUESTS = 20
RATE_LIMIT_WINDOW = 2         # seconds
REQUEST_DELAY = RATE_LIMIT_WINDOW / RATE_LIMIT_REQUESTS  # ~0.1s per request


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def build_auth_header(api_key: str) -> dict:
    """Lemlist uses Basic auth with empty username and API key as password."""
    token = base64.b64encode(f":{api_key}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _request_with_retry(method: str, url: str, headers: dict,
                         params: dict | None = None,
                         max_retries: int = 5) -> requests.Response:
    """Fire an HTTP request, honouring Retry-After on 429 responses."""
    for attempt in range(1, max_retries + 1):
        resp = requests.request(method, url, headers=headers, params=params,
                                timeout=120)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2))
            print(f"  ⏳ Rate-limited. Retrying in {retry_after}s "
                  f"(attempt {attempt}/{max_retries})…")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Still rate-limited after {max_retries} retries: {url}")


# ──────────────────────────────────────────────
# API CALLS
# ──────────────────────────────────────────────
def fetch_all_campaigns(headers: dict) -> list[dict]:
    """Paginate through all campaigns using version=v2."""
    campaigns: list[dict] = []
    page = 1
    print("\n📋 Fetching campaign list…")
    while True:
        params = {
            "version": "v2",
            "limit": CAMPAIGNS_PAGE_LIMIT,
            "page": page,
        }
        resp = _request_with_retry("GET", f"{BASE_URL}/campaigns",
                                   headers=headers, params=params)
        data = resp.json()

        # v2 API wraps campaigns in {"campaigns": [...], "pagination": {...}}
        if isinstance(data, dict) and "campaigns" in data:
            batch = data["campaigns"]
            pagination = data.get("pagination", {})
            total_pages = pagination.get("totalPage", page)
        elif isinstance(data, list):
            batch = data
            total_pages = page  # unknown, stop when empty
        else:
            break

        if not batch:
            break
        campaigns.extend(batch)
        print(f"   Page {page}/{total_pages}: {len(batch)} campaigns")
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    print(f"   ✅ Total campaigns found: {len(campaigns)}")
    return campaigns


def export_campaign_leads(campaign: dict, headers: dict) -> list[dict]:
    """Export leads for a single campaign as JSON.  Returns list of lead dicts."""
    cid = campaign["_id"]
    name = campaign.get("name", cid)
    url = f"{BASE_URL}/campaigns/{cid}/export/leads"
    params = {"state": EXPORT_STATE, "format": EXPORT_FORMAT}

    try:
        time.sleep(REQUEST_DELAY)  # light throttle
        resp = _request_with_retry("GET", url, headers=headers, params=params)
    except requests.HTTPError as exc:
        print(f"   ⚠️  Skipping campaign '{name}' ({cid}): {exc}")
        return []
    except RuntimeError as exc:
        print(f"   ⚠️  Skipping campaign '{name}' ({cid}): {exc}")
        return []

    try:
        data = resp.json()
    except json.JSONDecodeError:
        print(f"   ⚠️  Non-JSON response for campaign '{name}' ({cid})")
        return []

    leads = data if isinstance(data, list) else [data]

    # Tag every lead with its source campaign for traceability
    for lead in leads:
        lead["_campaignId"] = cid
        lead["_campaignName"] = name

    return leads


# ──────────────────────────────────────────────
# MERGE & SAVE
# ──────────────────────────────────────────────
def merge_and_save(all_leads: list[dict], output_path: Path) -> None:
    """Write combined leads to a JSON file.
    Uses streaming write to keep memory flat for large datasets."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Collect the union of all keys across every lead (preserves all columns)
    all_keys = set()
    for lead in all_leads:
        all_keys.update(lead.keys())

    print(f"\n💾 Writing {len(all_leads)} leads ({len(all_keys)} columns) → {output_path}")

    # Stream-write as a JSON array to avoid building a huge string in memory
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("[\n")
        for idx, lead in enumerate(all_leads):
            json.dump(lead, fh, ensure_ascii=False, default=str)
            if idx < len(all_leads) - 1:
                fh.write(",\n")
            else:
                fh.write("\n")
        fh.write("]\n")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"   ✅ Saved ({size_mb:.2f} MB)")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main() -> None:
    # Prompt for API key
    api_key = input("🔑 Enter your Lemlist API key: ")
    if not api_key.strip():
        print("❌ No API key provided. Exiting.")
        return
    headers = build_auth_header(api_key.strip())

    # 1. Fetch campaigns
    campaigns = fetch_all_campaigns(headers)
    if not campaigns:
        print("No campaigns found. Exiting.")
        return

    # 2. Export leads concurrently with a progress bar
    all_leads: list[dict] = []
    failed: list[str] = []

    print(f"\n🚀 Exporting leads from {len(campaigns)} campaigns "
          f"(concurrency={MAX_WORKERS})…\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(export_campaign_leads, c, headers): c
            for c in campaigns
        }
        with tqdm(total=len(futures), unit="campaign",
                  desc="Exporting", ncols=90) as pbar:
            for future in as_completed(futures):
                campaign = futures[future]
                try:
                    leads = future.result()
                    all_leads.extend(leads)
                    pbar.set_postfix(leads=len(all_leads), refresh=True)
                except Exception as exc:
                    name = campaign.get("name", campaign["_id"])
                    failed.append(name)
                    tqdm.write(f"   ❌ {name}: {exc}")
                finally:
                    pbar.update(1)

    if failed:
        print(f"\n⚠️  {len(failed)} campaign(s) failed: {', '.join(failed)}")

    if not all_leads:
        print("No leads retrieved. Exiting.")
        return

    # 3. Merge & save
    merge_and_save(all_leads, OUTPUT_FILE)
    print("\n🎉 Done!")


if __name__ == "__main__":
    main()
