"""
Delete Phone Values from Leads
===============================
Reads unique lead IDs from data/unique_lead_ids.txt and calls
DELETE /api/leads/{leadId}/variables?phone=
to erase the phone variable on each lead.

Runs concurrently with rate-limit handling and a progress bar.
"""

import base64
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BASE_URL = "https://api.lemlist.com/api"
INPUT_FILE = Path("data/matched_lead_ids.csv")
VARIABLE_TO_ERASE = "phone"       # query param name to clear
START_FROM_ROW = 1            # 1-based; set to 1 to process all
MAX_WORKERS = 5                   # concurrent requests (stay within 20 req/2s)
RATE_LIMIT_REQUESTS = 20
RATE_LIMIT_WINDOW = 2             # seconds
REQUEST_DELAY = RATE_LIMIT_WINDOW / RATE_LIMIT_REQUESTS  # ~0.1s per request
LOG_FILE = Path("data/delete_phone_log.txt")


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def build_auth_header(api_key: str) -> dict:
    token = base64.b64encode(f":{api_key}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def delete_phone(lead_id: str, headers: dict, max_retries: int = 5) -> dict:
    """DELETE /api/leads/{leadId}/variables?phone= with retry on 429."""
    url = f"{BASE_URL}/leads/{lead_id}/variables"
    params = {VARIABLE_TO_ERASE: ""}

    for attempt in range(1, max_retries + 1):
        time.sleep(REQUEST_DELAY)
        resp = requests.delete(url, headers=headers, params=params, timeout=60)

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2))
            time.sleep(retry_after)
            continue

        return {
            "leadId": lead_id,
            "status": resp.status_code,
            "ok": resp.status_code == 200,
        }

    return {"leadId": lead_id, "status": 429, "ok": False, "error": "rate-limited"}


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main() -> None:
    # Load lead IDs
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        print("   Run extract_ids.py first.")
        return

    lead_ids = []
    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            lid = row.get("_id", "").strip()
            if lid:
                lead_ids.append(lid)
    print(f"📂 Loaded {len(lead_ids):,} lead IDs from {INPUT_FILE}")

    # Skip already-processed rows
    if START_FROM_ROW > 1:
        lead_ids = lead_ids[START_FROM_ROW - 1:]
        print(f"⏩ Resuming from row {START_FROM_ROW:,} → {len(lead_ids):,} remaining")

    # Auth
    api_key = input("🔑 Enter your Lemlist API key: ").strip()
    if not api_key:
        print("❌ No API key provided. Exiting.")
        return
    headers = build_auth_header(api_key)

    # Process concurrently
    success = 0
    failed = 0
    errors: list[dict] = []

    print(f"\n🚀 Deleting '{VARIABLE_TO_ERASE}' on {len(lead_ids):,} leads "
          f"(concurrency={MAX_WORKERS})…\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(delete_phone, lid, headers): lid
            for lid in lead_ids
        }
        with tqdm(total=len(futures), unit="lead",
                  desc="Erasing phone", ncols=90) as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result["ok"]:
                        success += 1
                    else:
                        failed += 1
                        errors.append(result)
                except Exception as exc:
                    lid = futures[future]
                    failed += 1
                    errors.append({"leadId": lid, "error": str(exc)})
                    tqdm.write(f"   ❌ {lid}: {exc}")
                finally:
                    pbar.set_postfix(ok=success, fail=failed, refresh=True)
                    pbar.update(1)

    # Summary
    print(f"\n✅ Done!")
    print(f"   Success: {success:,}")
    print(f"   Failed:  {failed:,}")

    # Log failures
    if errors:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "w", encoding="utf-8") as fh:
            for e in errors:
                fh.write(f"{e}\n")
        print(f"   Error log: {LOG_FILE}")


if __name__ == "__main__":
    main()
