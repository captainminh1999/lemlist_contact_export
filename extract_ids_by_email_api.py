"""
Extract Lead IDs via Lemlist API (Get Lead by Email)
=====================================================
Reads emails from contact_phone_to_remove.csv, calls
GET /api/leads/{email}?version=v2 for each email, collects
all returned lead _id values, and writes unique IDs to a CSV.

API docs: https://developer.lemlist.com/api-reference/endpoints/leads/get-lead-by-email
"""

import base64
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BASE_URL = "https://api.lemlist.com/api"
INPUT_FILE = Path("data/contact_phone_to_remove.csv")
OUTPUT_FILE = Path("data/matched_lead_ids_by_api.csv")
LOG_FILE = Path("data/extract_ids_by_email_api_log.txt")

MAX_WORKERS = 5
RATE_LIMIT_REQUESTS = 20
RATE_LIMIT_WINDOW = 2  # seconds
REQUEST_DELAY = RATE_LIMIT_WINDOW / RATE_LIMIT_REQUESTS  # ~0.1s per request
BATCH_LIMIT = 50  # Set to 50 for testing, None for all emails


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def build_auth_header(api_key: str) -> dict:
    token = base64.b64encode(f":{api_key}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def load_emails(csv_path: Path) -> list[str]:
    """Load unique, non-empty emails from the contacts CSV."""
    emails: set[str] = set()
    with open(csv_path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            email = (row.get("email") or "").strip()
            if email:
                emails.add(email)
    return sorted(emails)


def get_lead_ids_by_email(
    email: str, headers: dict, max_retries: int = 5
) -> dict:
    """
    GET /api/leads/{email}?version=v2
    Returns {"email": ..., "lead_ids": [...], "ok": True/False, ...}
    """
    url = f"{BASE_URL}/leads/{email}"
    params = {"version": "v2"}

    for attempt in range(1, max_retries + 1):
        time.sleep(REQUEST_DELAY)
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
        except requests.RequestException as exc:
            if attempt == max_retries:
                return {"email": email, "lead_ids": [], "ok": False, "error": str(exc)}
            time.sleep(2)
            continue

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2))
            time.sleep(retry_after)
            continue

        if resp.status_code == 404:
            return {"email": email, "lead_ids": [], "ok": True, "status": 404}

        if resp.status_code != 200:
            return {
                "email": email,
                "lead_ids": [],
                "ok": False,
                "status": resp.status_code,
                "error": resp.text[:200],
            }

        # Response is a JSON array of lead objects
        data = resp.json()
        if isinstance(data, list):
            ids = [lead["_id"] for lead in data if "_id" in lead]
        elif isinstance(data, dict) and "_id" in data:
            ids = [data["_id"]]
        else:
            ids = []

        return {"email": email, "lead_ids": ids, "ok": True, "status": 200}

    return {"email": email, "lead_ids": [], "ok": False, "error": "rate-limited"}


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main() -> None:
    # Load emails
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    emails = load_emails(INPUT_FILE)
    print(f"📂 Loaded {len(emails):,} unique emails from {INPUT_FILE}")

    if BATCH_LIMIT:
        emails = emails[:BATCH_LIMIT]
        print(f"   Limited to first {len(emails):,} emails for testing")

    if not emails:
        print("❌ No emails found. Exiting.")
        return

    # Auth
    api_key = os.environ.get("LEMLIST_API_KEY", "").strip()
    if not api_key:
        api_key = input("🔑 Enter your Lemlist API key: ").strip()
    if not api_key:
        print("❌ No API key provided. Set LEMLIST_API_KEY env var or provide it when prompted.")
        return
    headers = build_auth_header(api_key)

    # Process concurrently
    all_lead_ids: set[str] = set()
    success = 0
    not_found = 0
    failed = 0
    errors: list[dict] = []

    print(
        f"\n🚀 Fetching lead IDs for {len(emails):,} emails "
        f"(concurrency={MAX_WORKERS})…\n"
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(get_lead_ids_by_email, email, headers): email
            for email in emails
        }
        with tqdm(
            total=len(futures), unit="email", desc="Fetching leads", ncols=90
        ) as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result["ok"]:
                        if result.get("status") == 404:
                            not_found += 1
                            if not_found <= 5:  # Print first 5 404s
                                tqdm.write(f"   ℹ️  404: {result['email']}")
                        else:
                            success += 1
                            for lid in result["lead_ids"]:
                                all_lead_ids.add(lid)
                    else:
                        failed += 1
                        errors.append(result)
                        if failed <= 5:  # Print first 5 failures
                            tqdm.write(f"   ❌ {result.get('email', '?')}: {result.get('error', 'unknown')}")
                except Exception as exc:
                    email = futures[future]
                    failed += 1
                    errors.append({"email": email, "error": str(exc)})
                    tqdm.write(f"   ❌ {email}: {exc}")
                finally:
                    pbar.set_postfix(
                        ids=len(all_lead_ids), ok=success, miss=not_found, fail=failed,
                        refresh=True,
                    )
                    pbar.update(1)

    # Write output
    sorted_ids = sorted(all_lead_ids)
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["_id"])
        for lid in sorted_ids:
            writer.writerow([lid])

    # Summary
    print(f"\n✅ Done!")
    print(f"   Emails queried:     {len(emails):,}")
    print(f"   Found (200):        {success:,}")
    print(f"   Not found (404):    {not_found:,}")
    print(f"   Failed:             {failed:,}")
    print(f"   Unique lead IDs:    {len(all_lead_ids):,}")
    print(f"   Output:             {OUTPUT_FILE}")

    # Log failures
    if errors:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "w", encoding="utf-8") as fh:
            for e in errors:
                fh.write(f"{e}\n")
        print(f"   Error log:          {LOG_FILE}")


if __name__ == "__main__":
    main()
