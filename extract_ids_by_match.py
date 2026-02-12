"""
Extract Lead IDs by Matching Email or LinkedIn URL
===================================================
Reads contacts from contact_phone_to_remove.csv, matches them
against the all_leads JSON file by email OR linkedinUrl, and
writes the unique matched lead _id values to a CSV.
"""

import csv
import json
import sys
import time
from pathlib import Path

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
INPUT_DIR = Path("data")
OUTPUT_DIR = Path("data")

CONTACTS_CSV = INPUT_DIR / "contact_phone_to_remove.csv"
OUTPUT_FILE = OUTPUT_DIR / "matched_lead_ids.csv"

# Auto-detect: pick the latest all_leads_*.json in INPUT_DIR
LEADS_FILE = None  # set to a specific Path to override auto-detect


def find_latest_leads_file(directory: Path) -> Path:
    """Find the most recent all_leads_*.json file."""
    files = sorted(directory.glob("all_leads_*.json"), reverse=True)
    if not files:
        print(f"❌ No all_leads_*.json files found in {directory}")
        sys.exit(1)
    return files[0]


def load_contacts(csv_path: Path) -> tuple[set[str], set[str]]:
    """Load emails and LinkedIn URLs from the contacts CSV."""
    emails: set[str] = set()
    linkedin_urls: set[str] = set()

    with open(csv_path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            email = (row.get("email") or "").strip().lower()
            linkedin = (row.get("linkedinUrl") or "").strip().lower().rstrip("/")
            if email:
                emails.add(email)
            if linkedin:
                linkedin_urls.add(linkedin)

    return emails, linkedin_urls


def main() -> None:
    # --- Load contacts to match against ---
    print(f"📂 Loading contacts: {CONTACTS_CSV}")
    emails, linkedin_urls = load_contacts(CONTACTS_CSV)
    print(f"   Emails to match:   {len(emails):,}")
    print(f"   LinkedIn to match: {len(linkedin_urls):,}")

    # --- Stream the leads JSON ---
    leads_path = LEADS_FILE or find_latest_leads_file(INPUT_DIR)
    print(f"📂 Scanning leads:   {leads_path}")
    print(f"   File size:        {leads_path.stat().st_size / (1024*1024):.1f} MB")

    start = time.time()

    matched_ids: set[str] = set()
    total = 0
    matched_by_email = 0
    matched_by_linkedin = 0

    with open(leads_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip().rstrip(",")
            if not line or line in ("[]", "[", "]"):
                continue
            try:
                lead = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1
            lead_id = lead.get("_id")
            if not lead_id:
                continue

            lead_email = (lead.get("email") or "").strip().lower()
            lead_linkedin = (lead.get("linkedinUrl") or "").strip().lower().rstrip("/")

            email_match = lead_email and lead_email in emails
            linkedin_match = lead_linkedin and lead_linkedin in linkedin_urls

            if email_match or linkedin_match:
                matched_ids.add(lead_id)
                if email_match:
                    matched_by_email += 1
                if linkedin_match:
                    matched_by_linkedin += 1

            if total % 50_000 == 0:
                print(f"   Processed {total:,} leads… ({len(matched_ids):,} matched so far)")

    elapsed = time.time() - start

    # --- Write output ---
    sorted_ids = sorted(matched_ids)
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["_id"])
        for lid in sorted_ids:
            writer.writerow([lid])

    print(f"\n✅ Done in {elapsed:.1f}s")
    print(f"   Total leads scanned:   {total:,}")
    print(f"   Matched by email:      {matched_by_email:,}")
    print(f"   Matched by LinkedIn:   {matched_by_linkedin:,}")
    print(f"   Unique matched IDs:    {len(matched_ids):,}")
    print(f"   Output:                {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
