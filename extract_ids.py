"""
Extract & Deduplicate Lead IDs
==============================
Reads the combined leads JSON, extracts unique _id values,
and writes them to a CSV file.
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

# Auto-detect: pick the latest all_leads_*.json in INPUT_DIR
INPUT_FILE = None  # set to a specific Path to override auto-detect


def find_latest_leads_file(directory: Path) -> Path:
    """Find the most recent all_leads_*.json file."""
    files = sorted(directory.glob("all_leads_*.json"), reverse=True)
    if not files:
        print(f"❌ No all_leads_*.json files found in {directory}")
        sys.exit(1)
    return files[0]


def main() -> None:
    input_path = INPUT_FILE or find_latest_leads_file(INPUT_DIR)
    print(f"📂 Reading: {input_path}")
    print(f"   File size: {input_path.stat().st_size / (1024*1024):.1f} MB")

    start = time.time()

    # Stream-parse: read line by line to keep memory low
    # The file is a JSON array with one object per line (our export format)
    seen_ids: set[str] = set()
    total = 0
    missing = 0

    with open(input_path, "r", encoding="utf-8") as fh:
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
            if lead_id:
                seen_ids.add(lead_id)
            else:
                missing += 1

            if total % 50_000 == 0:
                print(f"   Processed {total:,} leads… ({len(seen_ids):,} unique so far)")

    elapsed = time.time() - start

    # Write output
    output_path = OUTPUT_DIR / "unique_lead_ids.csv"
    sorted_ids = sorted(seen_ids)
    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["_id"])
        for lid in sorted_ids:
            writer.writerow([lid])

    print(f"\n✅ Done in {elapsed:.1f}s")
    print(f"   Total leads:    {total:,}")
    print(f"   Unique _id:     {len(seen_ids):,}")
    print(f"   Duplicates:     {total - len(seen_ids) - missing:,}")
    if missing:
        print(f"   Missing _id:    {missing:,}")
    print(f"   Output:         {output_path}")


if __name__ == "__main__":
    main()
