# Lemlist Campaign Leads Exporter

Export **all leads from every Lemlist campaign** into a single combined JSON file.

## Quick Start

```bash
pip install -r requirements.txt
python export_leads.py
```

You'll be prompted to paste your Lemlist API key (input is masked).

## What it does

1. Fetches all campaigns via the Lemlist API (paginated).
2. Exports leads from each campaign **concurrently** (with a progress bar).
3. Respects the **20 req / 2 s** rate limit with automatic retry on `429`.
4. Merges all leads into a single JSON with the **union of all columns**.
5. Tags each lead with `_campaignId` and `_campaignName` for traceability.

## Configuration

Edit the constants at the top of `export_leads.py`:

| Variable | Default | Description |
|---|---|---|
| `EXPORT_STATE` | `"all"` | Lead state filter (see API docs) |
| `MAX_WORKERS` | `5` | Concurrent export threads |
| `OUTPUT_DIR` | `data/` | Where the output JSON is saved |

## Output

Files land in `data/` (git-ignored) with a timestamped name, e.g.:

```
data/all_leads_20260210_143000.json
```
