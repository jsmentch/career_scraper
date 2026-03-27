# career-scraper

Python CLI to download job postings from employer career sites in **JSONL** or **CSV** for local filtering and parsing. The first supported source is **Apple** (`jobs.apple.com`).

## Install

```bash
cd /Users/jeff/Documents/career_scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Usage

```bash
# All active roles in the United States (default location filter)
python -m career_scraper apple --out apple_us.jsonl

# Keyword search + location (repeatable). Use either URL slugs or legacy postLocation ids:
python -m career_scraper apple --query "machine learning" \
  --location-id united-states-USA \
  --out apple_ml.jsonl
# (still accepts e.g. --location-id postLocation-USA — mapped via ISO country codes)

# Resolve location IDs from a free-text query (uses Apple's refdata API; picks first match)
python -m career_scraper apple --location-query "United States" --out apple_us.jsonl

# List candidate location IDs for a string
python -m career_scraper apple --list-locations "Germany"

# CSV instead of JSONL
python -m career_scraper apple --format csv --out apple_us.csv
```

### Output path convention

Use this folder format for raw pulls:

`data/raw/apple/YYYY-MM-DD/apple_us_all.jsonl`

Example command:

```bash
run_date="$(date +%F)"
out_dir="data/raw/apple/${run_date}"
mkdir -p "${out_dir}"
python -m career_scraper apple \
  --location-id united-states-USA \
  --out "${out_dir}/apple_us_all.jsonl"
```

### Apple note

Apple does not document a stable public API for bulk job export. Job listings are read from the **same HTML search pages** your browser loads (`/{locale}/search?...`), by parsing the embedded `__staticRouterHydrationData` payload. Location hints use `GET /api/v1/refData/postlocation` when you pass `--location-query` or `--list-locations`. Those mechanisms **may change** at any time. Use modest request pacing (`--page-delay`); comply with [Apple’s site terms](https://www.apple.com/legal/internet-services/terms/site.html) and applicable law.

## Development

```bash
pytest
ruff check career_scraper tests
```

### Manual smoke test

With network access, run:

```bash
python -m career_scraper apple --max-pages 1 --page-delay 0 --timeout 20 -o /tmp/apple_sample.jsonl
```

Confirm the file is non-empty JSONL (`wc -l`, or `python -c "import json; print(json.loads(open('/tmp/apple_sample.jsonl').readline())['title'])"`).

**If you omit `--max-pages`**, the tool tries to download **every** page for the chosen location (for the United States that is often thousands of roles and hundreds of HTTP requests, so it can take a long time). Use `--max-pages` while testing, and keep `--timeout` modest (each stalled request fails after that many seconds instead of hanging indefinitely).
