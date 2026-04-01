# rolefetch

Python CLI to download job postings from employer career sites in **JSONL** or **CSV** for local filtering and parsing. Supported sources include **Apple** (`jobs.apple.com`), **Amazon** (`amazon.jobs`), and **Google** (`google.com/about/careers`).

## Install

```bash
cd rolefetch  # repository root
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Usage

```bash
rolefetch --version

# All active US roles: default output path is
#   data/raw/apple/YYYY-MM-DD/apple_united-states-USA_all.jsonl
python -m rolefetch apple

# Progress on stderr (page, new ids, totals)
python -m rolefetch apple -v --max-pages 3

# Keyword search + location (repeatable). Use either URL slugs or legacy postLocation ids:
python -m rolefetch apple --query "machine learning" \
  --location-id united-states-USA \
  --out apple_ml.jsonl
# (still accepts e.g. --location-id postLocation-USA — mapped via ISO country codes)

# Resolve location IDs from a free-text query (uses Apple's refdata API; picks first match)
python -m rolefetch apple --location-query "United States" --out apple_us.jsonl

# List candidate location IDs for a string
python -m rolefetch apple --list-locations "Germany"

# CSV instead of JSONL
python -m rolefetch apple --format csv --out apple_us.csv

# Errors only (no “Wrote N jobs …” summary)
python -m rolefetch apple -q -o apple_us.jsonl

# Amazon (uses the site’s search.json endpoint — fast JSON, not HTML)
python -m rolefetch amazon --loc-query "United States" -v --max-pages 2

# Full Amazon pull for a location (default out: data/raw/amazon/YYYY-MM-DD/…)
python -m rolefetch amazon --loc-query "United States" --query "software engineer"

# Google Careers (parses large HTML results pages; use --max-pages while testing)
python -m rolefetch google --location "United States" -v --max-pages 2

python -m rolefetch google --location "United States" --query "engineer"
```

### Output path convention

If you omit `--out` / `-o`, files go under:

`data/raw/apple/YYYY-MM-DD/apple_<first-location-slug>_all.jsonl`

(with `.csv` when using `--format csv`; multiple `--location-id` uses `apple_multi_all.*`).

Example explicit path (same folder layout):

```bash
run_date="$(date +%F)"
out_dir="data/raw/apple/${run_date}"
mkdir -p "${out_dir}"
python -m rolefetch apple \
  --location-id united-states-USA \
  --out "${out_dir}/apple_us_all.jsonl"
```

Amazon defaults look like:

`data/raw/amazon/YYYY-MM-DD/amazon_<loc-query-or-query-or-all>_all.jsonl`

Google defaults look like:

`data/raw/google/YYYY-MM-DD/google_<location-or-query-or-all>_all.jsonl`

### Apple note

Apple does not document a stable public API for bulk job export. Job listings are read from the **same HTML search pages** your browser loads (`/{locale}/search?...`), by parsing the embedded `__staticRouterHydrationData` payload. Location hints use `GET /api/v1/refData/postlocation` when you pass `--location-query` or `--list-locations`. Those mechanisms **may change** at any time. Use modest request pacing (`--page-delay`); comply with [Apple’s site terms](https://www.apple.com/legal/internet-services/terms/site.html) and applicable law.

### Amazon note

The CLI calls `https://www.amazon.jobs/{locale}/search.json` with the same query parameters the web UI uses (`base_query`, `loc_query`, `offset`, `result_limit`, etc.). That endpoint is **not documented as a public API** for third parties, may change or rate-limit at any time, and is subject to [Amazon’s site terms](https://www.amazon.com/gp/help/customer/display.html?nodeId=508088). Use `--page-delay` and modest `result_limit` values; respect robots and applicable law.

### Google note

The CLI downloads `https://www.google.com/about/careers/applications/jobs/results` HTML (the same paginated view the site serves to browsers) and extracts job links from the markup. Pages can be **large** and slow compared with Amazon’s JSON; pagination stops when a page returns **no listings** or **no new job ids**. This is **not a supported public API**, markup may change without notice, and automation may be constrained by [Google’s Terms of Service](https://policies.google.com/terms) and [robots.txt](https://www.google.com/robots.txt). Prefer generous `--timeout`, sensible `--page-delay`, and `--max-pages` while experimenting.

### Meta / metacareers (out of scope)

This project does **not** include scraping or automation for [Meta Careers](https://www.metacareers.com/) (`metacareers.com`). That site relies on internal GraphQL and similar mechanisms, and its `robots.txt` restricts automated collection without express permission. Adding Meta support would be fragile and legally sensitive, so it is intentionally not implemented here.

## Development

```bash
pytest
ruff check rolefetch tests
```

### Manual smoke test

With network access, run:

```bash
python -m rolefetch apple --max-pages 1 --page-delay 0 --timeout 20 -o /tmp/apple_sample.jsonl
```

Confirm the file is non-empty JSONL (`wc -l`, or `python -c "import json; print(json.loads(open('/tmp/apple_sample.jsonl').readline())['title'])"`).

**If you omit `--max-pages`**, the tool tries to download **every** page for the chosen location (for the United States that is often thousands of roles and hundreds of HTTP requests, so it can take a long time). Use `--max-pages` while testing, and keep `--timeout` modest (each stalled request fails after that many seconds instead of hanging indefinitely).
