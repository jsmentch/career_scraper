from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

from rolefetch import __version__
from rolefetch.export import write_csv, write_jsonl
from rolefetch.sources.amazon import (
    AmazonAPIError,
    amazon_client,
)
from rolefetch.sources.amazon import (
    fetch_jobs as amazon_fetch_jobs,
)
from rolefetch.sources.apple import (
    AppleAPIError,
    apple_client,
    fetch_jobs_for_locations,
    fetch_postlocation_matches,
    resolve_location_slug,
)
from rolefetch.sources.google import (
    GoogleCareersError,
    google_client,
)
from rolefetch.sources.google import (
    fetch_jobs as google_fetch_jobs,
)


def _default_amazon_out_path(*, loc_query: str, base_query: str, fmt: str) -> Path:
    run_date = date.today().isoformat()
    if loc_query.strip():
        part = re.sub(r"[^a-zA-Z0-9_.-]+", "_", loc_query.strip())[:80]
    elif base_query.strip():
        part = re.sub(r"[^a-zA-Z0-9_.-]+", "_", base_query.strip())[:80]
    else:
        part = "all"
    ext = "csv" if fmt == "csv" else "jsonl"
    return Path(f"data/raw/amazon/{run_date}/amazon_{part}_all.{ext}")


def _default_google_out_path(*, location: str, query: str, fmt: str) -> Path:
    run_date = date.today().isoformat()
    if location.strip():
        part = re.sub(r"[^a-zA-Z0-9_.-]+", "_", location.strip())[:80]
    elif query.strip():
        part = re.sub(r"[^a-zA-Z0-9_.-]+", "_", query.strip())[:80]
    else:
        part = "all"
    ext = "csv" if fmt == "csv" else "jsonl"
    return Path(f"data/raw/google/{run_date}/google_{part}_all.{ext}")


def _default_apple_out_path(location_ids: list[str], *, fmt: str) -> Path:
    run_date = date.today().isoformat()
    if len(location_ids) == 1:
        part = re.sub(r"[^a-zA-Z0-9_.-]+", "_", location_ids[0])
    else:
        part = "multi"
    ext = "csv" if fmt == "csv" else "jsonl"
    return Path(f"data/raw/apple/{run_date}/apple_{part}_all.{ext}")


def _cmd_apple(args: argparse.Namespace) -> int:
    if args.list_locations is not None:
        with apple_client(locale=args.locale, timeout=args.timeout) as client:
            matches = fetch_postlocation_matches(
                client, input_query=args.list_locations, locale=args.locale
            )
        if not matches:
            print("No matches.", file=sys.stderr)
            return 1
        for i, m in enumerate(matches):
            lid = m.get("id") or m.get("postLocationId") or m.get("locationId", "")
            name = (
                m.get("displayName")
                or m.get("name")
                or m.get("label")
                or m.get("title")
                or m
            )
            print(f"{i}\t{lid}\t{name}")
        return 0

    location_ids: list[str] = list(args.location_id or [])
    with apple_client(locale=args.locale, timeout=args.timeout) as client:
        if args.location_query:
            slug = resolve_location_slug(
                client,
                location_query=args.location_query,
                locale=args.locale,
                pick_index=args.location_index,
            )
            location_ids.append(slug)
        if not location_ids:
            location_ids = ["united-states-USA"]

        out_path = Path(args.out) if args.out else _default_apple_out_path(
            location_ids, fmt=args.format
        )
        if args.verbose and not args.quiet:
            print(f"Output path: {out_path}", file=sys.stderr)

        progress_cb = (lambda m: print(m, file=sys.stderr)) if args.verbose else None

        try:
            jobs = fetch_jobs_for_locations(
                client,
                location_ids=location_ids,
                query=args.query,
                locale=args.locale,
                page_delay_sec=args.page_delay,
                max_pages=args.max_pages,
                include_raw=not args.no_raw,
                progress=progress_cb,
            )
        except AppleAPIError as e:
            print(f"Apple API error: {e}", file=sys.stderr)
            return 1

    if args.format == "csv":
        write_csv(jobs, out_path)
    else:
        write_jsonl(jobs, out_path, include_raw=not args.no_raw)

    if not args.quiet:
        print(f"Wrote {len(jobs)} jobs to {out_path}", file=sys.stderr)
    return 0


def _cmd_amazon(args: argparse.Namespace) -> int:
    out_path = (
        Path(args.out)
        if args.out
        else _default_amazon_out_path(
            loc_query=args.loc_query,
            base_query=args.query,
            fmt=args.format,
        )
    )
    if args.verbose and not args.quiet:
        print(f"Output path: {out_path}", file=sys.stderr)

    progress_cb = (lambda m: print(m, file=sys.stderr)) if args.verbose else None

    with amazon_client(timeout=args.timeout) as client:
        try:
            jobs = amazon_fetch_jobs(
                client,
                base_query=args.query,
                loc_query=args.loc_query,
                locale_prefix=args.locale,
                result_limit=args.result_limit,
                sort=args.sort,
                page_delay_sec=args.page_delay,
                max_pages=args.max_pages,
                include_raw=not args.no_raw,
                progress=progress_cb,
            )
        except AmazonAPIError as e:
            print(f"Amazon jobs error: {e}", file=sys.stderr)
            return 1

    if args.format == "csv":
        write_csv(jobs, out_path)
    else:
        write_jsonl(jobs, out_path, include_raw=not args.no_raw)

    if not args.quiet:
        print(f"Wrote {len(jobs)} jobs to {out_path}", file=sys.stderr)
    return 0


def _cmd_google(args: argparse.Namespace) -> int:
    out_path = (
        Path(args.out)
        if args.out
        else _default_google_out_path(
            location=args.location,
            query=args.query,
            fmt=args.format,
        )
    )
    if args.verbose and not args.quiet:
        print(f"Output path: {out_path}", file=sys.stderr)

    progress_cb = (lambda m: print(m, file=sys.stderr)) if args.verbose else None

    with google_client(timeout=args.timeout) as client:
        try:
            jobs = google_fetch_jobs(
                client,
                location=args.location,
                query=args.query,
                page_delay_sec=args.page_delay,
                max_pages=args.max_pages,
                include_raw=not args.no_raw,
                progress=progress_cb,
            )
        except GoogleCareersError as e:
            print(f"Google careers error: {e}", file=sys.stderr)
            return 1

    if args.format == "csv":
        write_csv(jobs, out_path)
    else:
        write_jsonl(jobs, out_path, include_raw=not args.no_raw)

    if not args.quiet:
        print(f"Wrote {len(jobs)} jobs to {out_path}", file=sys.stderr)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rolefetch",
        description="Download job listings from employer career sites.",
    )
    p.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    sub = p.add_subparsers(dest="command", required=True)

    apple = sub.add_parser("apple", help="Fetch listings from jobs.apple.com")
    apple.add_argument("--query", default="", help="Search query string (default: empty).")
    apple.add_argument(
        "--locale",
        default="en-us",
        help="Locale path segment (default: en-us).",
    )
    apple.add_argument(
        "--location-id",
        action="append",
        dest="location_id",
        metavar="SLUG_OR_ID",
        help=(
            "Location filter: URL slug (e.g. united-states-USA) or legacy postLocation-XXX "
            "(repeatable)."
        ),
    )
    apple.add_argument(
        "--location-query",
        help="Resolve a location via Apple's refdata API (see also --location-index).",
    )
    apple.add_argument(
        "--location-index",
        type=int,
        default=0,
        help="When using --location-query, pick this match index (default: 0).",
    )
    apple.add_argument(
        "--list-locations",
        metavar="TEXT",
        help="Print candidate location ids for TEXT and exit (tab-separated: index, id, label).",
    )
    apple.add_argument(
        "--out",
        "-o",
        metavar="PATH",
        help=(
            "Output file path. If omitted, writes under data/raw/apple/YYYY-MM-DD/ "
            "based on today's date and the first location (see README)."
        ),
    )
    apple.add_argument(
        "--format",
        choices=("jsonl", "csv"),
        default="jsonl",
        help="Output format (default: jsonl).",
    )
    log = apple.add_mutually_exclusive_group()
    log.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress messages to stderr while fetching.",
    )
    log.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only print errors (no summary line).",
    )
    apple.add_argument(
        "--page-delay",
        type=float,
        default=0.35,
        metavar="SEC",
        help="Delay between paginated requests (default: 0.35).",
    )
    apple.add_argument(
        "--max-pages",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N pages (for testing).",
    )
    apple.add_argument(
        "--no-raw",
        action="store_true",
        help="Omit raw API payload from JSONL output.",
    )
    apple.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30).",
    )
    apple.set_defaults(func=_cmd_apple)

    amazon = sub.add_parser("amazon", help="Fetch listings from amazon.jobs (search.json)")
    amazon.add_argument(
        "--query",
        default="",
        metavar="TEXT",
        help="Keyword / base_query filter (default: empty).",
    )
    amazon.add_argument(
        "--loc-query",
        default="",
        metavar="TEXT",
        help='Location filter passed as loc_query (default: empty for unscoped search).',
    )
    amazon.add_argument(
        "--locale",
        default="en",
        help='Locale path prefix for search.json (default: en → /en/search.json).',
    )
    amazon.add_argument(
        "--sort",
        default="recent",
        help="Sort order (default: recent).",
    )
    amazon.add_argument(
        "--result-limit",
        type=int,
        default=100,
        metavar="N",
        help="Jobs per request, max 100 (default: 100).",
    )
    amazon.add_argument(
        "--out",
        "-o",
        metavar="PATH",
        help=(
            "Output file path. If omitted, writes under data/raw/amazon/YYYY-MM-DD/ "
            "from loc_query, query, or 'all' (see README)."
        ),
    )
    amazon.add_argument(
        "--format",
        choices=("jsonl", "csv"),
        default="jsonl",
        help="Output format (default: jsonl).",
    )
    log_a = amazon.add_mutually_exclusive_group()
    log_a.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress messages to stderr while fetching.",
    )
    log_a.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only print errors (no summary line).",
    )
    amazon.add_argument(
        "--page-delay",
        type=float,
        default=0.25,
        metavar="SEC",
        help="Delay between paginated requests (default: 0.25).",
    )
    amazon.add_argument(
        "--max-pages",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N pages (for testing).",
    )
    amazon.add_argument(
        "--no-raw",
        action="store_true",
        help="Omit raw API payload from JSONL output.",
    )
    amazon.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30).",
    )
    amazon.set_defaults(func=_cmd_amazon)

    google = sub.add_parser(
        "google",
        help="Fetch listings from Google Careers (HTML results pages)",
    )
    google.add_argument(
        "--location",
        default="",
        metavar="TEXT",
        help=(
            "Location filter passed to the site (e.g. \"United States\"). "
            "Leave empty for unscoped search."
        ),
    )
    google.add_argument(
        "--query",
        default="",
        metavar="TEXT",
        help="Keyword filter passed as q= (default: empty).",
    )
    google.add_argument(
        "--out",
        "-o",
        metavar="PATH",
        help=(
            "Output file path. If omitted, writes under data/raw/google/YYYY-MM-DD/ "
            "from --location, --query, or 'all' (see README)."
        ),
    )
    google.add_argument(
        "--format",
        choices=("jsonl", "csv"),
        default="jsonl",
        help="Output format (default: jsonl).",
    )
    log_g = google.add_mutually_exclusive_group()
    log_g.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress messages to stderr while fetching.",
    )
    log_g.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only print errors (no summary line).",
    )
    google.add_argument(
        "--page-delay",
        type=float,
        default=0.5,
        metavar="SEC",
        help="Delay between paginated requests (default: 0.5).",
    )
    google.add_argument(
        "--max-pages",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N pages (for testing).",
    )
    google.add_argument(
        "--no-raw",
        action="store_true",
        help="Omit raw listing payload from JSONL output.",
    )
    google.add_argument(
        "--timeout",
        type=float,
        default=45.0,
        help="HTTP timeout in seconds (default: 45).",
    )
    google.set_defaults(func=_cmd_google)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
