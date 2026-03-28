from __future__ import annotations

import json
import re
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode

import httpx
import pycountry

from career_scraper.models import Job

POSTLOCATION_URL = "https://jobs.apple.com/api/v1/refData/postlocation"

_HYDRATION_RE = re.compile(
    r'window\.__staticRouterHydrationData\s*=\s*JSON\.parse\("((?:\\.|[^"\\])*)"\)',
    re.DOTALL,
)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Safety cap: malformed responses should never spin forever.
_MAX_SEARCH_PAGES = 5000


class AppleAPIError(RuntimeError):
    """Raised when Apple's jobs site returns an unexpected or error response."""


def _html_headers(locale: str) -> Dict[str, str]:
    lc = locale.lower().replace("_", "-")
    return {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://jobs.apple.com/{lc}/search",
    }


def _api_headers(locale: str) -> Dict[str, str]:
    lc = locale.lower().replace("_", "-")
    return {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://jobs.apple.com",
        "Referer": f"https://jobs.apple.com/{lc}/search",
    }


def parse_search_from_hydration_html(html: str) -> Dict[str, Any]:
    """Return the `loaderData.search` object from the SSR hydration payload."""
    m = _HYDRATION_RE.search(html)
    if not m:
        raise AppleAPIError(
            "Could not find embedded search data in the HTML page. "
            "Apple may have changed jobs.apple.com; open an issue with a saved HTML sample."
        )
    escaped = m.group(1)
    inner_json = json.loads('"' + escaped + '"')
    data = json.loads(inner_json)
    loader = data.get("loaderData")
    if not isinstance(loader, dict):
        raise AppleAPIError("Hydration payload missing loaderData.")
    search = loader.get("search")
    if not isinstance(search, dict):
        raise AppleAPIError("Hydration payload missing loaderData.search.")
    return search


def ref_record_to_location_slug(record: Dict[str, Any]) -> str:
    """Build the `location=` query value used on the public search URL."""
    name = record.get("name_en_US") or record.get("displayName") or record.get("name") or ""
    code = record.get("code") or ""
    name = str(name).strip()
    code = str(code).strip()
    if not name or not code:
        raise AppleAPIError(f"Location record missing name or code: {record!r}")
    kebab = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return f"{kebab}-{code}"


def postlocation_id_to_slug(postlocation_id: str) -> str:
    """Map e.g. postLocation-USA to united-states-USA using ISO 3166-1 alpha-3."""
    m = re.match(r"^postLocation-([A-Za-z0-9]{3})$", postlocation_id.strip())
    if not m:
        raise AppleAPIError(
            f"Unrecognized postLocation id {postlocation_id!r}; "
            "expected form like postLocation-USA."
        )
    alpha3 = m.group(1).upper()
    country = pycountry.countries.get(alpha_3=alpha3)
    if country is None:
        raise AppleAPIError(f"Unknown country code {alpha3!r} in {postlocation_id!r}.")
    return ref_record_to_location_slug(
        {"name_en_US": country.name, "code": alpha3}
    )


def fetch_postlocation_matches(
    client: httpx.Client,
    *,
    input_query: str,
    locale: str = "en-us",
) -> List[Dict[str, Any]]:
    """Return candidate location objects from Apple's refdata endpoint (shape varies)."""
    params = {"input": input_query}
    r = client.get(POSTLOCATION_URL, params=params, headers=_api_headers(locale))
    _raise_for_apple_status(r)
    data = r.json()
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        res = data.get("res")
        if isinstance(res, list):
            return [x for x in res if isinstance(x, dict)]
        for key in ("results", "data", "locations", "postLocations"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
    return []


def resolve_location_slug(
    client: httpx.Client,
    *,
    location_query: str,
    locale: str = "en-us",
    pick_index: int = 0,
) -> str:
    matches = fetch_postlocation_matches(client, input_query=location_query, locale=locale)
    if not matches:
        raise AppleAPIError(f"No location matches for query: {location_query!r}")
    if pick_index < 0 or pick_index >= len(matches):
        raise AppleAPIError(
            f"Location pick index {pick_index} out of range (0..{len(matches) - 1})"
        )
    return ref_record_to_location_slug(matches[pick_index])


def normalize_apple_job(record: Dict[str, Any], *, locale: str, include_raw: bool) -> Job:
    external_id = str(record.get("id") or record.get("reqId") or record.get("positionId", ""))
    position_id = str(record.get("positionId") or "").strip()
    if not position_id and external_id:
        if external_id.startswith("PIPE-"):
            position_id = external_id.removeprefix("PIPE-")
        else:
            position_id = external_id
    lc = locale.lower().replace("_", "-")
    url = f"https://jobs.apple.com/{lc}/details/{position_id}" if position_id else ""

    title = str(record.get("postingTitle") or record.get("transformedPostingTitle") or "").strip()
    summary = record.get("jobSummary")
    summary_str = str(summary).strip() if summary is not None else None

    team_name = None
    team = record.get("team")
    if isinstance(team, dict):
        tn = team.get("teamName")
        if tn is not None:
            team_name = str(tn).strip() or None

    locs: List[str] = []
    raw_locs = record.get("locations")
    if isinstance(raw_locs, list):
        for loc in raw_locs:
            if not isinstance(loc, dict):
                continue
            name = loc.get("name") or loc.get("city") or loc.get("countryName")
            if name:
                locs.append(str(name).strip())
            elif loc.get("countryName"):
                locs.append(str(loc["countryName"]).strip())

    posted = record.get("postDateInGMT")
    posted_str = str(posted).strip() if posted else None

    raw = dict(record) if include_raw else None
    return Job(
        source="apple",
        external_id=external_id or position_id,
        title=title or "(no title)",
        company="Apple",
        url=url,
        posted_at=posted_str,
        summary=summary_str,
        team=team_name,
        locations=locs,
        raw=raw,
    )


def _search_page_url(
    locale: str,
    *,
    location_slug: str,
    search_query: str,
    page: int,
) -> str:
    lc = locale.lower().replace("_", "-")
    params: List[tuple[str, str]] = [("location", location_slug)]
    if search_query.strip():
        params.append(("search", search_query.strip()))
    if page > 1:
        params.append(("page", str(page)))
    return f"https://jobs.apple.com/{lc}/search?{urlencode(params)}"


def fetch_jobs_for_locations(
    client: httpx.Client,
    *,
    location_ids: List[str],
    query: str = "",
    locale: str = "en-us",
    page_delay_sec: float = 0.35,
    max_pages: Optional[int] = None,
    include_raw: bool = True,
    progress: Optional[Callable[[str], None]] = None,
) -> List[Job]:
    """
    Fetch jobs by walking the public search HTML (SSR hydration), paginated with ?page=.

    ``location_ids`` may be either URL slugs (``united-states-USA``) or legacy ids
    (``postLocation-USA``); the latter are converted with pycountry.
    """
    if not location_ids:
        raise AppleAPIError("At least one location is required.")

    slugs: List[str] = []
    for loc in location_ids:
        loc = loc.strip()
        if loc.startswith("postLocation-"):
            slugs.append(postlocation_id_to_slug(loc))
        else:
            slugs.append(loc)

    collected: Dict[str, Dict[str, Any]] = {}
    for slug in slugs:
        page = 1
        total_records: Optional[int] = None
        page_size_hint: Optional[int] = None
        pages_needed: Optional[int] = None

        if progress is not None:
            progress(
                f"Location {slug!r} — starting fetch"
                + (f" (search query: {query!r})" if query.strip() else "")
            )

        while True:
            if page > _MAX_SEARCH_PAGES:
                raise AppleAPIError(
                    f"Stopped after {_MAX_SEARCH_PAGES} pages for location={slug!r} "
                    "to avoid an infinite loop (unexpected response from Apple)."
                )
            if max_pages is not None and page > max_pages:
                break

            url = _search_page_url(locale, location_slug=slug, search_query=query, page=page)
            r = client.get(url, headers=_html_headers(locale))
            _raise_for_apple_status(r)
            if "text/html" not in (r.headers.get("content-type") or "").lower():
                raise AppleAPIError(
                    f"Expected HTML from search page, got {r.headers.get('content-type')!r}."
                )

            try:
                search = parse_search_from_hydration_html(r.text)
            except (json.JSONDecodeError, AppleAPIError) as e:
                raise AppleAPIError(
                    f"Failed to parse search data from {url}: {e}"
                ) from e

            batch = search.get("searchResults") or []
            if not isinstance(batch, list):
                raise AppleAPIError("searchResults is not a list in hydration data.")

            if not batch:
                break

            if page == 1:
                tr = search.get("totalRecords")
                if tr is not None:
                    try:
                        total_records = int(tr)
                    except (TypeError, ValueError):
                        total_records = None
                if len(batch) > 0:
                    page_size_hint = len(batch)
                    if (
                        total_records is not None
                        and total_records > 0
                        and page_size_hint > 0
                    ):
                        pages_needed = (total_records + page_size_hint - 1) // page_size_hint

            before = len(collected)
            for item in batch:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("id") or item.get("reqId") or item.get("positionId", ""))
                if key:
                    collected.setdefault(key, item)

            if len(collected) == before:
                if progress is not None:
                    progress(
                        f"Location {slug!r} — page {page}: no new job ids "
                        f"(batch size {len(batch)}); stopping."
                    )
                break

            if progress is not None:
                added = len(collected) - before
                parts = [
                    f"page {page}",
                    f"+{added} new",
                    f"{len(collected)} unique",
                ]
                if total_records is not None:
                    parts.append(f"site_total≈{total_records}")
                if pages_needed is not None and max_pages is None:
                    parts.append(f"~{pages_needed} pages")
                if max_pages is not None:
                    parts.append(f"max_pages={max_pages}")
                progress(f"Location {slug!r} — " + ", ".join(parts))

            if (
                pages_needed is not None
                and max_pages is None
                and page >= pages_needed
            ):
                break

            page += 1
            time.sleep(page_delay_sec)

    return [
        normalize_apple_job(rec, locale=locale, include_raw=include_raw)
        for rec in collected.values()
    ]


def apple_client(*, locale: str = "en-us", timeout: float = 30.0) -> httpx.Client:
    t = httpx.Timeout(
        timeout,
        connect=min(15.0, float(timeout)),
        read=float(timeout),
        write=min(30.0, float(timeout)),
        pool=min(15.0, float(timeout)),
    )
    return httpx.Client(
        headers=_html_headers(locale),
        timeout=t,
        follow_redirects=True,
    )


def _raise_for_apple_status(response: httpx.Response) -> None:
    if response.status_code == 429:
        raise AppleAPIError("HTTP 429: rate limited. Increase --page-delay or retry later.")
    if response.status_code == 403:
        raise AppleAPIError(
            "HTTP 403: forbidden. Apple's edge may block automated clients; try another network."
        )
    if response.status_code >= 400:
        raise AppleAPIError(
            f"HTTP {response.status_code}: {response.text[:400]!r}"
        )
