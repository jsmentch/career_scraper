from __future__ import annotations

import html as html_lib
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from rolefetch.models import Job

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

GOOGLE_APPLICATIONS_BASE = "https://www.google.com/about/careers/applications/"
GOOGLE_RESULTS_URL = urljoin(GOOGLE_APPLICATIONS_BASE, "jobs/results")

_MAX_SAFETY_PAGES = 2000

_ANCHOR_RE = re.compile(r"<a\s+([^>]+)>", re.IGNORECASE)
_HREF_RE = re.compile(
    r'href\s*=\s*["\'](jobs/results/\d+-[^"\']+)["\']',
    re.IGNORECASE,
)
_ARIA_TITLE_RE = re.compile(
    r'aria-label\s*=\s*["\']Learn more about\s+(.+?)["\']',
    re.IGNORECASE,
)
_EXTERNAL_ID_RE = re.compile(r"jobs/results/(\d+)-", re.IGNORECASE)


class GoogleCareersError(RuntimeError):
    """Raised when Google Careers HTML listing fetch fails or the page shape is unexpected."""


def _headers() -> Dict[str, str]:
    return {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def parse_results_page(html: str) -> List[Tuple[str, str, str]]:
    """
    Extract job rows from a careers results HTML page.

    Returns list of (external_id, title, relative_href_path) preserving first-seen order.
    """
    rows: List[Tuple[str, str, str]] = []
    seen: set[str] = set()

    for m in _ANCHOR_RE.finditer(html):
        attrs = m.group(1)
        hm = _HREF_RE.search(attrs)
        if not hm:
            continue
        raw_path = html_lib.unescape(hm.group(1).strip())
        idm = _EXTERNAL_ID_RE.match(raw_path)
        if not idm:
            continue
        eid = idm.group(1)
        if eid in seen:
            continue
        seen.add(eid)
        am = _ARIA_TITLE_RE.search(attrs)
        title = am.group(1).strip() if am else "(no title)"
        rows.append((eid, title, raw_path))

    return rows


def _locations_from_href_path(path: str) -> List[str]:
    q = urlparse(path).query
    if not q:
        return []
    vals = parse_qs(q).get("location")
    if not vals:
        return []
    return [html_lib.unescape(v.strip()) for v in vals if v and v.strip()]


def _absolute_job_url(relative_path: str) -> str:
    rel = relative_path.lstrip("/")
    return urljoin(GOOGLE_APPLICATIONS_BASE, rel)


def normalize_google_row(
    external_id: str,
    title: str,
    relative_path: str,
    *,
    include_raw: bool,
    page_num: int,
) -> Job:
    url = _absolute_job_url(relative_path)
    locs = _locations_from_href_path(relative_path)
    raw: Optional[Dict[str, Any]] = None
    if include_raw:
        raw = {
            "relative_href": relative_path,
            "results_page": page_num,
        }
    return Job(
        source="google",
        external_id=external_id,
        title=title,
        company="Google",
        url=url,
        posted_at=None,
        summary=None,
        team=None,
        locations=locs,
        raw=raw,
    )


def fetch_jobs(
    client: httpx.Client,
    *,
    location: str = "",
    query: str = "",
    page_delay_sec: float = 0.5,
    max_pages: Optional[int] = None,
    include_raw: bool = True,
    progress: Optional[Callable[[str], None]] = None,
) -> List[Job]:
    """
    Paginate HTML results (``jobs/results?...&page=``) until a page adds no new job ids,
    returns no listings, or ``max_pages`` is reached.

    Listing markup is undocumented and may change without notice.
    """
    collected: Dict[str, Job] = {}
    page_num = 1

    while True:
        if max_pages is not None and page_num > max_pages:
            break
        if page_num > _MAX_SAFETY_PAGES:
            raise GoogleCareersError(
                f"Stopped after {_MAX_SAFETY_PAGES} pages to avoid an infinite loop."
            )

        params: Dict[str, Any] = {"page": page_num}
        if location.strip():
            params["location"] = location.strip()
        if query.strip():
            params["q"] = query.strip()

        r = client.get(GOOGLE_RESULTS_URL, params=params, headers=_headers())
        _raise_google_status(r)

        html = r.text
        batch = parse_results_page(html)

        if not batch:
            break

        before = len(collected)
        for eid, title, rel in batch:
            if eid in collected:
                continue
            collected[eid] = normalize_google_row(
                eid,
                title,
                rel,
                include_raw=include_raw,
                page_num=page_num,
            )

        added = len(collected) - before
        if progress is not None:
            progress(
                "Google careers — "
                f"page {page_num}, +{added} new, {len(collected)} unique so far"
            )

        if added == 0:
            break

        page_num += 1
        time.sleep(page_delay_sec)

    return list(collected.values())


def google_client(*, timeout: float = 45.0) -> httpx.Client:
    t = httpx.Timeout(
        timeout,
        connect=min(15.0, float(timeout)),
        read=float(timeout),
        write=min(30.0, float(timeout)),
        pool=min(15.0, float(timeout)),
    )
    return httpx.Client(timeout=t, follow_redirects=True, headers=_headers())


def _raise_google_status(response: httpx.Response) -> None:
    if response.status_code == 429:
        raise GoogleCareersError(
            "HTTP 429: rate limited. Increase --page-delay and retry later."
        )
    if response.status_code == 403:
        raise GoogleCareersError(
            "HTTP 403: forbidden. Try again later or from another network."
        )
    if response.status_code >= 400:
        raise GoogleCareersError(
            f"HTTP {response.status_code}: {response.text[:400]!r}"
        )
