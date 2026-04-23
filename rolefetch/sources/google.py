from __future__ import annotations

import html as html_lib
import re
import time
from dataclasses import replace
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import httpx

from rolefetch.models import Job

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

GOOGLE_APPLICATIONS_BASE = "https://www.google.com/about/careers/applications/"
GOOGLE_RESULTS_URL = urljoin(GOOGLE_APPLICATIONS_BASE, "jobs/results")

_MAX_SAFETY_PAGES = 2000

_JOB_DETAIL_SECTION_PREFIXES: Tuple[str, ...] = (
    "<h3>Minimum qualifications",
    "<h3>Preferred qualifications",
    "<h3>About the job</h3>",
    "<h3>Responsibilities</h3>",
)
_JOB_DETAIL_END_MARKER = '<div class="bE3reb">'

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


def _job_detail_url(job: Job) -> str:
    """Strip query string so the detail request matches the canonical posting URL."""
    parts = urlparse(job.url)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, "", ""))


def parse_job_detail_description(html: str) -> Optional[str]:
    """
    Extract the main posting HTML (qualifications, about, responsibilities) from a
    job detail page. Relies on headings and the standard legal footer wrapper; markup
    may change without notice.
    """
    starts: List[int] = []
    for prefix in _JOB_DETAIL_SECTION_PREFIXES:
        i = html.find(prefix)
        if i >= 0:
            starts.append(i)
    if not starts:
        return None
    start = min(starts)
    end = html.find(_JOB_DETAIL_END_MARKER, start)
    if end < 0:
        return None
    fragment = html[start:end].strip()
    return fragment or None


def _fetch_job_detail_description_html(client: httpx.Client, job: Job) -> Optional[str]:
    url = _job_detail_url(job)
    r = client.get(url, headers=_headers())
    _raise_google_status(r)
    return parse_job_detail_description(r.text)


def _enrich_jobs_with_job_descriptions(
    client: httpx.Client,
    jobs: List[Job],
    *,
    include_raw: bool,
    delay_sec: float,
    progress: Optional[Callable[[str], None]] = None,
) -> List[Job]:
    """One GET per job posting page; sets ``summary`` to extracted HTML fragment."""
    out: List[Job] = []
    n = len(jobs)
    for i, job in enumerate(jobs):
        if i > 0:
            time.sleep(delay_sec)
        jd_html = _fetch_job_detail_description_html(client, job)
        new_summary = jd_html.strip() if jd_html else None

        if include_raw:
            merged: Dict[str, Any] = dict(job.raw) if job.raw else {}
            if new_summary:
                merged["jobDescriptionHtml"] = new_summary
            out.append(replace(job, summary=new_summary, raw=merged))
        else:
            out.append(replace(job, summary=new_summary))

        if progress is not None:
            progress(
                f"Google JDs — {i + 1}/{n} id {job.external_id}"
                + ("" if new_summary else " (no description block matched)")
            )

    return out


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
    fetch_details: bool = False,
    detail_delay_sec: Optional[float] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> List[Job]:
    """
    Paginate HTML results (``jobs/results?...&page=``) until a page adds no new job ids,
    returns no listings, or ``max_pages`` is reached.

    When ``fetch_details`` is true, performs one GET per job on the posting URL and
    parses the main description HTML into ``summary`` (and ``raw.jobDescriptionHtml``
    when ``include_raw`` is true).

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

    jobs = list(collected.values())
    if fetch_details and jobs:
        d_delay = detail_delay_sec if detail_delay_sec is not None else page_delay_sec
        jobs = _enrich_jobs_with_job_descriptions(
            client,
            jobs,
            include_raw=include_raw,
            delay_sec=float(d_delay),
            progress=progress,
        )

    return jobs


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
