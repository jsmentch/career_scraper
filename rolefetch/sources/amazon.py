from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List, Optional

import httpx

from rolefetch.models import Job

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_MAX_RESULT_LIMIT = 100
_MAX_OFFSET_PAGES = 5000


class AmazonAPIError(RuntimeError):
    """Raised when Amazon Jobs JSON search returns an error or unexpected payload."""


def _headers() -> Dict[str, str]:
    return {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
    }


def search_json_url(locale_prefix: str) -> str:
    """Build search.json URL (e.g. locale_prefix \"en\" -> /en/search.json)."""
    loc = locale_prefix.strip().strip("/") or "en"
    return f"https://www.amazon.jobs/{loc}/search.json"


def _humanize_amazon_label(label: str) -> str:
    """Turn slug-like team labels into plain words (hyphens/spaces)."""
    return label.replace("-", " ").replace("_", " ").strip()


def _amazon_team_str(value: Any) -> Optional[str]:
    """Pick a short team / job-family string; avoid dumping full API dicts."""
    if value is None:
        return None
    if isinstance(value, str):
        t = value.strip()
        return t or None
    if isinstance(value, dict):
        for key in ("title", "headline", "name"):
            v = value.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        lab = value.get("label")
        if isinstance(lab, str) and lab.strip():
            return _humanize_amazon_label(lab.strip())
        return None
    t = str(value).strip()
    return t or None


def _amazon_location_line(item: Any) -> Optional[str]:
    """One human-readable location line (drops embedded JSON metadata blobs)."""
    if item is None:
        return None
    if isinstance(item, dict):
        for key in ("display_name", "location", "normalizedLocation", "locationNonStemming"):
            v = item.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None
    if isinstance(item, str):
        s = item.strip()
        if not s:
            return None
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                return None
            if isinstance(obj, dict):
                for key in ("location", "normalizedLocation", "locationNonStemming", "display_name"):
                    v = obj.get(key)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
            return None
        return s
    return str(item).strip() or None


def _amazon_locations_list(record: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    top = _amazon_location_line(record.get("location"))
    if top:
        lines.append(top)
    multi = record.get("locations")
    if isinstance(multi, list):
        for item in multi:
            line = _amazon_location_line(item)
            if line:
                lines.append(line)
    deduped: List[str] = []
    seen: set[str] = set()
    for x in lines:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def normalize_amazon_job(
    record: Dict[str, Any],
    *,
    include_raw: bool,
    short_summary_only: bool = False,
) -> Job:
    external_id = str(record.get("id") or record.get("job_path") or "")
    path = (record.get("job_path") or "").strip()
    if path.startswith("/"):
        url = f"https://www.amazon.jobs{path}"
    elif path:
        url = f"https://www.amazon.jobs/{path}"
    else:
        url = ""

    title = str(record.get("title") or "").strip() or "(no title)"
    company = str(record.get("company_name") or "Amazon").strip()

    short_desc = record.get("description_short")
    if short_summary_only:
        summary = short_desc
    else:
        summary = short_desc or record.get("description")
    summary_str = str(summary).strip() if summary else None

    locs = _amazon_locations_list(record)

    posted = record.get("posted_date")
    posted_str = str(posted).strip() if posted else None

    team_str = _amazon_team_str(record.get("team")) or _amazon_team_str(
        record.get("job_family")
    )

    raw = dict(record) if include_raw else None
    return Job(
        source="amazon",
        external_id=external_id or path,
        title=title,
        company=company,
        url=url,
        posted_at=posted_str,
        summary=summary_str,
        team=team_str,
        locations=locs,
        raw=raw,
    )


def fetch_jobs(
    client: httpx.Client,
    *,
    base_query: str = "",
    loc_query: str = "",
    locale_prefix: str = "en",
    result_limit: int = 100,
    sort: str = "recent",
    page_delay_sec: float = 0.25,
    max_pages: Optional[int] = None,
    include_raw: bool = True,
    short_summary_only: bool = False,
    progress: Optional[Callable[[str], None]] = None,
) -> List[Job]:
    """
    Paginate ``/search.json`` until all reported hits are fetched or a page is empty.

    This uses the same JSON endpoint the amazon.jobs UI calls; it is not a documented
    public API and may change without notice.
    """
    if result_limit < 1 or result_limit > _MAX_RESULT_LIMIT:
        raise AmazonAPIError(
            f"result_limit must be 1..{_MAX_RESULT_LIMIT} (got {result_limit})."
        )

    url = search_json_url(locale_prefix)
    collected_by_id: Dict[str, Dict[str, Any]] = {}
    offset = 0
    total_reported: Optional[int] = None
    page_idx = 0

    while True:
        if max_pages is not None and page_idx >= max_pages:
            break
        if page_idx >= _MAX_OFFSET_PAGES:
            raise AmazonAPIError(
                f"Stopped after {_MAX_OFFSET_PAGES} pages to avoid an infinite loop."
            )

        params: Dict[str, Any] = {
            "result_limit": result_limit,
            "offset": offset,
            "sort": sort,
        }
        if base_query.strip():
            params["base_query"] = base_query.strip()
        if loc_query.strip():
            params["loc_query"] = loc_query.strip()

        r = client.get(url, params=params, headers=_headers())
        _raise_amazon_status(r)

        try:
            payload = r.json()
        except ValueError as e:
            raise AmazonAPIError(f"Non-JSON response: {r.text[:400]!r}") from e

        if not isinstance(payload, dict):
            raise AmazonAPIError(f"Expected JSON object, got {type(payload).__name__}.")

        err = payload.get("error")
        if err:
            raise AmazonAPIError(f"Amazon search error: {err!r}")

        if total_reported is None:
            hits = payload.get("hits")
            if hits is not None:
                try:
                    total_reported = int(hits)
                except (TypeError, ValueError):
                    total_reported = None

        batch = payload.get("jobs") or []
        if not isinstance(batch, list):
            raise AmazonAPIError("`jobs` is not a list.")

        if not batch:
            break

        before = len(collected_by_id)
        for item in batch:
            if not isinstance(item, dict):
                continue
            jid = str(item.get("id") or item.get("job_path") or "")
            if jid:
                collected_by_id.setdefault(jid, item)

        if progress is not None:
            added = len(collected_by_id) - before
            parts = [
                f"page {page_idx + 1}",
                f"offset {offset}",
                f"+{added} new",
                f"{len(collected_by_id)} unique",
            ]
            if total_reported is not None:
                parts.append(f"reported_total≈{total_reported}")
            progress("Amazon jobs — " + ", ".join(parts))

        offset += len(batch)
        page_idx += 1

        if total_reported is not None and offset >= total_reported:
            break

        time.sleep(page_delay_sec)

    return [
        normalize_amazon_job(
            rec,
            include_raw=include_raw,
            short_summary_only=short_summary_only,
        )
        for rec in collected_by_id.values()
    ]


def amazon_client(*, timeout: float = 30.0) -> httpx.Client:
    t = httpx.Timeout(
        timeout,
        connect=min(15.0, float(timeout)),
        read=float(timeout),
        write=min(30.0, float(timeout)),
        pool=min(15.0, float(timeout)),
    )
    return httpx.Client(timeout=t, follow_redirects=True, headers=_headers())


def _raise_amazon_status(response: httpx.Response) -> None:
    if response.status_code == 429:
        raise AmazonAPIError("HTTP 429: rate limited. Increase --page-delay and retry later.")
    if response.status_code == 403:
        raise AmazonAPIError(
            "HTTP 403: forbidden. Try again later or from another network."
        )
    if response.status_code >= 400:
        raise AmazonAPIError(
            f"HTTP {response.status_code}: {response.text[:400]!r}"
        )
