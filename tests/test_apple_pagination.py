"""Unit tests for Apple search pagination without calling the network."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from career_scraper.sources.apple import fetch_jobs_for_locations, parse_search_from_hydration_html

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _hydration_page_html(search: dict) -> str:
    payload = {"loaderData": {"search": search}}
    inner = json.dumps(payload, separators=(",", ":"))
    escaped = json.dumps(inner)[1:-1]
    return (
        "<!DOCTYPE html><html><script>"
        f'window.__staticRouterHydrationData = JSON.parse("{escaped}");'
        "</script></html>"
    )


def test_parse_search_from_fixture_record() -> None:
    record = json.loads((FIXTURES / "apple_search_page.json").read_text(encoding="utf-8"))[
        "searchResults"
    ][0]
    html = _hydration_page_html({"searchResults": [record], "totalRecords": 1})
    search = parse_search_from_hydration_html(html)
    assert len(search["searchResults"]) == 1
    assert search["searchResults"][0]["positionId"] == "114438206"


def _make_transport() -> httpx.MockTransport:
    page1_record = json.loads((FIXTURES / "apple_search_page.json").read_text(encoding="utf-8"))[
        "searchResults"
    ][0]
    html1 = _hydration_page_html({"searchResults": [page1_record], "totalRecords": 2})
    html2 = _hydration_page_html({"searchResults": [], "totalRecords": 2})

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/search") and "page=2" in str(request.url):
            return httpx.Response(200, text=html2, headers={"content-type": "text/html"})
        if request.url.path.endswith("/search"):
            return httpx.Response(200, text=html1, headers={"content-type": "text/html"})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def test_fetch_stops_on_empty_page() -> None:
    transport = _make_transport()
    lines: list[str] = []
    with httpx.Client(transport=transport, base_url="https://jobs.apple.com") as client:
        jobs = fetch_jobs_for_locations(
            client,
            location_ids=["united-states-USA"],
            query="",
            locale="en-us",
            page_delay_sec=0,
            max_pages=None,
            include_raw=False,
            progress=lines.append,
        )
    assert len(jobs) == 1
    assert jobs[0].external_id == "PIPE-114438206"
    assert any("starting fetch" in line for line in lines)
    assert any("page 1" in line for line in lines)


def test_postlocation_id_maps_to_slug() -> None:
    transport = _make_transport()
    with httpx.Client(transport=transport, base_url="https://jobs.apple.com") as client:
        jobs = fetch_jobs_for_locations(
            client,
            location_ids=["postLocation-USA"],
            query="",
            locale="en-us",
            page_delay_sec=0,
            max_pages=1,
            include_raw=False,
        )
    assert len(jobs) == 1
