"""Tests for Google Careers HTML listing client (no live network)."""

from __future__ import annotations

from pathlib import Path

import httpx

from rolefetch.sources.google import (
    GoogleCareersError,
    fetch_jobs,
    normalize_google_row,
    parse_results_page,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_results_page_fixture() -> None:
    html = (FIXTURES / "google_results_sample.html").read_text(encoding="utf-8")
    rows = parse_results_page(html)
    assert [(e, t) for e, t, _ in rows] == [
        ("111111", "Software Engineer"),
        ("222222", "Program Manager"),
    ]


def test_normalize_google_row_url_and_locations() -> None:
    job = normalize_google_row(
        "111111",
        "Software Engineer",
        "jobs/results/111111-example?location=United+States",
        include_raw=True,
        page_num=3,
    )
    assert job.source == "google"
    assert job.company == "Google"
    assert job.external_id == "111111"
    assert job.title == "Software Engineer"
    assert job.url == (
        "https://www.google.com/about/careers/applications/jobs/results/"
        "111111-example?location=United+States"
    )
    assert job.locations == ["United States"]
    assert job.raw is not None
    assert job.raw.get("results_page") == 3


def test_fetch_stops_when_page_has_no_listings() -> None:
    pages = {
        1: '<a href="jobs/results/1-a?location=X" aria-label="Learn more about A"></a>',
        2: "<html><body></body></html>",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        p = int(request.url.params.get("page", "0"))
        body = pages.get(p, "")
        return httpx.Response(200, text=body)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        jobs = fetch_jobs(
            client,
            location="",
            query="",
            page_delay_sec=0,
            include_raw=False,
        )

    assert len(jobs) == 1
    assert jobs[0].external_id == "1"
    assert jobs[0].title == "A"


def test_fetch_stops_when_no_new_ids() -> None:
    same = (
        '<a href="jobs/results/1-a" aria-label="Learn more about One"></a>'
        '<a href="jobs/results/2-b" aria-label="Learn more about Two"></a>'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=same)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        jobs = fetch_jobs(
            client,
            page_delay_sec=0,
            max_pages=10,
            include_raw=False,
        )

    assert len(jobs) == 2


def test_http_error_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="no")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        try:
            fetch_jobs(client, page_delay_sec=0, include_raw=False, max_pages=1)
        except GoogleCareersError as e:
            assert "500" in str(e)
        else:
            raise AssertionError("expected GoogleCareersError")
