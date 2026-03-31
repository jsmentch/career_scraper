"""Tests for Amazon jobs search.json client (no live network)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from career_scraper.sources.amazon import AmazonAPIError, fetch_jobs, normalize_amazon_job

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_normalize_amazon_job() -> None:
    record = json.loads((FIXTURES / "amazon_search_page.json").read_text(encoding="utf-8"))[
        "jobs"
    ][0]
    job = normalize_amazon_job(record, include_raw=True)
    assert job.source == "amazon"
    assert job.title == "Software Engineer"
    assert job.company == "Amazon.com Services LLC"
    assert job.url == "https://www.amazon.jobs/en/jobs/100/software-engineer"
    assert job.external_id == "11111111-1111-1111-1111-111111111111"
    assert "Seattle" in job.locations[0]
    assert job.team == "Software Development"
    assert job.raw is not None


def test_fetch_paginates_until_offset_reaches_hits() -> None:
    """First response: 1 job, hits=2. Second: 1 job. Third: empty -> stop."""
    p1 = {
        "error": None,
        "hits": 2,
        "jobs": [
            {
                "id": "a",
                "title": "T1",
                "company_name": "Amazon",
                "job_path": "/en/jobs/1/t1",
                "location": "X",
            }
        ],
    }
    p2 = {
        "error": None,
        "hits": 2,
        "jobs": [
            {
                "id": "b",
                "title": "T2",
                "company_name": "Amazon",
                "job_path": "/en/jobs/2/t2",
                "location": "Y",
            }
        ],
    }
    p3 = {"error": None, "hits": 2, "jobs": []}

    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        q = request.url.params.get("offset", "0")
        off = int(q)
        calls.append(off)
        if off == 0:
            return httpx.Response(200, json=p1)
        if off == 1:
            return httpx.Response(200, json=p2)
        return httpx.Response(200, json=p3)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="https://www.amazon.jobs") as client:
        jobs = fetch_jobs(
            client,
            base_query="",
            loc_query="",
            locale_prefix="en",
            result_limit=1,
            page_delay_sec=0,
            include_raw=False,
        )

    assert len(jobs) == 2
    assert {j.external_id for j in jobs} == {"a", "b"}
    assert calls == [0, 1]


def test_error_payload_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"error": "bad", "jobs": []},
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, base_url="https://www.amazon.jobs") as client:
        try:
            fetch_jobs(
                client,
                result_limit=10,
                page_delay_sec=0,
                include_raw=False,
            )
        except AmazonAPIError as e:
            assert "bad" in str(e)
        else:
            raise AssertionError("expected AmazonAPIError")
