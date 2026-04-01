import json
from pathlib import Path

from rolefetch.sources.apple import normalize_apple_job

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_normalize_apple_job_from_fixture_record() -> None:
    payload = json.loads((FIXTURES / "apple_search_page.json").read_text(encoding="utf-8"))
    record = payload["searchResults"][0]
    job = normalize_apple_job(record, locale="en-us", include_raw=True)

    assert job.source == "apple"
    assert job.company == "Apple"
    assert job.external_id == "PIPE-114438206"
    assert job.title == "FR-Technical Specialist"
    assert "Summary line one" in (job.summary or "")
    assert job.posted_at == "2025-02-18T14:49:20.237887131Z"
    assert job.team == "Apple Retail"
    assert "France" in job.locations
    assert job.url == "https://jobs.apple.com/en-us/details/114438206"
    assert job.raw is not None
    assert job.raw["positionId"] == "114438206"


def test_normalize_without_raw() -> None:
    payload = json.loads((FIXTURES / "apple_search_page.json").read_text(encoding="utf-8"))
    record = payload["searchResults"][0]
    job = normalize_apple_job(record, locale="en-us", include_raw=False)
    assert job.raw is None
