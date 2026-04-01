import csv
import json
from pathlib import Path

from rolefetch.export import write_csv, write_jsonl
from rolefetch.models import Job


def test_write_jsonl_roundtrip(tmp_path: Path) -> None:
    jobs = [
        Job(
            source="apple",
            external_id="PIPE-1",
            title="T",
            company="Apple",
            url="https://jobs.apple.com/en-us/details/1",
            posted_at=None,
            summary=None,
            team=None,
            locations=["A", "B"],
            raw={"k": 1},
        )
    ]
    path = tmp_path / "out.jsonl"
    write_jsonl(jobs, path, include_raw=True)
    line = path.read_text(encoding="utf-8").strip()
    d = json.loads(line)
    assert d["external_id"] == "PIPE-1"
    assert d["locations"] == ["A", "B"]
    assert d["raw"]["k"] == 1


def test_write_jsonl_exclude_raw(tmp_path: Path) -> None:
    jobs = [
        Job(
            source="apple",
            external_id="1",
            title="T",
            company="Apple",
            url="u",
            posted_at=None,
            summary=None,
            team=None,
            locations=[],
            raw={"x": 1},
        )
    ]
    path = tmp_path / "out.jsonl"
    write_jsonl(jobs, path, include_raw=False)
    d = json.loads(path.read_text(encoding="utf-8").strip())
    assert "raw" not in d


def test_write_csv_columns(tmp_path: Path) -> None:
    jobs = [
        Job(
            source="apple",
            external_id="PIPE-1",
            title="Title",
            company="Apple",
            url="https://example.com",
            posted_at="2025-01-01T00:00:00Z",
            summary="S",
            team="Team",
            locations=["X", "Y"],
            raw={"a": 1},
        )
    ]
    path = tmp_path / "out.csv"
    write_csv(jobs, path)
    rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    assert len(rows) == 1
    assert rows[0]["title"] == "Title"
    assert rows[0]["locations"] == "X | Y"
    assert json.loads(rows[0]["raw_json"]) == {"a": 1}
