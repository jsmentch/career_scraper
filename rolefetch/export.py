from __future__ import annotations

import csv
import json
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any, Dict, Union

from rolefetch.models import Job

CSV_COLUMNS = [
    "source",
    "external_id",
    "title",
    "company",
    "url",
    "posted_at",
    "summary",
    "team",
    "locations",
    "raw_json",
]


def write_jsonl(jobs: Iterable[Job], path: Union[Path, str], *, include_raw: bool = True) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for job in jobs:
            line = json.dumps(job.to_json_dict(include_raw=include_raw), ensure_ascii=False)
            f.write(line + "\n")


def write_csv(jobs: Iterable[Job], path: Union[Path, str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for job in jobs:
            w.writerow(_job_to_csv_row(job))


def _job_to_csv_row(job: Job) -> Dict[str, Any]:
    loc = " | ".join(job.locations) if job.locations else ""
    raw_json = json.dumps(job.raw, ensure_ascii=False) if job.raw else ""
    return {
        "source": job.source,
        "external_id": job.external_id,
        "title": job.title,
        "company": job.company,
        "url": job.url,
        "posted_at": job.posted_at or "",
        "summary": job.summary or "",
        "team": job.team or "",
        "locations": loc,
        "raw_json": raw_json,
    }


def print_jsonl(jobs: Iterable[Job], stream: IO[str], *, include_raw: bool = True) -> None:
    for job in jobs:
        line = json.dumps(job.to_json_dict(include_raw=include_raw), ensure_ascii=False)
        stream.write(line + "\n")
