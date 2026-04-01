from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Job:
    """Normalized job record for export and downstream parsing."""

    source: str
    external_id: str
    title: str
    company: str
    url: str
    posted_at: Optional[str]
    summary: Optional[str]
    team: Optional[str]
    locations: List[str]
    raw: Optional[Dict[str, Any]] = None

    def to_json_dict(self, *, include_raw: bool = True) -> Dict[str, Any]:
        d = asdict(self)
        if not include_raw:
            d.pop("raw", None)
        return d
