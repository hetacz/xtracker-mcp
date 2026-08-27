"""Standalone comprehensive date checks for the XTracker Polymarket API."""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Mapping

import requests

API_URL = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "downloads" / "polymarket_tests"
FIELD_NAMES = ("id", "userId", "platformId", "content", "createdAt", "importedAt", "metrics")
CASES = (
    ("no_params", {}),
    ("start_2024_08_08", {"startDate": "2024-08-08T00:00:00.000Z"}),
    ("start_2025_03_03", {"startDate": "2025-03-03T00:00:00.000Z"}),
    ("start_2025_11_12", {"startDate": "2025-11-12T00:00:00.000Z"}),
    ("end_2025_11_19", {"endDate": "2025-11-19T23:59:59.999Z"}),
    (
        "range_2024_08_08_2025_11_19",
        {
            "startDate": "2024-08-08T00:00:00.000Z",
            "endDate": "2025-11-19T23:59:59.999Z",
        },
    ),
    (
        "range_2025_03_03_2025_11_19",
        {
            "startDate": "2025-03-03T00:00:00.000Z",
            "endDate": "2025-11-19T23:59:59.999Z",
        },
    ),
    (
        "range_2025_11_12_2025_11_19",
        {
            "startDate": "2025-11-12T00:00:00.000Z",
            "endDate": "2025-11-19T23:59:59.999Z",
        },
    ),
)


def _write_artifacts(
    response: requests.Response,
    payload: dict[str, object],
    case_name: str,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    prefix = OUTPUT_DIR / f"{case_name}_{timestamp}"

    prefix.with_name(f"{prefix.name}_raw.txt").write_bytes(response.content)
    prefix.with_name(f"{prefix.name}_meta.json").write_text(
        json.dumps(
            {
                "test_name": case_name,
                "timestamp": timestamp,
                "url": response.url,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "content_length": len(response.content),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    prefix.with_name(f"{prefix.name}_pretty.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    posts = payload["data"]
    assert isinstance(posts, list)
    if posts:
        with prefix.with_suffix(".csv").open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=FIELD_NAMES, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(posts)


def run_case(case_name: str, params: Mapping[str, str]) -> requests.Response:
    """Run one live case, validate its contract, and archive its response."""
    response = requests.get(API_URL, params=dict(params) or None, timeout=60)
    response.raise_for_status()
    assert "json" in response.headers.get("Content-Type", "").lower()

    payload = response.json()
    assert isinstance(payload, dict)
    assert payload.get("success") is True
    assert isinstance(payload.get("data"), list)
    _write_artifacts(response, payload, case_name)
    return response


def main() -> int:
    """Run all date combinations and return nonzero if any check fails."""
    failures = 0
    for case_name, params in CASES:
        try:
            response = run_case(case_name, params)
            print(f"PASS {case_name}: {response.status_code}, {len(response.content)} bytes")
        except Exception as exc:  # Continue so the manual report includes every case.
            failures += 1
            print(f"FAIL {case_name}: {exc}")

    print(f"{len(CASES) - failures}/{len(CASES)} comprehensive checks passed")
    print(f"Artifacts: {OUTPUT_DIR}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
