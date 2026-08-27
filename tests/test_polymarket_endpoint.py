"""Live contract checks for the public XTracker Polymarket endpoint."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Mapping

import pytest
import requests

API_URL = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
DOWNLOAD_DIR = Path(__file__).resolve().parents[1] / "downloads" / "polymarket_tests"

CASES = [
    ("no_params", {}, 30),
    ("only_start_date", {"startDate": "2025-11-25T17:00:00.000Z"}, 30),
    ("only_end_date", {"endDate": "2025-12-02T17:00:59.000Z"}, 30),
    (
        "start_and_end_date",
        {
            "startDate": "2025-11-25T17:00:00.000Z",
            "endDate": "2025-12-02T17:00:59.000Z",
        },
        30,
    ),
    ("historical_2020", {"startDate": "2020-01-01T00:00:00.000Z"}, 60),
]


def _validate_response(response: requests.Response) -> dict[str, object]:
    """Raise on transport/schema failures and return the decoded payload."""
    response.raise_for_status()
    assert response.content, "XTracker returned an empty response"
    assert "json" in response.headers.get("Content-Type", "").lower()

    payload = response.json()
    assert isinstance(payload, dict)
    assert payload.get("success") is True
    assert isinstance(payload.get("data"), list)
    return payload


def _save_response(
    response: requests.Response,
    payload: dict[str, object],
    test_name: str,
    output_dir: Path,
) -> None:
    """Save raw response, metadata, and formatted JSON for manual inspection."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    prefix = output_dir / f"{test_name}_{timestamp}"

    prefix.with_name(f"{prefix.name}_raw.txt").write_bytes(response.content)
    metadata = {
        "test_name": test_name,
        "timestamp": timestamp,
        "url": response.url,
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "content_length": len(response.content),
        "content_type": response.headers.get("Content-Type", "N/A"),
    }
    prefix.with_name(f"{prefix.name}_meta.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )
    prefix.with_name(f"{prefix.name}_pretty.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def _run_case(
    test_name: str,
    params: Mapping[str, str],
    timeout: int,
    output_dir: Path,
) -> requests.Response:
    response = requests.get(API_URL, params=dict(params) or None, timeout=timeout)
    payload = _validate_response(response)
    _save_response(response, payload, test_name, output_dir)
    return response


@pytest.mark.live
@pytest.mark.parametrize(
    ("test_name", "params", "timeout"),
    CASES,
    ids=[case[0] for case in CASES],
)
def test_polymarket_endpoint(
    test_name: str,
    params: Mapping[str, str],
    timeout: int,
    tmp_path: Path,
) -> None:
    """Verify each supported date-filter shape against the live endpoint."""
    _run_case(test_name, params, timeout, tmp_path)


def main() -> int:
    """Run all live cases and retain their payloads under downloads/."""
    failures = 0
    for test_name, params, timeout in CASES:
        try:
            response = _run_case(test_name, params, timeout, DOWNLOAD_DIR)
            print(f"PASS {test_name}: {response.status_code}, {len(response.content)} bytes")
        except Exception as exc:  # Standalone runner must report every case.
            failures += 1
            print(f"FAIL {test_name}: {exc}")

    print(f"{len(CASES) - failures}/{len(CASES)} live endpoint checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
