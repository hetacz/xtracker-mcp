"""Pytest and standalone checks for the local Polymarket database integration."""

from __future__ import annotations

import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Callable

# Preserve direct ``python tests_db/test_polymarket_integration.py`` usage.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_imports() -> None:
    """All integration modules import successfully."""
    from src import db, download_polymarket

    assert callable(db.load_database)
    assert callable(download_polymarket.fetch_tweets_from_api)


def test_database_load() -> None:
    """The checked-in database loads with internally consistent statistics."""
    from src.db import get_database_stats, load_database

    database = load_database()
    stats = get_database_stats()

    assert not database.empty
    assert {"id", "text"}.issubset(database.columns)
    assert stats["total_tweets"] == len(database)
    assert stats["oldest_date"] is not None
    assert stats["newest_date"] is not None


def test_database_functions() -> None:
    """The most-recent ID and timestamp are valid and mutually consistent."""
    from src.db import get_most_recent_timestamp, get_most_recent_tweet_id

    recent_id = get_most_recent_tweet_id()
    recent_timestamp = get_most_recent_timestamp()

    assert recent_id is not None and recent_id.isdigit()
    assert isinstance(recent_timestamp, datetime)
    assert recent_timestamp.tzinfo is not None


def test_csv_conversion() -> None:
    """Database export produces the documented three-column CSV."""
    from src.db import database_to_csv_with_timestamps

    csv_bytes = database_to_csv_with_timestamps()
    lines = csv_bytes.decode("utf-8").splitlines()

    assert lines[0] == "id,text,created_at"
    assert len(lines) > 1


def test_api_functions_exist() -> None:
    """All public Polymarket aggregation functions remain callable."""
    from src.download_polymarket import (
        get_avg_per_day_pm,
        get_total_tweets_pm,
        get_tweets_by_15min_pm,
        get_tweets_by_date_pm,
        get_tweets_by_hour_pm,
        get_tweets_by_week_pm,
        get_tweets_by_weekday_pm,
    )

    functions = (
        get_tweets_by_hour_pm,
        get_tweets_by_date_pm,
        get_tweets_by_weekday_pm,
        get_tweets_by_week_pm,
        get_tweets_by_15min_pm,
        get_total_tweets_pm,
        get_avg_per_day_pm,
    )
    assert all(callable(function) for function in functions)


def test_main_integration() -> None:
    """The HTTP app and MCP server are both wired by the entry point."""
    import main

    assert hasattr(main, "app")
    assert hasattr(main, "mcp")


def main_test() -> int:
    """Run the same checks without requiring pytest's test runner."""
    checks: tuple[tuple[str, Callable[[], None]], ...] = (
        ("Imports", test_imports),
        ("Database Load", test_database_load),
        ("Database Functions", test_database_functions),
        ("CSV Conversion", test_csv_conversion),
        ("API Functions", test_api_functions_exist),
        ("Main Integration", test_main_integration),
    )
    failures = 0

    for name, check in checks:
        try:
            check()
            print(f"{name:.<40} PASS")
        except Exception:
            failures += 1
            print(f"{name:.<40} FAIL")
            traceback.print_exc()

    print(f"TOTAL: {len(checks) - failures}/{len(checks)} tests passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main_test())
