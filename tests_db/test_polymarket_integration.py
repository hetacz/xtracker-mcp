"""Quick test script to verify Polymarket integration works."""
import sys
import traceback


def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    try:
        from src import db
        from src import download_polymarket
        print("✓ All modules imported successfully")
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        traceback.print_exc()
        return False


def test_database_load():
    """Test loading the database."""
    print("\nTesting database load...")
    try:
        from src.db import load_database, get_database_stats

        df = load_database()
        print(f"✓ Database loaded: {len(df)} tweets")

        stats = get_database_stats()
        print(f"  Total tweets: {stats['total_tweets']:,}")
        print(f"  Oldest:       {stats['oldest_date']}")
        print(f"  Newest:       {stats['newest_date']}")
        return True
    except Exception as e:
        print(f"✗ Database load failed: {e}")
        traceback.print_exc()
        return False


def test_database_functions():
    """Test database utility functions."""
    print("\nTesting database functions...")
    try:
        from src.db import get_most_recent_tweet_id, get_most_recent_timestamp

        recent_id = get_most_recent_tweet_id()
        print(f"✓ Most recent tweet ID: {recent_id}")

        recent_ts = get_most_recent_timestamp()
        print(f"✓ Most recent timestamp: {recent_ts}")

        return True
    except Exception as e:
        print(f"✗ Database functions failed: {e}")
        traceback.print_exc()
        return False


def test_csv_conversion():
    """Test converting database to CSV with timestamps."""
    print("\nTesting CSV conversion...")
    try:
        from src.db import database_to_csv_with_timestamps

        csv_bytes = database_to_csv_with_timestamps()
        lines = csv_bytes.decode('utf-8').split('\n')

        print(f"✓ CSV conversion successful: {len(lines) - 1} rows")
        print(f"  Header: {lines[0]}")
        if len(lines) > 1 and lines[1]:
            print(f"  First row: {lines[1][:80]}...")

        return True
    except Exception as e:
        print(f"✗ CSV conversion failed: {e}")
        traceback.print_exc()
        return False


def test_api_functions_exist():
    """Test that API functions are defined."""
    print("\nTesting API function definitions...")
    try:
        from src.download_polymarket import (
            get_tweets_by_hour_pm,
            get_tweets_by_date_pm,
            get_tweets_by_weekday_pm,
            get_tweets_by_week_pm,
            get_tweets_by_15min_pm,
            get_total_tweets_pm,
            get_avg_per_day_pm,
        )

        print("✓ All API functions defined")
        return True
    except Exception as e:
        print(f"✗ API function check failed: {e}")
        traceback.print_exc()
        return False


def test_main_integration():
    """Test that main.py integrates correctly."""
    print("\nTesting main.py integration...")
    try:
        import main

        # Check that app exists
        if hasattr(main, 'app'):
            print("✓ FastAPI app created")

        # Check that MCP server exists
        if hasattr(main, 'mcp'):
            print("✓ MCP server created")

        return True
    except Exception as e:
        print(f"✗ Main integration failed: {e}")
        traceback.print_exc()
        return False


def main_test():
    """Run all tests."""
    print("=" * 60)
    print("POLYMARKET INTEGRATION TEST SUITE")
    print("=" * 60)

    results = [
        ("Imports", test_imports()), ("Database Load", test_database_load()),
        ("Database Functions", test_database_functions()), ("CSV Conversion", test_csv_conversion()),
        ("API Functions", test_api_functions_exist()), ("Main Integration", test_main_integration())
    ]

    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)

    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name:.<40} {status}")

    total = len(results)
    passed = sum(1 for _, p in results if p)

    print("=" * 60)
    print(f"TOTAL: {passed}/{total} tests passed")
    print("=" * 60)

    return 0 if passed == total else 1


if __name__ == '__main__':
    sys.exit(main_test())
