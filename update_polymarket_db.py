"""Utility script to manually update the Polymarket tweet database.

Usage:
    python update_polymarket_db.py                          # Auto-detect and fetch new tweets
    python update_polymarket_db.py --start 2025-11-25       # Fetch from specific date
    python update_polymarket_db.py --start 2025-11-20 --end 2025-11-28  # Fetch date range
    python update_polymarket_db.py --stats                  # Show database statistics
"""
import argparse
import logging
import sys
from datetime import datetime

from src.db import get_database_stats
from src.download_polymarket import fetch_and_update_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_iso_date(date_str: str) -> str:
    """Convert date string to ISO format with time."""
    try:
        # Try parsing various formats
        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except ValueError:
                continue

        # If none matched, return as-is
        return date_str
    except Exception as e:
        logger.error(f"Error formatting date '{date_str}': {e}")
        return date_str


def show_stats():
    """Display database statistics."""
    stats = get_database_stats()

    print("\n" + "=" * 60)
    print("POLYMARKET TWEET DATABASE STATISTICS")
    print("=" * 60)
    print(f"Total tweets:    {stats['total_tweets']:,}")
    print(f"Oldest tweet:    {stats['oldest_date'] or 'N/A'}")
    print(f"Newest tweet:    {stats['newest_date'] or 'N/A'}")
    print("=" * 60 + "\n")


def update_database(start_date=None, end_date=None, auto_detect=True):
    """Update the database with new tweets."""
    print("\n" + "=" * 60)
    print("UPDATING POLYMARKET TWEET DATABASE")
    print("=" * 60)

    if start_date:
        start_date = format_iso_date(start_date)
        print(f"Start date:      {start_date}")

    if end_date:
        end_date = format_iso_date(end_date)
        print(f"End date:        {end_date}")

    if auto_detect and not start_date:
        print("Mode:            Auto-detect (fetch from last tweet)")

    print("\nFetching tweets from Polymarket API...")
    print("-" * 60)

    try:
        total, added = fetch_and_update_database(
            start_date=start_date,
            end_date=end_date,
            auto_detect_start=auto_detect
        )

        print("-" * 60)
        print(f"✓ Update complete!")
        print(f"  Total tweets in database: {total:,}")
        print(f"  New tweets added:         {added:,}")

        if added == 0:
            print("\n  ℹ No new tweets found (all fetched tweets already in database)")

        print("=" * 60 + "\n")

        return True

    except Exception as e:
        print("-" * 60)
        print(f"✗ Error updating database: {e}")
        print("=" * 60 + "\n")
        logger.exception("Database update failed")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Update the Polymarket tweet database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Auto-detect and fetch new tweets
  %(prog)s --start 2025-11-25                 # Fetch from specific date
  %(prog)s --start 2025-11-20 --end 2025-11-28  # Fetch date range
  %(prog)s --stats                            # Show database statistics
        """
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD or ISO format)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD or ISO format)'
    )

    parser.add_argument(
        '--no-auto-detect',
        action='store_true',
        help='Disable auto-detection of start date from database'
    )

    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics and exit'
    )

    args = parser.parse_args()

    # Show stats mode
    if args.stats:
        show_stats()
        return 0

    # Update mode
    success = update_database(
        start_date=args.start,
        end_date=args.end,
        auto_detect=not args.no_auto_detect
    )

    # Show stats after update
    show_stats()

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
