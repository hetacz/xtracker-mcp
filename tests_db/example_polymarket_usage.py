"""Example script demonstrating the Polymarket API integration.

This script shows how to use the new Polymarket functions to:
1. Check database statistics
2. Fetch new tweets from the API
3. Query aggregated data
"""
import logging

# Configure logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("=" * 70)
print("POLYMARKET API INTEGRATION - EXAMPLE USAGE")
print("=" * 70)

# Example 1: Check Database Statistics
print("\n1. DATABASE STATISTICS")
print("-" * 70)

from src.db import get_database_stats, load_database

stats = get_database_stats()
print(f"Total tweets in database: {stats['total_tweets']:,}")
print(f"Oldest tweet:             {stats['oldest_date']}")
print(f"Newest tweet:             {stats['newest_date']}")

# Example 2: Load Database and Show Sample
print("\n2. DATABASE SAMPLE")
print("-" * 70)

df = load_database()
if not df.empty:
    print(f"Database has {len(df)} rows")
    print("\nFirst 3 tweets:")
    for idx, row in df.head(3).iterrows():
        print(f"  ID: {row['id']}")
        print(f"  Text: {row['text'][:80]}...")
        print()
else:
    print("Database is empty")

# Example 3: Fetch New Tweets (with auto-detection)
print("\n3. FETCH NEW TWEETS")
print("-" * 70)

from src.download_polymarket import fetch_and_update_database

print("Fetching new tweets from Polymarket API...")
print("(This will auto-detect the last tweet and fetch only newer ones)")
print()

total, added = fetch_and_update_database(auto_detect_start=True)

print(f"✓ Fetch complete!")
print(f"  Total tweets now: {total:,}")
print(f"  New tweets added: {added:,}")

if added == 0:
    print("\n  ℹ No new tweets (all fetched tweets already in database)")

# Example 4: Get Aggregated Data
print("\n4. AGGREGATED DATA SAMPLES")
print("-" * 70)

from src.download_polymarket import (
    get_total_tweets_pm,
    get_avg_per_day_pm,
    get_first_tweet_date_pm
)

print(f"Total tweets (from processed data): {get_total_tweets_pm():,}")
print(f"Average tweets per day:              {get_avg_per_day_pm():.2f}")
print(f"First tweet date:                    {get_first_tweet_date_pm()}")

# Example 5: Get CSV Data
print("\n5. CSV OUTPUTS")
print("-" * 70)

from src.download_polymarket import get_tweets_by_hour_pm, get_tweets_by_date_pm

# Get tweets by hour
hour_csv = get_tweets_by_hour_pm()
hour_lines = hour_csv.strip().split('\n')
print(f"Tweets by hour CSV: {len(hour_lines)} lines")
print(f"  Header: {hour_lines[0]}")
if len(hour_lines) > 1:
    print(f"  Sample: {hour_lines[1]}")

print()

# Get tweets by date
date_csv = get_tweets_by_date_pm()
date_lines = date_csv.strip().split('\n')
print(f"Tweets by date CSV: {len(date_lines)} lines")
print(f"  Header: {date_lines[0]}")
if len(date_lines) > 1:
    print(f"  First:  {date_lines[1]}")
    if len(date_lines) > 2:
        print(f"  Last:   {date_lines[-1]}")

# Example 6: Fetch Specific Date Range
print("\n6. FETCH SPECIFIC DATE RANGE")
print("-" * 70)

# This example shows how to fetch a specific date range
# Uncomment to try it:

# from src.download_polymarket import fetch_tweets_from_api
#
# tweets = fetch_tweets_from_api(
#     start_date="2025-11-25T00:00:00.000Z",
#     end_date="2025-11-28T23:59:59.000Z"
# )
#
# print(f"Fetched {len(tweets)} tweets for date range")
# if tweets:
#     print(f"First tweet ID: {tweets[0]['id']}")
#     print(f"First tweet text: {tweets[0]['text'][:80]}...")

print("(Example code commented out - uncomment to try)")

print("\n" + "=" * 70)
print("EXAMPLES COMPLETE")
print("=" * 70)
print("\nFor more information, see:")
print("  - POLYMARKET_QUICKSTART.md")
print("  - POLYMARKET_IMPLEMENTATION.md")
print("\nTo update database manually:")
print("  python update_polymarket_db.py")
print("\nTo run tests:")
print("  python test_polymarket_integration.py")
print()
