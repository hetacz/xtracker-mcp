"""Download and process tweets from the Polymarket XTracker API endpoint."""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import pytz
import requests

from src.db import (
    append_tweets,
    database_to_csv_with_timestamps,
    get_most_recent_timestamp,
)
from src.sanitize import (
    DOWNLOAD_DIR,
    count_tweets,
    create_clean_timestamps_csv,
    get_average_tweets_per_day,
    get_first_tweet_timestamp,
    process_by_15min,
    process_by_date,
    process_by_hour,
    process_by_week,
    process_by_weekday,
    process_last_tue_fri_counts,
    sanitize_csv_to_file,
    save_tweets_to_csv,
)

logger = logging.getLogger(__name__)

# Polymarket API endpoint
POLYMARKET_API_URL = "https://xtracker.polymarket.com/api/users/elonmusk/posts"

# Output directories
DOWNLOAD_DIR_PM = os.path.join(DOWNLOAD_DIR, "polymarket_main")
DOWNLOAD_DIR_PM_RAW = os.path.join(DOWNLOAD_DIR, "polymarket_raw")

os.makedirs(DOWNLOAD_DIR_PM, exist_ok=True)
os.makedirs(DOWNLOAD_DIR_PM_RAW, exist_ok=True)

# Output paths
RAW_PM_PATH = os.path.join(DOWNLOAD_DIR_PM, 'raw_elonmusk_pm.csv')
PRE_PM_PATH = os.path.join(DOWNLOAD_DIR_PM, 'pre_elonmusk_pm.csv')
CLEAN_PM_PATH = os.path.join(DOWNLOAD_DIR_PM, 'clean_elonmusk_pm.csv')
CC_PM_PATH = os.path.join(DOWNLOAD_DIR_PM, 'cc_elonmusk_pm.csv')
UTC_PM_PATH = os.path.join(DOWNLOAD_DIR_PM, 'utc_elonmusk_pm.csv')

ENCODING = 'utf-8'


def _check_modify_date(path: str, modify_date: float = 300) -> bool:
    """Check if file exists and was modified within the specified time window."""
    return (
        os.path.exists(path)
        and time.time() - os.path.getmtime(path) < modify_date
    )


def _save_raw_json_response(response_data: dict, filename_prefix: str = "fetch") -> None:
    """Save raw JSON response to timestamped file for debugging."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save pretty JSON
    json_path = os.path.join(DOWNLOAD_DIR_PM_RAW, f"{filename_prefix}_{timestamp}.json")
    try:
        with open(json_path, 'w', encoding=ENCODING) as f:
            json.dump(response_data, f, indent=2)
        logger.info(f"Saved raw JSON response to {json_path}")
    except Exception as e:
        logger.warning(f"Failed to save raw JSON response: {e}")


def _sanitize_text(text: str) -> str:
    """Remove newlines and carriage returns from text."""
    return text.replace('\n', ' ').replace('\r', ' ').strip()


def fetch_tweets_from_api(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> list[dict[str, str]]:
    """Fetch tweets from Polymarket API.

    Args:
        start_date: Optional ISO datetime string (e.g., "2025-11-25T17:00:00.000Z")
        end_date: Optional ISO datetime string (e.g., "2025-12-02T17:00:59.000Z")

    Returns:
        List of dicts with 'id' and 'text' keys
    """
    params = {}
    if start_date:
        params['startDate'] = start_date
    if end_date:
        params['endDate'] = end_date

    try:
        logger.info(f"Fetching from Polymarket API: {POLYMARKET_API_URL}")
        if params:
            logger.info(f"Query parameters: {params}")

        response = requests.get(POLYMARKET_API_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Save raw response for debugging
        _save_raw_json_response(data, "fetch")

        if not data.get('success', False):
            logger.error(f"API returned success=false: {data}")
            return []

        posts = data.get('data', [])
        logger.info(f"Received {len(posts)} posts from API")

        # Extract id (platformId) and text (content)
        tweets = []
        for post in posts:
            platform_id = post.get('platformId')
            content = post.get('content')

            if platform_id and content:
                tweets.append(
                    {
                        'id': str(platform_id),
                        'text': _sanitize_text(content)
                    },
                )

        logger.info(f"Extracted {len(tweets)} valid tweets")
        return tweets

    except Exception as e:
        logger.error(f"Error fetching from Polymarket API: {e}")
        return []


def fetch_and_update_database(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    auto_detect_start: bool = True,
) -> tuple[int, int]:
    """Fetch new tweets from API and update the database.

    Args:
        start_date: Optional start date (ISO format)
        end_date: Optional end date (ISO format)
        auto_detect_start: If True and no start_date provided, auto-detect from database

    Returns:
        Tuple of (total_tweets_in_db, new_tweets_added)
    """
    # Auto-detect start date from most recent tweet in database
    if auto_detect_start and start_date is None:
        most_recent = get_most_recent_timestamp()
        if most_recent:
            # Fetch from 1 hour before the last tweet (buffer for reliability)
            buffered_start = most_recent - timedelta(hours=1)
            start_date = buffered_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            logger.info(f"Auto-detected start date from database: {start_date}")
        else:
            logger.info("Database empty, fetching without start date (last 1 month)")

    # Fetch from API
    tweets = fetch_tweets_from_api(start_date, end_date)

    # Append to database with deduplication
    total, added = append_tweets(tweets)

    return total, added


def _download_all_pm() -> tuple[bytes, bytes, bytes]:
    """Download and process Polymarket tweets with 5-minute caching.

    Returns:
        tuple of (clean_csv_bytes, utc_csv_bytes, cc_csv_bytes)
    """
    # Check cache freshness (5 minutes)
    if all(_check_modify_date(p) for p in (RAW_PM_PATH, PRE_PM_PATH, CLEAN_PM_PATH, UTC_PM_PATH, CC_PM_PATH)):
        logger.info('Using cached Polymarket files')
        with open(CLEAN_PM_PATH, 'rb') as f:
            clean_bytes = f.read()
        with open(UTC_PM_PATH, 'rb') as f:
            utc_bytes = f.read()
        with open(CC_PM_PATH, 'rb') as f:
            cc_bytes = f.read()
        return (clean_bytes, utc_bytes, cc_bytes)
    else:
        logger.info('Fetching fresh Polymarket data')

        # Fetch and update database
        total, added = fetch_and_update_database(auto_detect_start=True)
        logger.info(f"Database updated: {total} total tweets, {added} new tweets added")

        # Convert database to 3-column CSV format
        raw_csv_bytes = database_to_csv_with_timestamps()
        save_tweets_to_csv(raw_csv_bytes, RAW_PM_PATH)

        # Sanitize
        pre_bytes = sanitize_csv_to_file(raw_csv_bytes, PRE_PM_PATH)

        # Create clean timestamps
        clean_bytes, utc_bytes, cc_bytes = create_clean_timestamps_csv(
            pre_bytes, CLEAN_PM_PATH, UTC_PM_PATH, CC_PM_PATH,
        )

        return (clean_bytes, utc_bytes, cc_bytes)


def _download_pm() -> bytes:
    """Download and process Polymarket tweets, return clean CSV bytes."""
    clean_bytes, _, _ = _download_all_pm()
    return clean_bytes


# Mirror all the endpoint functions from download.py

def get_tweets_by_hour_pm() -> str:
    """Return normalized tweet counts grouped by hour (ET) as CSV text."""
    return process_by_hour(_download_pm()).decode(ENCODING)


def get_tweets_by_date_pm() -> str:
    """Return tweet counts grouped by date (ET) as CSV text."""
    return process_by_date(_download_pm()).decode(ENCODING)


def get_tweets_by_weekday_pm() -> str:
    """Return tweet counts grouped by weekday (ET) as CSV text."""
    return process_by_weekday(_download_pm()).decode(ENCODING)


def get_tweets_by_week_pm() -> str:
    """Return tweet counts grouped by week (starts on Friday 12:00 ET) as CSV text."""
    return process_by_week(_download_pm()).decode(ENCODING)


def get_latest_counts_pm() -> str:
    """Return total tweet counts since last Tuesday and Friday at noon ET as CSV text."""
    return process_last_tue_fri_counts(_download_pm()).decode(ENCODING)


def get_tweets_by_15min_pm() -> str:
    """Return tweet counts grouped into 15-minute buckets (ET) as CSV text."""
    return process_by_15min(_download_pm()).decode(ENCODING)


def get_total_tweets_pm() -> int:
    """Return the total number of tweets from Polymarket data."""
    return count_tweets(_download_pm())


def get_avg_per_day_pm() -> float:
    """Return the average tweets per day from Polymarket data."""
    return get_average_tweets_per_day(_download_pm())


def get_first_tweet_date_pm() -> str:
    """Return the ISO timestamp of the first tweet (ET) from Polymarket data."""
    dt = get_first_tweet_timestamp(_download_pm()).astimezone(pytz.timezone('America/New_York'))
    return dt.isoformat()


def get_time_now_pm() -> str:
    """Return the current ET ISO timestamp."""
    return datetime.now(pytz.timezone('America/New_York')).isoformat()


def get_data_range_pm() -> int:
    """Return the elapsed seconds between the first tweet and now (ET)."""
    first_tweet = get_first_tweet_timestamp(_download_pm()).astimezone(pytz.timezone('America/New_York'))
    now_et = datetime.now(pytz.timezone('America/New_York'))
    return int((now_et - first_tweet).total_seconds())


def get_utc_csv_pm() -> str:
    """Return the utc_elonmusk_pm.csv file as bytes."""
    _, utc_bytes, _ = _download_all_pm()
    return utc_bytes.decode(ENCODING)


def get_cc_csv_pm() -> str:
    """Return the cc_elonmusk_pm.csv file as bytes (recent 6 months)."""
    _, _, cc_bytes = _download_all_pm()
    return cc_bytes.decode(ENCODING)
