"""Database management for Polymarket tweet storage with id and text columns."""
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HISTORIC_DIR = os.path.join(ROOT_DIR, "historic")
DB_PATH = os.path.join(HISTORIC_DIR, "elonmusk_db.csv")
ENCODING = "utf-8"

# Twitter snowflake epoch
TWITTER_EPOCH_MS = 1288834974657

os.makedirs(HISTORIC_DIR, exist_ok=True)


def _snowflake_to_datetime(snowflake_id: int) -> datetime:
    """Convert a Twitter Snowflake ID to a UTC timezone-aware datetime."""
    ts_ms = (int(snowflake_id) >> 22) + TWITTER_EPOCH_MS
    ts_s = ts_ms / 1000.0
    return datetime.fromtimestamp(ts_s, tz=timezone.utc)


def load_database() -> pd.DataFrame:
    """Load the existing database from CSV.

    Returns:
        DataFrame with columns ['id', 'text']. Returns empty DataFrame if file doesn't exist.
    """
    if not os.path.exists(DB_PATH):
        logger.warning(f"Database file not found at {DB_PATH}, returning empty DataFrame")
        return pd.DataFrame(columns=['id', 'text'])

    try:
        df = pd.read_csv(DB_PATH, dtype={'id': str}, encoding=ENCODING)
        logger.info(f"Loaded {len(df)} tweets from database")
        return df
    except Exception as e:
        logger.error(f"Error loading database: {e}")
        return pd.DataFrame(columns=['id', 'text'])


def save_database(df: pd.DataFrame) -> None:
    """Save the database DataFrame to CSV.

    Args:
        df: DataFrame with columns ['id', 'text']
    """
    try:
        df.to_csv(DB_PATH, index=False, encoding=ENCODING)
        logger.info(f"Saved {len(df)} tweets to database")
    except Exception as e:
        logger.error(f"Error saving database: {e}")
        raise


def append_tweets(new_tweets: list[dict[str, str]]) -> tuple[int, int]:
    """Append new tweets to the database with deduplication.

    Args:
        new_tweets: List of dicts with 'id' and 'text' keys

    Returns:
        Tuple of (total_tweets, new_tweets_added)
    """
    if not new_tweets:
        logger.info("No new tweets to append")
        df = load_database()
        return len(df), 0

    # Load existing database
    existing_df = load_database()
    existing_ids = set(existing_df['id'].astype(str)) if not existing_df.empty else set()

    # Filter out duplicates
    unique_new_tweets = []
    for tweet in new_tweets:
        tweet_id = str(tweet['id'])
        if tweet_id not in existing_ids:
            unique_new_tweets.append(
                {
                    'id': tweet_id,
                    'text': tweet['text']
                })
            existing_ids.add(tweet_id)

    if not unique_new_tweets:
        logger.info(f"All {len(new_tweets)} tweets already exist in database")
        return len(existing_df), 0

    # Create DataFrame from new tweets
    new_df = pd.DataFrame(unique_new_tweets)

    # Append to existing
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Save back to disk
    save_database(combined_df)

    logger.info(f"Added {len(unique_new_tweets)} new tweets (out of {len(new_tweets)} fetched)")
    return len(combined_df), len(unique_new_tweets)


def get_most_recent_tweet_id() -> Optional[str]:
    """Get the most recent tweet ID from the database.

    Returns:
        The highest snowflake ID as a string, or None if database is empty
    """
    df = load_database()
    if df.empty:
        return None

    # Convert to numeric, find max
    try:
        ids = pd.to_numeric(df['id'], errors='coerce').dropna()
        if ids.empty:
            return None
        max_id = int(ids.max())
        return str(max_id)
    except Exception as e:
        logger.error(f"Error finding most recent tweet: {e}")
        return None


def get_most_recent_timestamp() -> Optional[datetime]:
    """Get the timestamp of the most recent tweet in the database.

    Returns:
        UTC datetime of the most recent tweet, or None if database is empty
    """
    most_recent_id = get_most_recent_tweet_id()
    if most_recent_id is None:
        return None

    try:
        return _snowflake_to_datetime(int(most_recent_id))
    except Exception as e:
        logger.error(f"Error converting tweet ID to timestamp: {e}")
        return None


def database_to_csv_with_timestamps() -> bytes:
    """Convert the database to 3-column CSV format (id, text, created_at).

    Returns:
        CSV bytes with columns: id, text, created_at
    """
    df = load_database()

    if df.empty:
        # Return empty CSV with headers
        return b'id,text,created_at\n'

    # Convert IDs to timestamps
    df_copy = df.copy()

    # Convert id to numeric for timestamp calculation
    df_copy['id_numeric'] = pd.to_numeric(df_copy['id'], errors='coerce')

    # Drop rows with invalid IDs
    df_copy = df_copy.dropna(subset=['id_numeric'])

    # Generate timestamps from snowflake IDs
    df_copy['created_at'] = df_copy['id_numeric'].apply(
        lambda x: _snowflake_to_datetime(int(x)).isoformat()
    )

    # Select and order columns
    output_df = df_copy[['id', 'text', 'created_at']]

    # Convert to CSV bytes
    csv_str = output_df.to_csv(index=False, encoding=ENCODING)
    return csv_str.encode(ENCODING)


def get_database_stats() -> dict:
    """Get statistics about the database.

    Returns:
        Dict with keys: total_tweets, oldest_date, newest_date
    """
    df = load_database()

    if df.empty:
        return {
            'total_tweets': 0,
            'oldest_date': None,
            'newest_date': None
        }

    try:
        ids = pd.to_numeric(df['id'], errors='coerce').dropna().astype('int64')

        if ids.empty:
            return {
                'total_tweets': len(df),
                'oldest_date': None,
                'newest_date': None
            }

        oldest_id = int(ids.min())
        newest_id = int(ids.max())

        oldest_date = _snowflake_to_datetime(oldest_id)
        newest_date = _snowflake_to_datetime(newest_id)

        return {
            'total_tweets': len(df),
            'oldest_date': oldest_date.isoformat(),
            'newest_date': newest_date.isoformat()
        }
    except Exception as e:
        logger.error(f"Error calculating database stats: {e}")
        return {
            'total_tweets': len(df),
            'oldest_date': None,
            'newest_date': None
        }
