import logging
import os
import time
from datetime import datetime

import pytz
import requests

from src.sanitize import (
    DOWNLOAD_DIR_MAIN, count_tweets, create_clean_timestamps_csv, get_average_tweets_per_day,
    get_first_tweet_timestamp, process_by_15min, process_by_date, process_by_hour, process_by_week, process_by_weekday,
    sanitize_csv_to_file, save_tweets_to_csv,
)

logger = logging.getLogger(__name__)

RAW_PATH = os.path.join(DOWNLOAD_DIR_MAIN, 'raw_elonmusk.csv')
PRE_PREFIX = os.path.join(DOWNLOAD_DIR_MAIN, 'pre_elonmusk')
PRE_PATH = f"{PRE_PREFIX}.csv"
CLEAN_PREFIX = os.path.join(DOWNLOAD_DIR_MAIN, 'clean_elonmusk')
CLEAN_PATH = f"{CLEAN_PREFIX}.csv"
CC_PREFIX = os.path.join(DOWNLOAD_DIR_MAIN, 'cc_elonmusk')
CC_PATH = f"{CC_PREFIX}.csv"
UTC_PREFIX = os.path.join(DOWNLOAD_DIR_MAIN, 'utc_elonmusk')
UTC_PATH = f"{UTC_PREFIX}.csv"

ENCODING = 'utf-8'


def _check_modify_date(path: str, modify_date: float = 300) -> bool:
    return (
        os.path.exists(path)
        and time.time() - os.path.getmtime(path) < modify_date
    )


def _download_all(force: bool = False) -> tuple[bytes, bytes, bytes]:
    """
    Download the full Elon Musk tweet CSV if local files are fresh; otherwise fetch from API.
    Sanitizes, processes, and saves aggregated results to disk.
    Set force=True to bypass cache freshness checks.

    Returns:
        tuple of (clean_csv_bytes, utc_csv_bytes, cc_csv_bytes)
    """
    # Check cache freshness (5 minutes) unless force refresh requested
    if not force and all(_check_modify_date(p) for p in (RAW_PATH, PRE_PATH, CLEAN_PATH, UTC_PATH, CC_PATH)):
        logger.info('Using cached files')
        with open(CLEAN_PATH, 'rb') as f:
            clean_bytes = f.read()
        with open(UTC_PATH, 'rb') as f:
            utc_bytes = f.read()
        with open(CC_PATH, 'rb') as f:
            cc_bytes = f.read()
        return clean_bytes, utc_bytes, cc_bytes
    else:
        logger.info('Downloading fresh data from XTracker API')
        resp = requests.post(
            'https://www.xtracker.io/api/download',
            json={'handle': 'elonmusk', 'platform': 'X'},
            headers={'Content-Type': 'application/json', 'media-type': 'text/event-stream'},
            timeout=30,
        )
        resp.raise_for_status()
        logger.info('Download status code: %s', resp.status_code)
        save_tweets_to_csv(resp.content, RAW_PATH)
        pre_bytes = sanitize_csv_to_file(resp.content, PRE_PREFIX)
        clean_bytes, utc_bytes, cc_bytes = create_clean_timestamps_csv(
            pre_bytes,
            CLEAN_PREFIX,
            UTC_PREFIX,
            CC_PREFIX,
        )
        return clean_bytes, utc_bytes, cc_bytes


def _download(force: bool = False) -> bytes:
    """
    Download the full Elon Musk tweet CSV if local files are fresh; otherwise fetch from API.
    Sanitizes, processes, and saves aggregated results to disk.

    Returns the processed clean CSV content as bytes.
    """
    clean_bytes, _, _ = _download_all(force)
    return clean_bytes


def get_tweets_by_hour(force: bool = False) -> str:
    return process_by_hour(_download(force)).decode(ENCODING)


def get_tweets_by_date(force: bool = False) -> str:
    return process_by_date(_download(force)).decode(ENCODING)


def get_tweets_by_weekday(force: bool = False) -> str:
    return process_by_weekday(_download(force)).decode(ENCODING)


def _anchor_from_param(anchor: int) -> int:
    if anchor not in range(7):
        raise ValueError("anchor must be in range 0..6 (0=Mon .. 6=Sun).")
    return anchor


def get_tweets_by_week(anchor: int = 4, use_utc: bool = False, force: bool = False) -> str:
    anchor = _anchor_from_param(anchor)
    return process_by_week(_download(force), anchor_weekday=anchor, use_utc=use_utc).decode(ENCODING)


def get_tweets_by_15min(force: bool = False) -> str:
    return process_by_15min(_download(force)).decode(ENCODING)


def get_total_tweets(force: bool = False) -> int:
    return count_tweets(_download(force))


def get_avg_per_day(force: bool = False) -> float:
    return get_average_tweets_per_day(_download(force))


def get_first_tweet_date(force: bool = False) -> str:
    dt = get_first_tweet_timestamp(_download(force)).astimezone(pytz.timezone('America/New_York'))
    return dt.isoformat()


def get_time_now() -> str:
    # Current time in Eastern Time (ET)
    return datetime.now(pytz.timezone('America/New_York')).isoformat()


def get_data_range(force: bool = False) -> int:
    first_tweet = get_first_tweet_timestamp(_download(force)).astimezone(pytz.timezone('America/New_York'))
    now_et = datetime.now(pytz.timezone('America/New_York'))
    return int((now_et - first_tweet).total_seconds())


def get_utc_csv(force: bool = False) -> str:
    """Return the utc_elonmusk.csv file as bytes."""
    _, utc_bytes, _ = _download_all(force)
    return utc_bytes.decode(ENCODING)


def get_cc_csv(force: bool = False) -> str:
    """Return the cc_elonmusk.csv file as bytes (recent 6 months)."""
    _, _, cc_bytes = _download_all(force)
    return cc_bytes.decode(ENCODING)
