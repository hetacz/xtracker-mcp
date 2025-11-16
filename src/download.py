import logging
import os
import time
from datetime import datetime

import pytz
import requests

from src.sanitize import DOWNLOAD_DIR_MAIN, count_tweets, create_clean_timestamps_csv, get_average_tweets_per_day, \
    get_first_tweet_timestamp, process_by_15min, process_by_date, process_by_hour, process_by_week, process_by_weekday, \
    sanitize_csv_to_file, save_tweets_to_csv

logger = logging.getLogger(__name__)

RAW_PATH = os.path.join(DOWNLOAD_DIR_MAIN, 'raw_elonmusk.csv')
PRE_PATH = os.path.join(DOWNLOAD_DIR_MAIN, 'pre_elonmusk.csv')
CLEAN_PATH = os.path.join(DOWNLOAD_DIR_MAIN, 'clean_elonmusk.csv')
CC_PATH = os.path.join(DOWNLOAD_DIR_MAIN, 'cc_elonmusk.csv')
UTC_PATH = os.path.join(DOWNLOAD_DIR_MAIN, 'utc_elonmusk.csv')

ENCODING = 'utf-8'


def _check_modify_date(path: str, modify_date: float = 300) -> bool:
    return (
            os.path.exists(path)
            and time.time() - os.path.getmtime(path) < modify_date
    )


def _download() -> bytes:
    """
    Download the full Elon Musk tweet CSV if local files are fresh; otherwise fetch from API.
    Sanitizes, processes, and saves aggregated results to disk.

    Returns the processed CSV content as bytes.
    """
    # Check cache freshness (5 minutes)
    if all(_check_modify_date(p) for p in (RAW_PATH, PRE_PATH, CLEAN_PATH, UTC_PATH, CC_PATH)):
        logger.info('Using cached file: %s', CLEAN_PATH)
        with open(CLEAN_PATH, 'rb') as f:
            return f.read()
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
        pre_bytes = sanitize_csv_to_file(resp.content, PRE_PATH)
        clean_bytes = create_clean_timestamps_csv(pre_bytes, CLEAN_PATH, UTC_PATH, CC_PATH)
        return clean_bytes


def get_tweets_by_hour() -> str:
    return process_by_hour(_download()).decode(ENCODING)


def get_tweets_by_date() -> str:
    return process_by_date(_download()).decode(ENCODING)


def get_tweets_by_weekday() -> str:
    return process_by_weekday(_download()).decode(ENCODING)


def get_tweets_by_week() -> str:
    return process_by_week(_download()).decode(ENCODING)


def get_tweets_by_15min() -> str:
    return process_by_15min(_download()).decode(ENCODING)


def get_total_tweets() -> int:
    return count_tweets(_download())


def get_avg_per_day() -> float:
    return get_average_tweets_per_day(_download())


def get_first_tweet_date() -> str:
    dt = get_first_tweet_timestamp(_download()).astimezone(pytz.timezone('America/New_York'))
    return dt.isoformat()


def get_time_now() -> str:
    # Current time in Eastern Time (ET)
    return datetime.now(pytz.timezone('America/New_York')).isoformat()


def get_data_range() -> int:
    first_tweet = get_first_tweet_timestamp(_download()).astimezone(pytz.timezone('America/New_York'))
    now_et = datetime.now(pytz.timezone('America/New_York'))
    return int((now_et - first_tweet).total_seconds())
