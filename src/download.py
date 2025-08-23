import logging
import os
import time
from datetime import datetime

import pytz
import requests

from src.sanitize import DOWNLOAD_DIR, count_tweet_ids, count_tweets, get_average_tweets_per_day, \
    get_first_tweet_timestamp, \
    process_by_date, process_by_hour, process_by_week, process_by_weekday, sanitize_csv_to_file, save_tweets_to_csv

logger = logging.getLogger(__name__)

RAW_PATH = os.path.join(DOWNLOAD_DIR, 'raw_elonmusk.csv')
PRE_PATH = os.path.join(DOWNLOAD_DIR, 'pre_elonmusk.csv')


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
    if _check_modify_date(RAW_PATH) and _check_modify_date(PRE_PATH):
        logger.info('Using cached file: %s', PRE_PATH)
        with open(PRE_PATH, 'rb') as f:
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
        return sanitize_csv_to_file(resp.content, PRE_PATH)


def get_tweets_by_hour() -> str:
    return process_by_hour(_download()).decode('utf-8')


def get_tweets_by_date() -> str:
    return process_by_date(_download()).decode('utf-8')


def get_tweets_by_weekday() -> str:
    return process_by_weekday(_download()).decode('utf-8')


def get_tweets_by_week() -> str:
    return process_by_week(_download()).decode('utf-8')


def get_total_tweets() -> int:
    return count_tweets(_download())


def get_total_tweets_by_id() -> int:
    return count_tweet_ids(_download())


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
