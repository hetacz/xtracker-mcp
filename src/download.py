import os
import time
from datetime import datetime, timezone

import requests

from src.sanitize import count_tweets, get_average_tweets_per_day, get_first_tweet_timestamp, process_by_date, \
    process_by_hour, \
    process_by_weekday, sanitize_csv_to_file, save_tweets_to_csv


def _download() -> bytes:
    """
    Download the full Elon Musk tweet CSV if local file is older than 5 minutes;
    otherwise use the existing file. Sanitize, process, and save aggregated result.

    Returns the processed CSV content as a UTF-8 string.
    """
    raw_path = 'raw_elonmusk.csv'
    pre_path = 'pre_elonmusk.csv'
    # Check cache freshness
    if os.path.exists(raw_path) and os.path.exists(pre_path) and (
            time.time() - os.path.getmtime(raw_path) < 5 * 60) and (time.time() - os.path.getmtime(pre_path) < 5 * 60):
        print('Using cached file')
        with open(pre_path, 'rb') as f:
            return f.read()
    else:
        print('Downloading fresh data')
        resp = requests.post(
            'https://www.xtracker.io/api/download',
            json={'handle': 'elonmusk', 'platform': 'X'},
            headers={'Content-Type': 'application/json', 'media-type': 'text/event-stream'},
            timeout=30,
        )
        resp.raise_for_status()
        print('status:', resp.status_code)
        save_tweets_to_csv(resp.content, raw_path)
        return sanitize_csv_to_file(resp.content, pre_path)
    # Process and save
    # parsed_bytes = process_tweets(sanitized_bytes)
    # save_tweets_to_csv(parsed_bytes, 'elonmusk.csv')
    # return parsed_bytes.decode('utf-8')


def get_tweets_by_hour() -> str:
    return process_by_hour(_download()).decode('utf-8')


def get_tweets_by_date() -> str:
    return process_by_date(_download()).decode('utf-8')


def get_tweets_by_weekday() -> str:
    return process_by_weekday(_download()).decode('utf-8')


def get_total_tweets() -> int:
    return count_tweets(_download())


def get_avg_per_day() -> float:
    return get_average_tweets_per_day(_download())


def get_first_tweet_date() -> str:
    return get_first_tweet_timestamp(_download()).isoformat()


def get_time_now() -> str:
    return datetime.now().isoformat()


def get_data_range() -> float:
    first_tweet = get_first_tweet_timestamp(_download())
    return (datetime.now(timezone.utc) - first_tweet).total_seconds()
