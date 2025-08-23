import calendar
import csv
import io
import os
import re
from datetime import datetime, timezone
from typing import Union

import pandas as pd
import pytz
from pandas import DataFrame

TWITTER_EPOCH_MS = 1288834974657
ET_TZ = pytz.timezone('America/New_York')

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOWNLOAD_DIR = os.path.join(ROOT_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def _snowflake_to_datetime(snowflake_id: int) -> datetime:
    """Convert a Twitter Snowflake ID to an ET (America/New_York) timezone-aware datetime."""
    ts_ms = (int(snowflake_id) >> 22) + TWITTER_EPOCH_MS
    ts_s = ts_ms / 1000.0
    return datetime.fromtimestamp(ts_s, tz=timezone.utc).astimezone(ET_TZ)


def _read_csv_file(file_bytes: bytes) -> DataFrame:
    buffer = io.BytesIO(file_bytes)
    return pd.read_csv(buffer, dtype={'id': 'string'})


def _safe_div(num, den):
    return (num / den) if (pd.notna(den) and den != 0) else 0


# compare length with ids found in raw?
def sanitize_csv_to_file(
        input_data: Union[bytes, str],
        output_path: str,
        encoding: str = 'utf-8'
) -> bytes:
    text = input_data.decode(encoding, errors='replace') if isinstance(input_data, bytes) else input_data
    dir_name = os.path.dirname(output_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    lines = text.splitlines()
    if not lines:
        open(output_path, 'wb').close()
        return b''

    # Reconstruct logical records by detecting lines starting with a 19-digit ID
    pattern_id = re.compile(r'^\d{19},')
    records = []
    buf = lines[0]  # header
    for line in lines[1:]:
        if pattern_id.match(line):
            records.append(buf)
            buf = line
        else:
            buf += ' ' + line
    records.append(buf)

    header = records.pop(0)
    # Prepare regex to split each record into exactly 3 parts
    rec_re = re.compile(r'^(\d{19}),(.*),(".*")$')

    with open(output_path, 'w', newline='', encoding=encoding) as out:
        writer = csv.writer(out)
        # write header
        writer.writerow(next(csv.reader([header])))

        for rec in records:
            m = rec_re.match(rec)
            if m:
                id_f, text_f, ts_quoted = m.group(1), m.group(2), m.group(3)
                ts_f = ts_quoted[1:-1]  # strip outer quotes
            else:
                # fallback: normal CSV split, then rejoin middle columns
                parts = next(csv.reader([rec]))
                id_f = parts[0]
                ts_f = parts[-1]
                text_f = ','.join(parts[1:-1])
            # final sanitize of stray newlines/carriage returns
            text_f = text_f.replace('\n', ' ').replace('\r', ' ')
            writer.writerow([id_f, text_f, ts_f])

    with open(output_path, 'rb') as f:
        return f.read()

    # error_count = 0
    #
    # with open(output_path, 'w', newline='', encoding=encoding) as out:
    #     writer = csv.writer(out)
    #     writer.writerow(next(csv.reader([header])))
    #     for rec in records:
    #         parts = next(csv.reader([rec]))
    #         if len(parts) >= 3:
    #             id_f, ts_f = parts[0], parts[-1]
    #             text_f = ','.join(parts[1:-1])
    #         else:
    #             error_count += 1
    #             continue
    #         text_f = text_f.replace('\n', ' ').replace('\r', ' ')
    #         writer.writerow([id_f, text_f, ts_f])
    #
    # if error_count > 0:
    #     logger.warning(f"Skipped {error_count} records due to malformed CSV format.")
    # else:
    #     logger.info(f"Sanitized CSV saved to {output_path} with no errors")
    #
    # with open(output_path, 'rb') as f:
    #     return f.read()


# TODO remove first week maybe as not full data OR remove all data behind this cutoff
def process_by_week(file_bytes: bytes, output_path: str = os.path.join(DOWNLOAD_DIR, 'by_week.csv')) -> bytes:
    df = _read_csv_file(file_bytes)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)  # already ET
    ts = df['timestamp']
    # Friday=4; find last Friday and set time to 12:00
    days_since_friday = (ts.dt.weekday - 4) % 7
    last_friday_same_week = ts - pd.to_timedelta(days_since_friday, unit='D')
    week_start_et = (last_friday_same_week.dt.floor('D') + pd.Timedelta(hours=12))
    # If tweet is before Fri 12:00, move start back one week
    week_start_et = week_start_et.where(ts >= week_start_et, week_start_et - pd.Timedelta(days=7))
    week_start_et = week_start_et.rename('week_start_et')
    # Count per custom week
    counts = (
        df.groupby(week_start_et)
        .size()
        .reset_index(name='count')
        .sort_values('week_start_et')
    )
    # Label column: ISO date of the custom week start (no time)
    counts['week'] = counts['week_start_et'].dt.strftime('%Y-%m-%d')
    # Final column order
    out_df = counts[['week', 'count']]
    out = io.BytesIO()
    out_df.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_weekday(file_bytes: bytes, output_path: str = os.path.join(DOWNLOAD_DIR, 'by_weekday.csv')) -> bytes:
    df = _read_csv_file(file_bytes)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)  # already ET
    df['weekday'] = df['timestamp'].dt.weekday
    weekdays = list(range(7))
    counts = (
        df.groupby('weekday')
        .size()
        .reindex(weekdays, fill_value=0)
        .reset_index(name='count')
    )
    avg_per_day = get_average_tweets_per_day(file_bytes)
    counts['count_per_avg_day'] = counts['count'].apply(lambda c: _safe_div(c, avg_per_day))
    total_counts = counts['count'].sum()
    counts['normalized'] = counts['count'].apply(lambda c: _safe_div(c, total_counts))
    # Map weekday numbers to names
    counts['day'] = counts['weekday'].map(lambda x: calendar.day_name[x])
    # Reorder columns: day, count, count_per_avg_day, normalized
    counts = counts[['day', 'count', 'count_per_avg_day', 'normalized']]
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_hour(file_bytes: bytes, output_path: str = os.path.join(DOWNLOAD_DIR, 'by_hour.csv')) -> bytes:
    df = _read_csv_file(file_bytes)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)  # already ET
    df['weekday'] = df['timestamp'].dt.weekday
    df['hour'] = df['timestamp'].dt.hour
    hours = list(range(24))
    counts = (
        df.groupby('hour')
        .size()
        .reindex(hours, fill_value=0)
        .reset_index(name='count')
    )
    avg_per_day = get_average_tweets_per_day(file_bytes)
    counts['count_per_avg_day'] = counts['count'].apply(lambda c: _safe_div(c, avg_per_day))
    total_counts = counts['count'].sum()
    counts['normalized'] = counts['count'].apply(lambda c: _safe_div(c, total_counts))
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_date(file_bytes: bytes, output_path: str = os.path.join(DOWNLOAD_DIR, 'by_date.csv')) -> bytes:
    df = _read_csv_file(file_bytes)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)  # already ET
    df['weekday'] = df['timestamp'].dt.weekday
    df['date'] = df['timestamp'].dt.date.astype(str)
    counts = (
        df.groupby('date')
        .size()
        .reset_index(name='count')
        .sort_values('date')
    )
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def count_tweets(file_bytes: bytes) -> int:
    return len(_read_csv_file(file_bytes))


def get_first_tweet_timestamp(file_bytes: bytes) -> datetime:
    df = _read_csv_file(file_bytes)
    return df['id'].apply(_snowflake_to_datetime).min()
    # Use the first row's id for the earliest tweet
    # first_id = df.loc[0, 'id']
    # return _snowflake_to_datetime(first_id)


def get_average_tweets_per_day(file_bytes: bytes) -> float:
    total = count_tweets(file_bytes)
    first_ts = get_first_tweet_timestamp(file_bytes)
    # Use Eastern Time for current time; duration unaffected by timezone choice
    now = datetime.now(ET_TZ)
    elapsed_days = (now - first_ts).total_seconds() / 86400.0
    return total / elapsed_days


def save_tweets_to_csv(csv_bytes: bytes, output_path: str) -> None:
    dir_name = os.path.dirname(output_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with open(output_path, 'wb') as f:
        f.write(csv_bytes)


# Todo: may serve as double check if parsed csv has correct length
def count_tweet_ids(input_data: Union[bytes, str], encoding: str = "utf-8", unique: bool = False) -> int:
    text = input_data.decode(encoding, errors="replace") if isinstance(input_data, bytes) else input_data
    text = text.lstrip("\ufeff")  # strip BOM if present
    pat = re.compile(r"(?m)^(\d{19})(?=,)")

    if unique:
        seen = set()
        for m in pat.finditer(text):
            seen.add(m.group(1))
        return len(seen)
    else:
        return sum(1 for _ in pat.finditer(text))
