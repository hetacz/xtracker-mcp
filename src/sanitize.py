import calendar
import csv
import io
import re
from datetime import datetime, timezone
from typing import Union

import pandas as pd

TWITTER_EPOCH_MS = 1288834974657


def _snowflake_to_datetime(snowflake_id: int) -> datetime:
    """Convert a Twitter Snowflake ID to a UTC datetime."""
    ts_ms = (int(snowflake_id) >> 22) + TWITTER_EPOCH_MS
    ts_s = ts_ms / 1000.0
    return datetime.fromtimestamp(ts_s, tz=timezone.utc)


def sanitize_csv_to_file(
        input_data: Union[bytes, str],
        output_path: str,
        encoding: str = 'utf-8'
) -> bytes:
    text = input_data.decode(encoding, errors='replace') if isinstance(input_data, bytes) else input_data
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


def process_by_weekday(file_bytes: bytes, output_path: str = 'by_weekday.csv') -> bytes:
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)
    df['weekday'] = df['timestamp'].dt.weekday
    weekdays = list(range(7))
    counts = (
        df.groupby('weekday')
        .size()
        .reindex(weekdays, fill_value=0)
        .reset_index(name='count')
    )
    avg_per_day = get_average_tweets_per_day(file_bytes)
    counts['count_per_avg_day'] = counts['count'] / avg_per_day if avg_per_day else 0
    total_counts = counts['count'].sum()
    counts['normalized'] = counts['count'] / total_counts if total_counts else 0
    # Map weekday numbers to names
    counts['day'] = counts['weekday'].map(lambda x: calendar.day_name[x])
    # Reorder columns: day, count, count_per_avg_day, normalized
    counts = counts[['day', 'count', 'count_per_avg_day', 'normalized']]
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_hour(file_bytes: bytes, output_path: str = 'by_hour.csv') -> bytes:
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)
    df['hour'] = df['timestamp'].dt.hour
    hours = list(range(24))
    counts = (
        df.groupby('hour')
        .size()
        .reindex(hours, fill_value=0)
        .reset_index(name='count')
    )
    # count per average day
    avg_per_day = get_average_tweets_per_day(file_bytes)
    counts['count_per_avg_day'] = counts['count'] / avg_per_day if avg_per_day != 0 else 0
    # normalized fraction of total
    total_counts = counts['count'].sum()
    counts['normalized'] = counts['count'] / total_counts if total_counts != 0 else 0
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_date(file_bytes: bytes, output_path: str = 'by_date.csv') -> bytes:
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)
    df['date'] = df['timestamp'].apply(lambda dt: dt.date().isoformat())
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
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    return len(df)


def get_first_tweet_timestamp(file_bytes: bytes) -> datetime:
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    # Use the first row's id for the earliest tweet
    first_id = df.loc[0, 'id']
    return _snowflake_to_datetime(first_id)


def get_average_tweets_per_day(file_bytes: bytes) -> float:
    total = count_tweets(file_bytes)
    first_ts = get_first_tweet_timestamp(file_bytes)
    now = datetime.now(timezone.utc)
    elapsed_days = (now - first_ts).total_seconds() / 86400.0
    return total / elapsed_days if elapsed_days > 0 else float('nan')


def save_tweets_to_csv(csv_bytes: bytes, output_path: str) -> None:
    with open(output_path, 'wb') as f:
        f.write(csv_bytes)
