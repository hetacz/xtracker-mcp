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


# todo if bracket missing add date with 0

def _snowflake_to_datetime(snowflake_id: int) -> datetime:
    """Convert a Twitter Snowflake ID to an ET (America/New_York) timezone-aware datetime."""
    ts_ms = (int(snowflake_id) >> 22) + TWITTER_EPOCH_MS
    ts_s = ts_ms / 1000.0
    return datetime.fromtimestamp(ts_s, tz=timezone.utc).astimezone(ET_TZ)


def _timestamps_et_from_bytes(file_bytes: bytes) -> pd.Series:
    """Parse bytes -> tz-aware America/New_York timestamps (header is guaranteed)."""
    df = _read_csv_file(file_bytes)  # must expose a 'timestamp' column
    ts_utc = pd.to_datetime(df['timestamp'], utc=True, errors='coerce').dropna()
    return ts_utc.dt.tz_convert(ET_TZ)


def _span_days_and_weekday_occurrences(ts: pd.Series) -> tuple[int, pd.Series]:
    if ts.empty:
        return 0, pd.Series([0] * 7, index=range(7))
    start_d = ts.min().floor('D')
    end_d = pd.Timestamp.now(tz=ET_TZ).floor('D')
    dr = pd.date_range(start=start_d, end=end_d, freq='D', tz=ET_TZ)
    days_total = len(dr)
    weekday_occ = (
        pd.Series(dr.weekday, name='weekday')
        .value_counts()
        .reindex(range(7), fill_value=0)
        .sort_index()
    )
    return days_total, weekday_occ


def _read_csv_file(file_bytes: bytes) -> DataFrame:
    buffer = io.BytesIO(file_bytes)
    return pd.read_csv(buffer, dtype={'id': 'string'})


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


def create_clean_timestamps_csv(
        input_data: Union[bytes, str],
        output_path: str,
        encoding: str = 'utf-8'
) -> bytes:
    # Normalize input to bytes for pandas
    file_bytes = input_data if isinstance(input_data, bytes) else input_data.encode(encoding, errors='replace')
    df = _read_csv_file(file_bytes)

    if 'id' not in df.columns:
        # Produce an empty CSV with just the header if id column is missing
        out = io.BytesIO()
        pd.DataFrame(columns=['timestamp']).to_csv(out, index=False)
        csv_bytes = out.getvalue()
        save_tweets_to_csv(csv_bytes, output_path)
        return csv_bytes

    ts_series = df['id'].apply(_snowflake_to_datetime).apply(lambda d: d.isoformat())
    out_df = pd.DataFrame({'timestamp': ts_series})

    out = io.BytesIO()
    out_df.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_date(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_date.csv')) -> bytes:
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        out_df = pd.DataFrame(columns=['date_start_et', 'total_count'])
    else:
        date_start = ts.dt.floor('D')  # ET midnight
        out_df = (
            date_start.rename('date_start_et')
            .to_frame()
            .groupby('date_start_et')
            .size()
            .reset_index(name='total_count')
            .sort_values('date_start_et')
        )
        out_df['date_start_et'] = out_df['date_start_et'].map(lambda d: d.isoformat())

    csv_bytes = out_df.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_hour(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_hour.csv')) -> bytes:
    ts = _timestamps_et_from_bytes(file_bytes)
    hours = list(range(24))

    if ts.empty:
        out_df = pd.DataFrame({'hour': hours, 'total_count': [0] * 24})
    else:
        out_df = (
            pd.Series(ts.dt.hour, name='hour')
            .to_frame()
            .groupby('hour')
            .size()
            .reindex(hours, fill_value=0)
            .reset_index(name='total_count')
            .astype({'total_count': 'int64'})
        )

    # Average tweets per hour across the span (include zero-tweet days)
    days_total, _ = _span_days_and_weekday_occurrences(ts)
    denom = days_total if days_total > 0 else 1
    out_df['avg'] = out_df['total_count'] / denom

    total = int(out_df['total_count'].sum())
    out_df['normalized'] = (out_df['total_count'] / total) if total > 0 else 0.0

    csv_bytes = out_df.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_weekday(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_weekday.csv')) -> bytes:
    ts = _timestamps_et_from_bytes(file_bytes)
    days_labels = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # How many occurrences of each weekday in the date span (Mon=0..Sun=6)
    _, weekday_occ = _span_days_and_weekday_occurrences(ts)

    # Total tweets per weekday
    counts = (
        pd.Series(ts.dt.weekday, name='weekday')
        .to_frame()
        .groupby('weekday')
        .size()
        .reindex(range(7), fill_value=0)
        .astype(int)
    )

    # Average tweets per that weekday across the span (zeros included)
    denom = weekday_occ.replace(0, pd.NA)
    avg_per_weekday = (counts / denom).fillna(0.0)

    total = int(counts.sum())
    norm = (counts / total).fillna(0.0) if total > 0 else counts.astype(float)

    out_df = pd.DataFrame(
        {
            'day': days_labels,
            'total_count': counts.values,
            'avg': avg_per_weekday.values,
            'normalized': norm.values,
        })

    csv_bytes = out_df.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


# TODO remove first week maybe as not full data OR remove all data behind this cutoff
def process_by_week(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_week.csv')) -> bytes:
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        out_df = pd.DataFrame(columns=['week_start_et', 'total_count'])
    else:
        # Friday = 4; anchor at Fri 12:00 ET
        days_since_friday = (ts.dt.weekday - 4) % 7
        last_friday = ts - pd.to_timedelta(days_since_friday, unit='D')
        week_start = last_friday.dt.floor('D') + pd.Timedelta(hours=12)
        week_start = week_start.where(ts >= week_start, week_start - pd.Timedelta(days=7))

        out_df = (
            week_start.rename('week_start_et')
            .to_frame()
            .groupby('week_start_et')
            .size()
            .reset_index(name='total_count')
            .sort_values('week_start_et')
        )
        out_df['week_start_et'] = out_df['week_start_et'].map(lambda d: d.isoformat())

    csv_bytes = out_df.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def count_tweets(file_bytes: bytes) -> int:
    # return len(_read_csv_file(file_bytes))
    return int(_timestamps_et_from_bytes(file_bytes).shape[0])


# todo if we normalize the data to a specific cutoff modify this to return the first tweet after that cutoff
def get_first_tweet_timestamp(file_bytes: bytes) -> datetime:
    # df = _read_csv_file(file_bytes)
    # return df['id'].apply(_snowflake_to_datetime).min()
    # Use the first row's id for the earliest tweet
    # first_id = df.loc[0, 'id']
    # return _snowflake_to_datetime(first_id)
    ts = _timestamps_et_from_bytes(file_bytes)
    return ts.min() if not ts.empty else pd.NaT.tz_localize(ET_TZ)


def get_average_tweets_per_day(file_bytes: bytes) -> float:
    # total = count_tweets(file_bytes)
    # first_ts = get_first_tweet_timestamp(file_bytes)
    # # Use Eastern Time for current time; duration unaffected by timezone choice
    # now = datetime.now(ET_TZ)
    # elapsed_days = (now - first_ts).total_seconds() / 86400.0
    # return total / elapsed_days
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        return 0.0
    total = int(ts.shape[0])
    span_days = max((pd.Timestamp.now(tz=ET_TZ) - ts.min()).total_seconds() / 86400.0, 1e-12)
    return total / span_days


def save_tweets_to_csv(csv_bytes: bytes, output_path: str) -> None:
    dir_name = os.path.dirname(output_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with open(output_path, 'wb') as f:
        f.write(csv_bytes)
