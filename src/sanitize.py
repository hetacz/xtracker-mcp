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
    """Convert a Twitter Snowflake ID to an UTC timezone-aware datetime."""
    ts_ms = (int(snowflake_id) >> 22) + TWITTER_EPOCH_MS
    ts_s = ts_ms / 1000.0
    return datetime.fromtimestamp(ts_s, tz=timezone.utc)


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


def create_clean_timestamps_csv(
        input_data: Union[bytes, str],
        output_path: str,
        output_path_utc: str,
        output_path_cc: str,
        trim_to_months: int = 6,
        encoding: str = 'utf-8',
) -> bytes:
    def _empty_csv_bytes() -> bytes:
        return pd.DataFrame(columns=['timestamp']).to_csv(index=False).encode(encoding)

    def _to_csv_bytes(series: pd.Series) -> bytes:
        df_out = pd.DataFrame({'timestamp': series.map(lambda d: d.isoformat(timespec='milliseconds'))})
        return df_out.to_csv(index=False).encode(encoding)

    def _mask_last_n_months_et(series: pd.Series, months: int = trim_to_months) -> pd.Series:
        now_et = pd.Timestamp.now(tz=ET_TZ)
        cutoff = now_et - pd.DateOffset(months=months)
        return series.map(lambda d: d >= cutoff)

    # Normalize input and read CSV
    file_bytes = input_data if isinstance(input_data, bytes) else input_data.encode(encoding, errors='replace')
    df = _read_csv_file(file_bytes)

    # Handle missing/empty ids
    if 'id' not in df.columns or df.empty:
        empty_csv = _empty_csv_bytes()
        for path in (output_path, output_path_utc, output_path_cc):
            save_tweets_to_csv(empty_csv, path)
        return empty_csv

    # Coerce ids and drop invalid rows
    ids = pd.to_numeric(df['id'], errors='coerce').dropna()
    if ids.empty:
        empty_csv = _empty_csv_bytes()
        for path in (output_path, output_path_utc, output_path_cc):
            save_tweets_to_csv(empty_csv, path)
        return empty_csv

    ids = ids.astype('int64')

    # Use helper _snowflake_to_datetime for UTC and convert to ET
    utc_series = ids.map(_snowflake_to_datetime)
    et_series = utc_series.map(lambda d: d.astimezone(ET_TZ))

    # Compute last trim_to_months months mask via helper
    mask = _mask_last_n_months_et(et_series, months=trim_to_months)

    # Build CSV bytes
    et_csv_bytes = _to_csv_bytes(et_series)
    utc_csv_bytes = _to_csv_bytes(utc_series)
    cc_csv_bytes = _to_csv_bytes(et_series[mask])

    # Save all outputs
    save_tweets_to_csv(et_csv_bytes, output_path)
    save_tweets_to_csv(utc_csv_bytes, output_path_utc)
    save_tweets_to_csv(cc_csv_bytes, output_path_cc)
    ###
    process_by_15min(et_csv_bytes)
    ###
    return et_csv_bytes


def process_by_date(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_date.csv')) -> bytes:
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        today = pd.Timestamp.now(tz=ET_TZ).floor('D')
        out_df = pd.DataFrame({'date_start_et': [today.isoformat()], 'total_count': [0]})
    else:
        date_start = ts.dt.floor('D')  # ET midnight
        end_d = pd.Timestamp.now(tz=ET_TZ).floor('D')
        start_d = ts.min().floor('D')
        all_days = pd.date_range(start=start_d, end=end_d, freq='D', tz=ET_TZ)

        out_df = (
            date_start.rename('date_start_et')
            .to_frame()
            .groupby('date_start_et')
            .size()
            .reindex(all_days, fill_value=0)
            .reset_index(name='total_count')
            .rename(columns={'index': 'date_start_et'})
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
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_week.csv'),
        anchor_weekday: int = 4,
        include_empty: bool = True
) -> bytes:
    def _anchors_noon_weekday_et(ts_et: pd.Series, anchor_weekday: int) -> pd.Series:
        if not isinstance(anchor_weekday, int) or not (0 <= anchor_weekday <= 6):
            raise ValueError("anchor_weekday must be an int in 0..6 (0=Mon .. 6=Sun).")
        days_since_anchor = (ts_et.dt.weekday - anchor_weekday) % 7
        last_anchor_midnight = (ts_et - pd.to_timedelta(days_since_anchor, unit="D")).dt.normalize()
        anchor = last_anchor_midnight + pd.Timedelta(hours=12)
        anchor = anchor.where(ts_et >= anchor, anchor - pd.Timedelta(days=7))
        return anchor  # tz-aware America/New_York

    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        out_df = pd.DataFrame(columns=['week_start_et', 'total_count'])
    else:
        anchors = _anchors_noon_weekday_et(ts, anchor_weekday)
        grouped = (anchors.to_frame(name="anchor_et")
                   .groupby("anchor_et", sort=True).size()
                   .reset_index(name="total_count"))

        if include_empty:
            start, end = grouped["anchor_et"].min(), grouped["anchor_et"].max()
            full_idx = pd.date_range(
                start=start, end=end,
                freq=pd.DateOffset(weeks=1),
                tz="America/New_York")
            grouped = (
                grouped.set_index("anchor_et")
                .reindex(full_idx, fill_value=0)
                .rename_axis("anchor_et").reset_index()
            )

        grouped["week_start_et"] = grouped["anchor_et"].map(lambda x: x.isoformat())
        out_df = grouped[["week_start_et", "total_count"]].sort_values("week_start_et", kind="stable")

    csv_bytes = out_df.to_csv(index=False).encode("utf-8")
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_15min(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_15min.csv'),
        output_path_recent: str = os.path.join(DOWNLOAD_DIR, 'by_15min_recent.csv'),
        output_path_last_tue: str = os.path.join(DOWNLOAD_DIR, 'by_15min_last_tue.csv'),
        output_path_last_fri: str = os.path.join(DOWNLOAD_DIR, 'by_15min_last_fri.csv'),
        include_empty: bool = False,
        months: int = 6,
) -> bytes:
    ts = _timestamps_et_from_bytes(file_bytes)

    if ts.empty:
        out_df = pd.DataFrame(columns=['15m_bucket_start_et', 'total_count'])
        csv_bytes = out_df.to_csv(index=False).encode('utf-8')
        save_tweets_to_csv(csv_bytes, output_path)
        save_tweets_to_csv(csv_bytes, output_path_recent)
        save_tweets_to_csv(csv_bytes, output_path_last_tue)
        save_tweets_to_csv(csv_bytes, output_path_last_fri)
        return csv_bytes

    # Robust ET wall-clock 15-minute flooring without tz-localize ambiguities
    ts_sorted = ts.sort_values(kind='stable')
    local_midnight = ts_sorted.dt.normalize()  # same day 00:00 ET
    secs_since_midnight = (ts_sorted - local_midnight).dt.total_seconds().astype('int64')
    bucket_secs = (secs_since_midnight // (15 * 60)) * (15 * 60)
    bucket_start = local_midnight + pd.to_timedelta(bucket_secs, unit='s')

    grouped = (
        pd.Series(bucket_start, name='15m_bucket_start_et')
        .to_frame()
        .groupby('15m_bucket_start_et', sort=True)
        .size()
        .reset_index(name='total_count')
    )

    if include_empty:
        # Build a complete 15-minute index from the aligned first bucket up to "now" aligned
        start_aligned = bucket_start.min()
        now_et = pd.Timestamp.now(tz=ET_TZ)
        now_midnight = now_et.normalize()
        now_secs = int((now_et - now_midnight).total_seconds())
        now_bucket_secs = (now_secs // (15 * 60)) * (15 * 60)
        now_aligned = now_midnight + pd.to_timedelta(now_bucket_secs, unit='s')
        full_idx = pd.date_range(start=start_aligned, end=now_aligned, freq='15min', tz=ET_TZ)
        grouped = (
            grouped.set_index('15m_bucket_start_et')
            .reindex(full_idx, fill_value=0)
            .rename_axis('15m_bucket_start_et')
            .reset_index()
        )

    # Keep a datetime copy for window filtering before string conversion
    grouped_dt = grouped.copy()

    # Full output: stringify to ISO with seconds precision (no microseconds)
    grouped['15m_bucket_start_et'] = grouped['15m_bucket_start_et'].map(lambda d: d.isoformat(timespec='seconds'))
    out_df = grouped[['15m_bucket_start_et', 'total_count']].sort_values('15m_bucket_start_et', kind='stable')
    full_csv_bytes = out_df.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(full_csv_bytes, output_path)

    # Recent window (last `months`) aligned to 15-min
    now_et = pd.Timestamp.now(tz=ET_TZ)
    cutoff_raw = now_et - pd.DateOffset(months=max(int(months), 0))
    cutoff_midnight = cutoff_raw.normalize()
    cutoff_secs = int((cutoff_raw - cutoff_midnight).total_seconds())
    cutoff_bucket_secs = (cutoff_secs // (15 * 60)) * (15 * 60)
    cutoff_aligned = cutoff_midnight + pd.to_timedelta(cutoff_bucket_secs, unit='s')
    recent = grouped_dt.loc[grouped_dt['15m_bucket_start_et'] >= cutoff_aligned].copy()
    recent['15m_bucket_start_et'] = recent['15m_bucket_start_et'].map(lambda d: d.isoformat(timespec='seconds'))
    recent_out = recent[['15m_bucket_start_et', 'total_count']].sort_values('15m_bucket_start_et', kind='stable')
    recent_csv_bytes = recent_out.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(recent_csv_bytes, output_path_recent)

    def _last_weekday_noon_et(target_weekday: int) -> pd.Timestamp:
        # Mon=0 .. Sun=6
        now = pd.Timestamp.now(tz=ET_TZ)
        today_noon = now.normalize() + pd.Timedelta(hours=12)
        base_noon = today_noon if now >= today_noon else today_noon - pd.Timedelta(days=1)
        delta_days = (base_noon.weekday() - target_weekday) % 7
        return base_noon - pd.Timedelta(days=int(delta_days))

    tue_cutoff = _last_weekday_noon_et(1)  # Tuesday
    fri_cutoff = _last_weekday_noon_et(4)  # Friday

    last_tue = grouped_dt.loc[grouped_dt['15m_bucket_start_et'] >= tue_cutoff].copy()
    last_tue['15m_bucket_start_et'] = last_tue['15m_bucket_start_et'].map(lambda d: d.isoformat(timespec='seconds'))
    last_tue_out = last_tue[['15m_bucket_start_et', 'total_count']].sort_values('15m_bucket_start_et', kind='stable')
    last_tue_csv_bytes = last_tue_out.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(last_tue_csv_bytes, output_path_last_tue)

    last_fri = grouped_dt.loc[grouped_dt['15m_bucket_start_et'] >= fri_cutoff].copy()
    last_fri['15m_bucket_start_et'] = last_fri['15m_bucket_start_et'].map(lambda d: d.isoformat(timespec='seconds'))
    last_fri_out = last_fri[['15m_bucket_start_et', 'total_count']].sort_values('15m_bucket_start_et', kind='stable')
    last_fri_csv_bytes = last_fri_out.to_csv(index=False).encode('utf-8')
    save_tweets_to_csv(last_fri_csv_bytes, output_path_last_fri)
    return full_csv_bytes


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
