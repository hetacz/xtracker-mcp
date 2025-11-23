import csv
import io
import os
import re
from datetime import datetime, timezone
from typing import Iterable, Union

import pandas as pd
import pytz
from pandas import DataFrame

TWITTER_EPOCH_MS = 1288834974657
ET_TZ = pytz.timezone('America/New_York')
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOWNLOAD_DIR = os.path.join(ROOT_DIR, "downloads")
DOWNLOAD_DIR_MAIN = os.path.join(DOWNLOAD_DIR, "main")
DOWNLOAD_DIR_15 = os.path.join(DOWNLOAD_DIR, "15m")
DOWNLOAD_DIR_15_ET = os.path.join(DOWNLOAD_DIR_15, "et")
DOWNLOAD_DIR_15_UTC = os.path.join(DOWNLOAD_DIR_15, "utc")
ENCODING = "utf-8"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR_MAIN, exist_ok=True)
os.makedirs(DOWNLOAD_DIR_15, exist_ok=True)
os.makedirs(DOWNLOAD_DIR_15_ET, exist_ok=True)
os.makedirs(DOWNLOAD_DIR_15_UTC, exist_ok=True)


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


def _ensure_parent_dir(path: str) -> None:
    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)


def _dataframe_to_csv_bytes(
        df: DataFrame,
        *,
        columns: Iterable[str] | None = None
) -> bytes:
    if columns is not None:
        df = df.loc[:, list(columns)]
    return df.to_csv(index=False).encode(ENCODING)


def _write_dataframe(
        df: DataFrame,
        path: str,
        *,
        columns: Iterable[str] | None = None
) -> bytes:
    csv_bytes = _dataframe_to_csv_bytes(df, columns=columns)
    save_tweets_to_csv(csv_bytes, path)
    return csv_bytes


def _isoformat_series(series: pd.Series, *, timespec: str = "seconds") -> pd.Series:
    return series.map(lambda d: d.isoformat(timespec=timespec))


def _anchors_noon_weekday_et(ts_et: pd.Series, anchor_weekday: int) -> pd.Series:
    if not isinstance(anchor_weekday, int) or not (0 <= anchor_weekday <= 6):
        raise ValueError("anchor_weekday must be an int in 0..6 (0=Mon .. 6=Sun).")
    # Compute target anchor date as local date minus delta days
    w = ts_et.dt.weekday
    delta = (w - anchor_weekday) % 7
    # Get the local dates
    dates = ts_et.dt.tz_convert(ET_TZ).dt.date  # array of Python dates
    # Subtract delta days on the date level
    anchor_dates = pd.to_datetime(dates) - pd.to_timedelta(delta, unit='D')
    # Build naive local noon for those dates, then tz-localize to ET (per-date DST aware)
    anchor_noon_naive = anchor_dates + pd.Timedelta(hours=12)
    anchor_noon = anchor_noon_naive.dt.tz_localize(ET_TZ)
    # If the timestamp is earlier than local noon of that day, drop back one week and rebuild noon
    mask = ts_et < anchor_noon
    prev_week_dates = (anchor_dates - pd.Timedelta(days=7))
    prev_noon = (prev_week_dates + pd.Timedelta(hours=12)).dt.tz_localize(ET_TZ)
    return anchor_noon.where(~mask, prev_noon)


def _floor_to_minutes(ts_et: pd.Series, minutes: int) -> pd.Series:
    """Floor timestamps to ET wall-clock buckets of the given size (in minutes), DST-safe.

    Implementation: convert to UTC, floor, convert back to ET so bucket labels
    align with wall-clock quarter-hour boundaries even across DST switches.
    """
    if minutes <= 0:
        raise ValueError("minutes must be a positive integer")
    ts_utc = ts_et.dt.tz_convert('UTC')
    floored_utc = ts_utc.dt.floor(f"{int(minutes)}min")
    return floored_utc.dt.tz_convert(ET_TZ)


def _align_now_to_minutes(now: pd.Timestamp, minutes: int) -> pd.Timestamp:
    # Align 'now' to the lower wall-clock bucket boundary in a DST-safe way via UTC
    now_utc = now.tz_convert('UTC')
    return now_utc.floor(f"{int(minutes)}min").tz_convert(ET_TZ)


def _write_time_buckets(df: DataFrame, path: str, column: str) -> bytes:
    view = df[[column, 'total_count']].copy()
    view[column] = _isoformat_series(view[column], timespec='seconds')
    return _write_dataframe(view, path)


# compare length with ids found in raw?
def sanitize_csv_to_file(
        input_data: Union[bytes, str],
        output_path: str,
) -> bytes:
    text = input_data.decode(ENCODING, errors='replace') if isinstance(input_data, bytes) else input_data
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
    # Prepare a regex to split each record into exactly 3 parts
    rec_re = re.compile(r'^(\d{19}),(.*),(".*")$')

    with open(output_path, 'w', newline='', encoding=ENCODING) as out:
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
        trim_to_months: int = 6
) -> tuple[bytes, bytes, bytes]:
    """Persist timestamp-only CSVs (ET, UTC, and recent window) derived from sanitized data.
    
    Returns:
        tuple of (et_csv_bytes, utc_csv_bytes, cc_csv_bytes)
    """

    def _empty_csv_bytes() -> bytes:
        return _dataframe_to_csv_bytes(pd.DataFrame(columns=['timestamp']))

    def _to_csv_bytes(series: pd.Series) -> bytes:
        df_out = pd.DataFrame({'timestamp': series.map(lambda d: d.isoformat(timespec='milliseconds'))})
        return _dataframe_to_csv_bytes(df_out)

    def _mask_last_n_months_et(series: pd.Series, months: int = trim_to_months) -> pd.Series:
        now_et = pd.Timestamp.now(tz=ET_TZ)
        cutoff = now_et - pd.DateOffset(months=months)
        return series.map(lambda d: d >= cutoff)

    # Normalize input and read CSV
    file_bytes = input_data if isinstance(input_data, bytes) else input_data.encode(ENCODING, errors='replace')
    df = _read_csv_file(file_bytes)

    # Handle missing/empty ids
    if 'id' not in df.columns or df.empty:
        empty_csv = _empty_csv_bytes()
        for path in (output_path, output_path_utc, output_path_cc):
            save_tweets_to_csv(empty_csv, path)
        return (empty_csv, empty_csv, empty_csv)

    # Coerce ids and drop invalid rows
    ids = pd.to_numeric(df['id'], errors='coerce').dropna()
    if ids.empty:
        empty_csv = _empty_csv_bytes()
        for path in (output_path, output_path_utc, output_path_cc):
            save_tweets_to_csv(empty_csv, path)
        return (empty_csv, empty_csv, empty_csv)

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
    return (et_csv_bytes, utc_csv_bytes, cc_csv_bytes)


def process_by_date(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_date.csv')) -> bytes:
    """Aggregate tweets per calendar day (ET), filling gaps with zero counts."""
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        today = pd.Timestamp.now(tz=ET_TZ).floor('D')
        out_df = pd.DataFrame({'date_start_et': [today.isoformat()], 'total_count': [0]})
        return _write_dataframe(out_df, output_path)

    date_start = ts.dt.floor('D')  # ET midnight
    end_d = pd.Timestamp.now(tz=ET_TZ).floor('D')
    start_d = ts.min().floor('D')
    all_days = pd.date_range(start=start_d, end=end_d, freq='D', tz=ET_TZ)

    grouped = (
        date_start.rename('date_start_et')
        .to_frame()
        .groupby('date_start_et')
        .size()
        .reindex(all_days, fill_value=0)
        .rename_axis('date_start_et')
        .reset_index(name='total_count')
        .sort_values('date_start_et', kind='stable')
    )
    grouped['date_start_et'] = _isoformat_series(grouped['date_start_et'], timespec='seconds')
    return _write_dataframe(grouped, output_path)


def process_by_hour(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_hour.csv')) -> bytes:
    """Aggregate tweets per clock hour (ET) with normalized frequency and a daily average."""
    ts = _timestamps_et_from_bytes(file_bytes)
    hours = pd.Index(range(24), name='hour')
    counts = (
        pd.Series(ts.dt.hour, name='hour')
        .value_counts()
        .reindex(hours, fill_value=0)
        .sort_index()
        .astype('int64')
    )

    days_total, _ = _span_days_and_weekday_occurrences(ts)
    denom = days_total if days_total > 0 else 1
    avg = counts / denom

    total = int(counts.sum())
    normalized = (counts.astype(float) / total) if total > 0 else pd.Series(0.0, index=hours, dtype='float64')

    out_df = pd.DataFrame(
        {
            'hour': hours,
            'total_count': counts.values,
            'avg': avg.values,
            'normalized': normalized.values,
        }
    )
    return _write_dataframe(out_df, output_path)


def process_by_weekday(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_weekday.csv')) -> bytes:
    """Aggregate tweets by weekday with average-per-occurrence and normalized proportions."""
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

    return _write_dataframe(out_df, output_path)


# TODO remove first week maybe as not full data OR remove all data behind this cutoff
def process_by_week(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR, 'by_week.csv'),
        anchor_weekday: int = 4,
        include_empty: bool = True
) -> bytes:
    """Aggregate tweets per anchored week (default: Friday noon) with optional gap filling."""
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        out_df = pd.DataFrame(columns=['week_start_et', 'total_count'])
        return _write_dataframe(out_df, output_path)

    anchors = _anchors_noon_weekday_et(ts, anchor_weekday)
    grouped = (
        anchors.to_frame(name="anchor_et")
        .groupby("anchor_et", sort=True)
        .size()
        .reset_index(name="total_count")
    )

    if include_empty:
        # Build a DST-aware weekly index by iterating local-noon to next week's local-noon
        def _next_week_noon(local_noon: pd.Timestamp) -> pd.Timestamp:
            d = local_noon.tz_convert(ET_TZ).to_pydatetime().date()
            d_next = d + pd.Timedelta(days=7)
            start_naive = pd.Timestamp(year=d_next.year, month=d_next.month, day=d_next.day)
            return (start_naive + pd.Timedelta(hours=12)).tz_localize(ET_TZ)

        start = grouped["anchor_et"].min()
        end = grouped["anchor_et"].max()
        idx = [start]
        cur = start
        # Iterate until we've reached/passed the last anchor
        while cur < end:
            cur = _next_week_noon(cur)
            idx.append(cur)
        full_idx = pd.DatetimeIndex(idx, tz=ET_TZ)

        grouped = (
            grouped.set_index("anchor_et")
            .reindex(full_idx, fill_value=0)
            .rename_axis("anchor_et")
            .reset_index()
        )

    grouped["week_start_et"] = grouped["anchor_et"].map(lambda x: x.isoformat())
    out_df = grouped[["week_start_et", "total_count"]].sort_values("week_start_et", kind="stable")
    return _write_dataframe(out_df, output_path)


def process_by_15min(
        file_bytes: bytes,
        output_path: str = os.path.join(DOWNLOAD_DIR_15_ET, 'by_15min.csv'),
        output_path_recent: str = os.path.join(DOWNLOAD_DIR_15_ET, 'by_15min_recent.csv'),
        output_path_last_tue: str = os.path.join(DOWNLOAD_DIR_15_ET, 'by_15min_last_tue.csv'),
        output_path_last_fri: str = os.path.join(DOWNLOAD_DIR_15_ET, 'by_15min_last_fri.csv'),
        output_path_utc: str = os.path.join(DOWNLOAD_DIR_15_UTC, 'by_15min_utc.csv'),
        output_path_recent_utc: str = os.path.join(DOWNLOAD_DIR_15_UTC, 'by_15min_recent_utc.csv'),
        output_path_last_tue_utc: str = os.path.join(DOWNLOAD_DIR_15_UTC, 'by_15min_last_tue_utc.csv'),
        output_path_last_fri_utc: str = os.path.join(DOWNLOAD_DIR_15_UTC, 'by_15min_last_fri_utc.csv'),
        include_empty: bool = False,
        months: int = 6,
) -> bytes:
    """Aggregate tweets into 15-minute ET buckets plus recent and weekday-specific slices.

    Also writes UTC-variant CSVs for the same buckets, with timestamps converted
    from ET (America/New_York) to UTC. Bucket boundaries are defined in ET and
    then converted to UTC, so the start of the ET series (e.g., 12:00 ET) maps
    to its corresponding UTC instant. UTC timestamps are formatted with a
    trailing 'Z'.
    """
    ts = _timestamps_et_from_bytes(file_bytes)

    def _write_time_buckets_utc_z(df: DataFrame, path: str, column: str) -> bytes:
        """Write UTC 15-minute buckets with timestamps formatted using 'Z'."""
        view = df[[column, 'total_count']].copy()
        # Expect timezone-aware UTC timestamps; format explicitly with Z
        view[column] = view[column].map(lambda d: d.strftime('%Y-%m-%dT%H:%M:%SZ'))
        return _write_dataframe(view, path)

    if ts.empty:
        # Prepare empty ET DataFrame and reuse for ET outputs
        empty_et = pd.DataFrame(columns=['15m_bucket_start_et', 'total_count'])
        empty_bytes_et = _write_dataframe(empty_et, output_path)

        # Empty UTC schema for the four UTC outputs
        empty_utc = pd.DataFrame(columns=['15m_bucket_start_utc', 'total_count'])
        _write_time_buckets_utc_z(empty_utc, output_path_utc, '15m_bucket_start_utc')
        for extra_path in (output_path_recent, output_path_last_tue, output_path_last_fri):
            save_tweets_to_csv(empty_bytes_et, extra_path)
        for extra_path in (output_path_recent_utc, output_path_last_tue_utc, output_path_last_fri_utc):
            _write_time_buckets_utc_z(empty_utc, extra_path, '15m_bucket_start_utc')
        return empty_bytes_et

    bucket_start = _floor_to_minutes(ts, 15)
    grouped = (
        bucket_start.to_frame(name='15m_bucket_start_et')
        .groupby('15m_bucket_start_et', sort=True)
        .size()
        .reset_index(name='total_count')
    )

    if include_empty:
        # Build a complete 15-minute index from the aligned first bucket up to "now" aligned
        start_aligned = bucket_start.min()
        now_aligned = _align_now_to_minutes(pd.Timestamp.now(tz=ET_TZ), 15)
        full_idx = pd.date_range(start=start_aligned, end=now_aligned, freq='15min', tz=ET_TZ)
        grouped = (
            grouped.set_index('15m_bucket_start_et')
            .reindex(full_idx, fill_value=0)
            .rename_axis('15m_bucket_start_et')
            .reset_index()
        )

    # Keep a datetime copy for window filtering before string conversion
    grouped_dt = grouped.copy()

    # Full output (ET)
    grouped_sorted = grouped_dt.sort_values('15m_bucket_start_et', kind='stable')
    full_csv_bytes = _write_time_buckets(grouped_sorted, output_path, '15m_bucket_start_et')

    # Full output (UTC) – convert ET bucket starts to UTC preserving instants
    grouped_utc = grouped_sorted.copy()
    grouped_utc['15m_bucket_start_utc'] = grouped_utc['15m_bucket_start_et'].dt.tz_convert('UTC')
    grouped_utc = grouped_utc[['15m_bucket_start_utc', 'total_count']]
    _write_time_buckets_utc_z(grouped_utc, output_path_utc, '15m_bucket_start_utc')

    # Recent window (last `months`) aligned to 15-min
    now_et = pd.Timestamp.now(tz=ET_TZ)
    cutoff_raw = now_et - pd.DateOffset(months=max(int(months), 0))
    cutoff_aligned = _align_now_to_minutes(cutoff_raw, 15)
    recent = grouped_dt.loc[grouped_dt['15m_bucket_start_et'] >= cutoff_aligned].copy()
    recent_sorted = recent.sort_values('15m_bucket_start_et', kind='stable')
    recent_csv_bytes = _write_time_buckets(recent_sorted, output_path_recent, '15m_bucket_start_et')

    # Recent window (UTC)
    recent_utc = recent_sorted.copy()
    recent_utc['15m_bucket_start_utc'] = recent_utc['15m_bucket_start_et'].dt.tz_convert('UTC')
    recent_utc = recent_utc[['15m_bucket_start_utc', 'total_count']]
    _write_time_buckets_utc_z(recent_utc, output_path_recent_utc, '15m_bucket_start_utc')

    def _last_weekday_noon_et(target_weekday: int) -> pd.Timestamp:
        """Find the most recent occurrence of target_weekday at noon ET.

        If today is the target weekday and we're past noon, return today's noon.
        Otherwise, return the most recent past occurrence.

        Uses calendar date arithmetic to avoid DST issues.

        Args:
            target_weekday: 0=Mon, 1=Tue, ..., 6=Sun
        """
        now = pd.Timestamp.now(tz=ET_TZ)

        # Get today's date as a Python date object
        today_date = now.date()

        # Calculate how many calendar days to go back
        days_back = (now.weekday() - target_weekday) % 7

        # Special case: if today is the target weekday
        if days_back == 0:
            # Check if we're past noon today
            today_noon_naive = pd.Timestamp(
                year=today_date.year, month=today_date.month,
                day=today_date.day, hour=12)
            today_noon = today_noon_naive.tz_localize(ET_TZ)

            if now >= today_noon:
                return today_noon
            else:
                # Before noon, go back 7 days
                days_back = 7

        # Calculate target date using calendar arithmetic
        from datetime import timedelta
        target_date = today_date - timedelta(days=days_back)

        # Construct noon timestamp for that date
        target_noon_naive = pd.Timestamp(
            year=target_date.year, month=target_date.month,
            day=target_date.day, hour=12)
        return target_noon_naive.tz_localize(ET_TZ)

    def _next_week_noon_et(start_noon: pd.Timestamp) -> pd.Timestamp:
        """Return the next week's local noon for the same weekday, DST-aware (167/169h).

        We compute by taking the date part in ET and constructing a new naive
        datetime at 12:00, then tz-localize to ET. This preserves wall-clock noon
        across DST transitions instead of adding a fixed 168h.
        """
        d = start_noon.tz_convert(ET_TZ).to_pydatetime().date()
        d_next = d + pd.Timedelta(days=7)
        # Build naive midnight then add 12:00 and localize
        end_naive_midnight = pd.Timestamp(year=d_next.year, month=d_next.month, day=d_next.day)
        end_noon = (end_naive_midnight + pd.Timedelta(hours=12)).tz_localize(ET_TZ)
        return end_noon

    # Noon-to-noon week windows
    tue_start = _last_weekday_noon_et(1)
    tue_end = _next_week_noon_et(tue_start)
    fri_start = _last_weekday_noon_et(4)
    fri_end = _next_week_noon_et(fri_start)

    # If we're past the corresponding noon boundary, return empty slices
    now_et = pd.Timestamp.now(tz=ET_TZ)

    if now_et >= tue_end:
        empty_tue = pd.DataFrame(
            {
                '15m_bucket_start_et': pd.Series([], dtype='datetime64[ns, America/New_York]'),
                'total_count': pd.Series([], dtype='int64')
            })
        _write_time_buckets(empty_tue, output_path_last_tue, '15m_bucket_start_et')

        empty_tue_utc = pd.DataFrame(
            {
                '15m_bucket_start_utc': pd.Series([], dtype='datetime64[ns, UTC]'),
                'total_count': pd.Series([], dtype='int64')
            })
        _write_time_buckets_utc_z(empty_tue_utc, output_path_last_tue_utc, '15m_bucket_start_utc')
    else:
        last_tue = grouped_dt.loc[(grouped_dt['15m_bucket_start_et'] >= tue_start) &
                                  (grouped_dt['15m_bucket_start_et'] < tue_end)].copy()
        last_tue_sorted = last_tue.sort_values('15m_bucket_start_et', kind='stable')
        _write_time_buckets(last_tue_sorted, output_path_last_tue, '15m_bucket_start_et')

        last_tue_utc = last_tue_sorted.copy()
        last_tue_utc['15m_bucket_start_utc'] = last_tue_utc['15m_bucket_start_et'].dt.tz_convert('UTC')
        last_tue_utc = last_tue_utc[['15m_bucket_start_utc', 'total_count']]
        _write_time_buckets_utc_z(last_tue_utc, output_path_last_tue_utc, '15m_bucket_start_utc')

    # For Friday: since last Friday noon up to now, but empty if past next Friday noon
    if now_et >= fri_end:
        empty_fri = pd.DataFrame(
            {
                '15m_bucket_start_et': pd.Series([], dtype='datetime64[ns, America/New_York]'),
                'total_count': pd.Series([], dtype='int64')
            })
        _write_time_buckets(empty_fri, output_path_last_fri, '15m_bucket_start_et')

        empty_fri_utc = pd.DataFrame(
            {
                '15m_bucket_start_utc': pd.Series([], dtype='datetime64[ns, UTC]'),
                'total_count': pd.Series([], dtype='int64')
            })
        _write_time_buckets_utc_z(empty_fri_utc, output_path_last_fri_utc, '15m_bucket_start_utc')
    else:
        last_fri = grouped_dt.loc[grouped_dt['15m_bucket_start_et'] >= fri_start].copy()
        last_fri_sorted = last_fri.sort_values('15m_bucket_start_et', kind='stable')
        _write_time_buckets(last_fri_sorted, output_path_last_fri, '15m_bucket_start_et')

        last_fri_utc = last_fri_sorted.copy()
        last_fri_utc['15m_bucket_start_utc'] = last_fri_utc['15m_bucket_start_et'].dt.tz_convert('UTC')
        last_fri_utc = last_fri_utc[['15m_bucket_start_utc', 'total_count']]
        _write_time_buckets_utc_z(last_fri_utc, output_path_last_fri_utc, '15m_bucket_start_utc')

    return full_csv_bytes


def count_tweets(file_bytes: bytes) -> int:
    """Return the number of tweets represented by the given CSV bytes."""
    return int(_timestamps_et_from_bytes(file_bytes).shape[0])


# todo if we normalize the data to a specific cutoff modify this to return the first tweet after that cutoff
def get_first_tweet_timestamp(file_bytes: bytes) -> datetime:
    """Return the earliest tweet timestamp in ET, or NaT when no data is present."""
    ts = _timestamps_et_from_bytes(file_bytes)
    return ts.min() if not ts.empty else pd.Timestamp("NaT", tz=ET_TZ)


def get_average_tweets_per_day(file_bytes: bytes) -> float:
    """Return the average tweets per day across the data coverage window."""
    ts = _timestamps_et_from_bytes(file_bytes)
    if ts.empty:
        return 0.0
    total = int(ts.shape[0])
    span_days = max((pd.Timestamp.now(tz=ET_TZ) - ts.min()).total_seconds() / 86400.0, 1e-12)
    return total / span_days


def save_tweets_to_csv(csv_bytes: bytes, output_path: str) -> None:
    """Write CSV bytes to disk ensuring the parent directory exists."""
    _ensure_parent_dir(output_path)
    with open(output_path, 'wb') as f:
        f.write(csv_bytes)
