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


def process_by_hour(file_bytes: bytes, output_path='by_hour') -> bytes:
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)
    df['hour'] = df['timestamp'].apply(lambda dt: dt.hour)
    hours = list(range(24))
    counts = (
        df.groupby('hour')
        .size()
        .reindex(hours, fill_value=0)
        .reset_index(name='count')
    )
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    csv_bytes = out.getvalue()
    # Save to file
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_by_date(file_bytes: bytes, output_path='by_date') -> bytes:
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
    # Save to file
    save_tweets_to_csv(csv_bytes, output_path)
    return csv_bytes


def process_tweets(file_bytes: bytes) -> bytes:
    """
    Process a CSV of tweets provided as bytes, remove unwanted columns, decode Snowflake IDs
    to UTC timestamps, group timestamps into 3-hour brackets, and return the result as CSV bytes.
    """
    buffer = io.BytesIO(file_bytes)
    df = pd.read_csv(buffer)
    df = df.drop(columns=['text', 'created_at'], errors='ignore')
    df['timestamp'] = df['id'].apply(_snowflake_to_datetime)
    # Round down to nearest 3-hour bracket
    df['bracket'] = df['timestamp'].apply(
        lambda dt: dt.replace(
            hour=(dt.hour // 3) * 3, minute=0, second=0, microsecond=0
        ))
    counts = (
        df.groupby('bracket')
        .size()
        .reset_index(name='count')
        .sort_values('bracket')
    )
    out = io.BytesIO()
    counts.to_csv(out, index=False)
    return out.getvalue()


def save_tweets_to_csv(csv_bytes: bytes, output_path: str) -> None:
    with open(output_path, 'wb') as f:
        f.write(csv_bytes)

# def parse_tweets_csv(
#         csv_payload: bytes
# ) -> List[Dict[str, str]]:
#     # Prepare text buffer
#     text = csv_payload.decode('utf-8', errors='replace')
#     buf = io.StringIO(text)
#
#     reader = csv.DictReader(buf, quotechar='"', doublequote=True)
#     records: List[Dict[str, str]] = []
#
#     for row in reader:
#         # Filter out None keys that can occur with malformed CSV data
#         filtered_row = {k: v for k, v in row.items() if k is not None}
#
#         # Remove text and id from row, keep others like 'created_at'
#         filtered_row.pop('text', None)
#         id_val = filtered_row.pop('id', None)
#         if not id_val:
#             continue
#         # Decode Snowflake
#         try:
#             sf = int(id_val)
#             ts_ms = (sf >> 22) + TWITTER_EPOCH_MS
#             dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
#             filtered_row['timestamp_utc'] = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
#         except (ValueError, TypeError):
#             filtered_row['timestamp_utc'] = ''
#         records.append(filtered_row)
#
#     return records
#
#
# def save_tweets_to_csv(
#         records: List[Dict[str, str]],
#         csv_path: Union[str, Path]
# ) -> str:
#     if not records:
#         raise ValueError("No records to save.")
#
#     # Determine CSV header order - ensure no None keys
#     header = [k for k in records[0].keys() if k is not None]
#
#     # Generate CSV content in-memory
#     buffer = io.StringIO()
#     writer = csv.DictWriter(buffer, fieldnames=header, quotechar='"', doublequote=True)
#     writer.writeheader()
#     for rec in records:
#         # Filter out None keys before writing
#         clean_rec = {k: v for k, v in rec.items() if k is not None}
#         writer.writerow(clean_rec)
#     csv_content = buffer.getvalue()
#     buffer.close()
#
#     # Ensure directory exists and remove old file
#     path = Path(csv_path)
#     path.parent.mkdir(parents=True, exist_ok=True)
#     if path.exists():
#         path.unlink()
#
#     # Write content to file
#     with path.open('w', newline='', encoding='utf-8') as f:
#         f.write(csv_content)
#
#     return csv_content

# def parse_tweets_csv(
#         csv_payload: Union[bytes, str]
# ) -> pd.DataFrame:
#
#     # Decode payload to text
#     text = csv_payload.decode('utf-8') if isinstance(csv_payload, (bytes, bytearray)) else csv_payload
#     lines = text.splitlines()
#     if not lines:
#         return pd.DataFrame()
#     # Assume header on first line: id,text,created_at
#     header = lines[0]
#     expected_cols = ['id', 'text', 'created_at']
#     if header.split(',')[:3] != expected_cols:
#         # Unexpected header
#         return pd.DataFrame()
#
#     records = []
#     for line in lines[1:]:
#         # Skip blank lines
#         if not line.strip():
#             continue
#         # Split at first comma and last comma
#         first_comma = line.find(',')
#         last_comma = line.rfind(',')
#         if first_comma == -1 or last_comma == -1 or first_comma == last_comma:
#             continue
#         id_val = line[:first_comma]
#         text_field = line[first_comma + 1:last_comma]
#         created_at = line[last_comma + 1:]
#         # Strip surrounding quotes from text field if present
#         if text_field.startswith('"') and text_field.endswith('"'):
#             text_field = text_field[1:-1]
#         # Replace double-double-quotes with single quote
#         text_field = text_field.replace('""', '"')
#         records.append({'id': id_val, 'text': text_field, 'created_at': created_at})
#     if not records:
#         return pd.DataFrame()
#     df = pd.DataFrame(records)
#     # Immediately drop 'text'
#     df.pop('text')
#
#     # Convert id Snowflake to UTC timestamp
#     def snowflake_to_utc(id_val: str) -> pd.Timestamp:
#         try:
#             sf = int(id_val)
#         except (ValueError, TypeError):
#             return pd.NaT
#         ts_ms = (sf >> 22) + TWITTER_EPOCH_MS
#         return pd.Timestamp(datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc))
#
#     df['timestamp_utc'] = df['id'].apply(snowflake_to_utc)
#     # Drop original id column
#     df.pop('id')
#     return df
#
#
# def save_tweets_to_csv_str(
#         df: pd.DataFrame,
#         csv_path: Union[str, Path]
# ) -> str:
#     if df is None or df.empty:
#         raise ValueError("DataFrame is empty or None; nothing to save.")
#     if 'timestamp_utc' not in df.columns:
#         raise ValueError("Missing 'timestamp_utc' column; ensure parse_tweets_csv was run.")
#
#     # Prepare DataFrame copy
#     out_df = df.copy()
#     # Format timestamp to ISO
#     out_df['timestamp_utc'] = out_df['timestamp_utc'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
#
#     # Ensure output directory exists
#     path = Path(csv_path)
#     path.parent.mkdir(parents=True, exist_ok=True)
#
#     # Remove existing file if it exists
#     try:
#         if path.exists():
#             path.unlink()
#     except OSError as e:
#         raise OSError(f"Could not remove existing file: {e}")
#
#     # Write new CSV
#     out_df.to_csv(path, index=False)
#     return out_df.to_string(index=False, header=True)
