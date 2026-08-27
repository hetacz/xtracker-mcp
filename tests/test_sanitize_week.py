import io

import pandas as pd

from src.sanitize import ET_TZ, process_by_week


def _make_csv(timestamps: list[str]) -> bytes:
    df = pd.DataFrame({"id": [str(10 + i) for i in range(len(timestamps))], "timestamp": timestamps})
    return df.to_csv(index=False).encode("utf-8")


def _read_output(csv_bytes: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(csv_bytes))
    if "week_start_et" in df.columns:
        df["week_start_et"] = pd.to_datetime(df["week_start_et"])
    if "week_start_utc" in df.columns:
        df["week_start_utc"] = pd.to_datetime(df["week_start_utc"])
    return df


def test_process_by_week_trims_initial_partial_week(tmp_path):
    timestamps = [
        "2024-05-22T18:00:00Z",  # midweek start, should be dropped
        "2024-05-25T18:00:00Z",  # first full week
        "2024-05-31T18:00:00Z",  # next week
    ]
    csv_bytes = _make_csv(timestamps)

    output_prefix = tmp_path / "by_week"
    result = process_by_week(csv_bytes, output_prefix=str(output_prefix))
    df = _read_output(result)

    expected_first_anchor = pd.Timestamp("2024-05-24T12:00:00", tz=ET_TZ)
    dropped_anchor = pd.Timestamp("2024-05-17T12:00:00", tz=ET_TZ)
    assert df["week_start_et"].min() == expected_first_anchor
    assert not (df["week_start_et"] == dropped_anchor).any()

    counts = dict(zip(df["week_start_et"], df["total_count"]))
    assert counts[expected_first_anchor] == 1
    assert counts[pd.Timestamp("2024-05-31T12:00:00", tz=ET_TZ)] == 1
    assert df["total_count"].sum() == 2


def test_process_by_week_keeps_full_first_week(tmp_path):
    timestamps = [
        "2024-05-24T16:00:00Z",  # aligns with anchor start (Fri noon ET)
        "2024-05-25T15:00:00Z",
    ]
    csv_bytes = _make_csv(timestamps)

    output_prefix = tmp_path / "by_week_full"
    result = process_by_week(csv_bytes, output_prefix=str(output_prefix))
    df = _read_output(result)

    expected_anchor = pd.Timestamp("2024-05-24T12:00:00", tz=ET_TZ)
    assert (df["week_start_et"] == expected_anchor).any()

    counts = dict(zip(df["week_start_et"], df["total_count"]))
    assert counts[expected_anchor] == 2


def test_process_by_week_outputs_utc(tmp_path):
    timestamps = [
        "2024-05-24T16:00:00Z",
        "2024-05-31T16:00:00Z",
    ]
    csv_bytes = _make_csv(timestamps)

    output_prefix = tmp_path / "by_week_fri_utc"
    result = process_by_week(csv_bytes, output_prefix=str(output_prefix), use_utc=True)
    df = _read_output(result)

    assert "week_start_utc" in df.columns
    first_anchor_utc = pd.Timestamp("2024-05-24T16:00:00+00:00")
    assert (df["week_start_utc"] == first_anchor_utc).any()
    counts = dict(zip(df["week_start_utc"], df["total_count"]))
    assert counts[first_anchor_utc] == 1
