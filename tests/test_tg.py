"""Focused tests for the public Telegram fallback endpoint."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import pandas as pd
from starlette.requests import Request

import main
from src import download_tg
from src.sanitize import process_by_15min_pair


def _snowflake(timestamp: datetime, sequence: int = 0) -> str:
    epoch_ms = 1_288_834_974_657
    timestamp_ms = int(timestamp.timestamp() * 1_000)
    return str(((timestamp_ms - epoch_ms) << 22) + sequence)


class _FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, pages: list[bytes]) -> None:
        self.pages = pages
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return _FakeResponse(self.pages[len(self.calls) - 1])


def test_post_filter_keeps_explicit_self_reply() -> None:
    message: download_tg.TelegramMessage = {
        "message_id": 1,
        "telegram_datetime": "",
        "label": "Reply",
        "reply_to_username": "ElonMusk",
        "alert_text": "continuation",
        "rendered_text": "[Reply] @ElonMusk continuation",
        "status_ids": ["1"],
        "links": [],
    }

    assert download_tg._post_filter(message) == (True, "self_reply")


def test_fetch_saves_full_post_data_and_fails_closed_on_telegram_labels(
    monkeypatch,
    tmp_path,
) -> None:
    newest = datetime(2026, 8, 27, 12, 17, tzinfo=timezone.utc)
    self_reply = datetime(2026, 8, 27, 12, 16, tzinfo=timezone.utc)
    older = datetime(2026, 8, 27, 11, 58, tzinfo=timezone.utc)
    newest_id = _snowflake(newest)
    self_reply_id = _snowflake(self_reply)
    older_id = _snowflake(older)
    reply_id = _snowflake(datetime(2026, 8, 27, 12, 10, tzinfo=timezone.utc))
    first_page = f"""
    <div class="tgme_widget_message_wrap"><div data-post="elonalert/101">
      <div class="tgme_widget_message_text">unlabeled</div>
      <a href="https://x.com/elonmusk/status/123">View</a>
    </div></div>
    <div class="tgme_widget_message_wrap"><div data-post="elonalert/102">
      <div class="tgme_widget_message_text">12:10:00 [Reply] @someone no</div>
      <a class="tgme_widget_message_inline_button url_button"
         href="https://x.com/elonmusk/status/{reply_id}">View</a>
    </div></div>
    <div class="tgme_widget_message_wrap"><div data-post="elonalert/103">
      <div class="tgme_widget_message_text">12:17:00 [Tweet] newest</div>
      <a class="tgme_widget_message_inline_button url_button"
         href="https://x.com/elonmusk/status/{newest_id}">View</a>
      <a href="https://t.me/elonalert/103"><time datetime="2026-08-27T12:17:08+00:00">12:17</time></a>
    </div></div>
    <div class="tgme_widget_message_wrap"><div data-post="elonalert/104">
      <div class="tgme_widget_message_text">12:16:00 [Reply] self continuation</div>
      <a class="tgme_widget_message_inline_button url_button"
         href="https://x.com/elonmusk/status/{self_reply_id}">View</a>
      <a href="https://t.me/elonalert/104"><time datetime="2026-08-27T12:16:07+00:00">12:16</time></a>
    </div></div>
    """.encode()
    second_page = f"""
    <div class="tgme_widget_message_wrap"><div data-post="elonalert/100">
      <div class="tgme_widget_message_text">11:58:00 [Quote] older</div>
      <a class="tgme_widget_message_inline_button url_button"
         href="https://twitter.com/elonmusk/status/{older_id}">View</a>
      <a href="https://t.me/elonalert/100"><time datetime="2026-08-27T11:58:09+00:00">11:58</time></a>
    </div></div>
    """.encode()
    session = _FakeSession([first_page, second_page])
    raw_path = tmp_path / "raw_elonmusk_tg.csv"
    monkeypatch.setattr(download_tg, "TG_RAW_OUTPUT_PATH", str(raw_path))

    result = download_tg.fetch_latest_non_reply_posts(
        5,
        session=session,  # type: ignore[arg-type]
    )

    assert result == [
        (newest_id, newest),
        (self_reply_id, self_reply),
        (older_id, older),
    ]
    assert len(session.calls) == 2
    assert session.calls[0]["url"] == "https://t.me/s/elonalert"
    assert session.calls[0]["params"] is None
    assert session.calls[1]["params"] == {"before": "101"}
    raw = pd.read_csv(raw_path, dtype="string", keep_default_na=False)
    assert list(raw.columns) == list(download_tg.TG_RAW_FIELDS)
    records = raw.to_dict("records")
    by_id = {row["id"]: row for row in records}
    assert len(records) == 5
    assert by_id[newest_id]["alert_text"] == "newest"
    assert by_id[newest_id]["included_in_15m"] == "true"
    assert by_id[newest_id]["filter_reason"] == "non_reply"
    assert by_id[self_reply_id]["label"] == "Reply"
    assert by_id[self_reply_id]["reply_to_username"] == ""
    assert by_id[self_reply_id]["included_in_15m"] == "true"
    assert by_id[self_reply_id]["filter_reason"] == "self_reply"
    assert by_id[reply_id]["reply_to_username"] == "someone"
    assert by_id[reply_id]["included_in_15m"] == "false"
    assert by_id[reply_id]["filter_reason"] == "reply_to_other"
    assert by_id["123"]["filter_reason"] == "unlabeled"
    assert records[0]["created_at_utc"] == "2026-08-27T12:17:00.000Z"
    assert records[0]["telegram_message_id"] == "103"
    assert records[0]["telegram_datetime"] == "2026-08-27T12:17:08+00:00"
    assert records[0]["alert_delay_seconds"] == "8.000"
    assert records[0]["telegram_url"] == "https://t.me/elonalert/103"
    assert records[0]["x_url"] == f"https://x.com/elonmusk/status/{newest_id}"
    assert f"https://x.com/elonmusk/status/{newest_id}" in json.loads(
        records[0]["source_links_json"],
    )
    assert not list(tmp_path.rglob("*.html"))


def test_process_by_15min_pair_writes_et_and_utc_files(tmp_path) -> None:
    timestamps = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "timestamp": [
                "2024-01-15T15:01:00Z",
                "2024-01-15T15:14:59Z",
                "2024-01-15T15:15:00Z",
            ],
        },
    ).to_csv(index=False).encode()
    et_prefix = str(tmp_path / "posts_by_15min")
    utc_prefix = str(tmp_path / "posts_by_15min_utc")

    et_bytes, utc_bytes = process_by_15min_pair(
        timestamps,
        output_prefix=et_prefix,
        output_prefix_utc=utc_prefix,
    )

    et = pd.read_csv(io.BytesIO(et_bytes))
    utc = pd.read_csv(io.BytesIO(utc_bytes))
    assert et.to_dict("records") == [
        {"15m_bucket_start_et": "2024-01-15T10:00:00-05:00", "total_count": 2},
        {"15m_bucket_start_et": "2024-01-15T10:15:00-05:00", "total_count": 1},
    ]
    assert utc.to_dict("records") == [
        {"15m_bucket_start_utc": "2024-01-15T15:00:00Z", "total_count": 2},
        {"15m_bucket_start_utc": "2024-01-15T15:15:00Z", "total_count": 1},
    ]
    assert (tmp_path / "posts_by_15min.csv").read_bytes() == et_bytes
    assert (tmp_path / "posts_by_15min_utc.csv").read_bytes() == utc_bytes


def test_fetch_tg_posts_writes_named_pair_and_returns_requested_timezone(
    monkeypatch,
    tmp_path,
) -> None:
    posts = [
        (
            _snowflake(datetime(2026, 8, 27, 12, 17, tzinfo=timezone.utc)),
            datetime(2026, 8, 27, 12, 17, tzinfo=timezone.utc),
        ),
    ]
    et_prefix = tmp_path / "elonmusk_tg"
    utc_prefix = tmp_path / "elonmusk_tg_utc"
    monkeypatch.setattr(download_tg, "fetch_latest_non_reply_posts", lambda n: posts)
    monkeypatch.setattr(download_tg, "TG_OUTPUT_PREFIX", str(et_prefix))
    monkeypatch.setattr(download_tg, "TG_OUTPUT_PREFIX_UTC", str(utc_prefix))

    result = download_tg.fetch_tg_posts_by_15min(1, utc=True)

    assert result.startswith("15m_bucket_start_utc,total_count")
    assert et_prefix.with_suffix(".csv").exists()
    assert utc_prefix.with_suffix(".csv").exists()


def test_tg_http_route_applies_defaults_and_selects_utc(monkeypatch) -> None:
    calls: list[tuple[int, bool]] = []

    def fake_fetch(n: int, utc: bool) -> str:
        calls.append((n, utc))
        return "15m_bucket_start_utc,total_count\n" if utc else "15m_bucket_start_et,total_count\n"

    monkeypatch.setattr(main, "fetch_tg_posts_by_15min", fake_fetch)

    def call_handler(query: bytes) -> tuple[int, str]:
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/tg/fetch",
                "query_string": query,
                "headers": [],
            },
        )
        response = main._tg_fetch_handler(request)
        return response.status_code, response.body.decode()

    default_status, default_body = call_handler(b"")
    utc_status, utc_body = call_handler(b"n=25&utc=true")
    invalid_status, _ = call_handler(b"n=0")

    assert any(route.path == "/tg/fetch" for route in main.app.routes)
    assert default_status == 200
    assert default_body.startswith("15m_bucket_start_et")
    assert utc_status == 200
    assert utc_body.startswith("15m_bucket_start_utc")
    assert invalid_status == 400
    assert calls == [(1_000, False), (25, True)]
