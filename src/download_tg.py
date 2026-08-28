"""Fetch recent Elon Musk post alerts from the public @elonalert Telegram channel."""

from __future__ import annotations

import csv
import html
import io
import json
import logging
import os
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import TypedDict

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.sanitize import DOWNLOAD_DIR_15, process_by_15min_pair, save_tweets_to_csv

logger = logging.getLogger(__name__)

TG_CHANNEL = "elonalert"
TG_PUBLIC_URL = f"https://t.me/s/{TG_CHANNEL}"
TG_USER_AGENT = "xtracker-mcp-telegram-fallback/1.0"
DEFAULT_POST_COUNT = 1_000
ENCODING = "utf-8"
TWITTER_EPOCH_MS = 1_288_834_974_657
NON_REPLY_LABELS = frozenset({"Tweet", "Quote", "ReTweet"})
SELF_REPLY_USERNAMES = frozenset({"elonmusk"})

DOWNLOAD_DIR_15_TG = os.path.join(DOWNLOAD_DIR_15, "tg")
TG_RAW_OUTPUT_PATH = os.path.join(DOWNLOAD_DIR_15_TG, "raw_elonmusk_tg.csv")
TG_OUTPUT_PREFIX = os.path.join(DOWNLOAD_DIR_15_TG, "elonmusk_tg")
TG_OUTPUT_PREFIX_UTC = os.path.join(DOWNLOAD_DIR_15_TG, "elonmusk_tg_utc")
TG_OUTPUT_PATH = f"{TG_OUTPUT_PREFIX}.csv"
TG_OUTPUT_PATH_UTC = f"{TG_OUTPUT_PREFIX_UTC}.csv"

WRAP_RE = re.compile(r'<div class="tgme_widget_message_wrap[^>]*>')
MESSAGE_ID_RE = re.compile(r'data-post="(?P<channel>[A-Za-z0-9_]+)/(?P<id>\d+)"')
DATETIME_RE = re.compile(r'<time datetime="([^"]+)"')
MESSAGE_TEXT_RE = re.compile(
    r'<div class="tgme_widget_message_text[^>]*>(.*?)</div>',
    re.DOTALL,
)
INLINE_STATUS_RE = re.compile(
    r'class="tgme_widget_message_inline_button url_button"\s+'
    r'href="https://(?:www\.)?(?:twitter\.com|x\.com)/elonmusk/status/(\d+)',
    re.IGNORECASE,
)
ANY_STATUS_RE = re.compile(
    r'href="https://(?:www\.)?(?:twitter\.com|x\.com)/elonmusk/status/(\d+)',
    re.IGNORECASE,
)
BRACKET_LABEL_RE = re.compile(
    r"\[(Tweet|Reply|ReTweet|Retweet|Elon Reply|Quote)\]",
    re.IGNORECASE,
)
LEGACY_LABEL_RE = re.compile(r"\b(TWEET|REPLY|RETWEET):", re.IGNORECASE)
LEADING_TIME_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?(?::)?\s*")
HREF_RE = re.compile(r'href="([^"]+)"')
TELEGRAM_USERNAME_RE = re.compile(
    r'href="https://t\.me/([A-Za-z0-9_]+)"[^>]*>.*?@([A-Za-z0-9_]+)',
    re.IGNORECASE | re.DOTALL,
)
MENTION_RE = re.compile(r"@([A-Za-z0-9_]+)")

TG_RAW_FIELDS = (
    "id",
    "created_at_utc",
    "telegram_message_id",
    "telegram_datetime",
    "alert_delay_seconds",
    "label",
    "reply_to_username",
    "alert_text",
    "rendered_text",
    "telegram_url",
    "x_url",
    "source_links_json",
    "included_in_15m",
    "filter_reason",
)

os.makedirs(DOWNLOAD_DIR_15_TG, exist_ok=True)


class TgFetchError(RuntimeError):
    """Raised when the Telegram fallback cannot produce a trustworthy result."""


class TelegramMessage(TypedDict):
    """Structured fields exposed by one public Telegram alert message."""

    message_id: int
    telegram_datetime: str
    label: str
    reply_to_username: str
    alert_text: str
    rendered_text: str
    status_ids: list[str]
    links: list[str]


def _validate_post_count(n: int) -> int:
    if isinstance(n, bool) or not isinstance(n, int):
        raise ValueError("n must be an integer")
    if n < 1:
        raise ValueError("n must be at least 1")
    return n


def _build_session() -> requests.Session:
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def _html_to_text(value: str) -> str:
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"</?(?:p|div|blockquote)[^>]*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", "", value)
    return "\n".join(
        line
        for raw_line in html.unescape(value).splitlines()
        if (line := re.sub(r"[ \t]+", " ", raw_line).strip())
    )


def _message_chunks(page_html: str) -> Iterable[str]:
    starts = list(WRAP_RE.finditer(page_html))
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(page_html)
        yield page_html[start.start() : end]


def _normalize_label(value: str) -> str:
    return {
        "tweet": "Tweet",
        "reply": "Reply",
        "elon reply": "Reply",
        "retweet": "ReTweet",
        "quote": "Quote",
    }.get(value.casefold(), "")


def _parse_message(chunk: str) -> TelegramMessage | None:
    identity = MESSAGE_ID_RE.search(chunk)
    if not identity or identity.group("channel").casefold() != TG_CHANNEL.casefold():
        return None

    text_match = MESSAGE_TEXT_RE.search(chunk)
    body_html = text_match.group(1) if text_match else ""
    rendered_text = _html_to_text(body_html)
    bracket_label = BRACKET_LABEL_RE.search(rendered_text)
    legacy_label = LEGACY_LABEL_RE.search(rendered_text)
    raw_label = (
        bracket_label.group(1)
        if bracket_label
        else legacy_label.group(1)
        if legacy_label
        else ""
    )
    label = _normalize_label(raw_label)
    alert_text = LEADING_TIME_RE.sub("", rendered_text, count=1)
    alert_text = BRACKET_LABEL_RE.sub("", alert_text, count=1)
    alert_text = LEGACY_LABEL_RE.sub("", alert_text, count=1).strip()
    reply_to_username = ""
    if label == "Reply":
        for target in TELEGRAM_USERNAME_RE.finditer(body_html):
            linked, visible = target.groups()
            if linked.casefold() != TG_CHANNEL.casefold():
                reply_to_username = visible
                break
        if not reply_to_username and (target := MENTION_RE.search(alert_text)):
            reply_to_username = target.group(1)

    inline_ids = sorted(set(INLINE_STATUS_RE.findall(chunk)), key=int)
    status_ids = inline_ids or sorted(set(ANY_STATUS_RE.findall(chunk)), key=int)
    timestamp = DATETIME_RE.search(chunk)
    return {
        "message_id": int(identity.group("id")),
        "telegram_datetime": timestamp.group(1) if timestamp else "",
        "label": label,
        "reply_to_username": reply_to_username,
        "alert_text": alert_text,
        "rendered_text": rendered_text,
        "status_ids": status_ids,
        "links": sorted(set(html.unescape(link) for link in HREF_RE.findall(chunk))),
    }


def _parse_page(page: bytes) -> list[TelegramMessage]:
    decoded = page.decode(ENCODING, errors="replace")
    return [
        message
        for chunk in _message_chunks(decoded)
        if (message := _parse_message(chunk)) is not None
    ]


def _request_page(
    session: requests.Session,
    before: int | None,
) -> list[TelegramMessage]:
    params = {"before": str(before)} if before is not None else None
    try:
        response = session.get(
            TG_PUBLIC_URL,
            params=params,
            headers={"User-Agent": TG_USER_AGENT, "Accept": "text/html"},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise TgFetchError(f"Telegram request failed: {exc}") from exc

    messages = _parse_page(response.content)
    if not messages and before is None:
        raise TgFetchError(f"Telegram returned no visible messages for @{TG_CHANNEL}")
    return messages


def _snowflake_timestamp(post_id: str) -> datetime | None:
    if not post_id.isdigit():
        return None
    try:
        timestamp_ms = (int(post_id) >> 22) + TWITTER_EPOCH_MS
        return datetime.fromtimestamp(timestamp_ms / 1_000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _telegram_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _write_raw_tg_posts(records: list[dict[str, str]]) -> None:
    """Persist all Telegram-available post data and metadata as CSV."""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=TG_RAW_FIELDS)
    writer.writeheader()
    writer.writerows(records)
    save_tweets_to_csv(output.getvalue().encode(ENCODING), TG_RAW_OUTPUT_PATH)


def _post_filter(message: TelegramMessage) -> tuple[bool, str]:
    """Return whether a Telegram-linked post belongs in the 15-minute data."""
    if message["label"] in NON_REPLY_LABELS:
        return True, "non_reply"
    if message["label"] != "Reply":
        return False, "unlabeled"

    target = message["reply_to_username"].casefold()
    if not target or target in SELF_REPLY_USERNAMES:
        # @elonalert omits the target on self-replies and names external targets.
        return True, "self_reply"
    return False, "reply_to_other"


def _raw_tg_record(
    post_id: str,
    timestamp: datetime,
    message: TelegramMessage,
    filter_reason: str,
) -> dict[str, str]:
    telegram_time = _telegram_datetime(message["telegram_datetime"])
    return {
        "id": post_id,
        "created_at_utc": timestamp.isoformat(timespec="milliseconds").replace(
            "+00:00",
            "Z",
        ),
        "telegram_message_id": str(message["message_id"]),
        "telegram_datetime": message["telegram_datetime"],
        "alert_delay_seconds": (
            f"{(telegram_time - timestamp).total_seconds():.3f}"
            if telegram_time
            else ""
        ),
        "label": message["label"],
        "reply_to_username": message["reply_to_username"],
        "alert_text": message["alert_text"],
        "rendered_text": message["rendered_text"],
        "telegram_url": f"https://t.me/{TG_CHANNEL}/{message['message_id']}",
        "x_url": f"https://x.com/elonmusk/status/{post_id}",
        "source_links_json": json.dumps(
            message["links"],
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "included_in_15m": "false",
        "filter_reason": filter_reason,
    }


def fetch_latest_non_reply_posts(
    n: int = DEFAULT_POST_COUNT,
    *,
    session: requests.Session | None = None,
) -> list[tuple[str, datetime]]:
    """Fetch exactly ``n`` total linked posts, then return the eligible subset."""
    target = _validate_post_count(n)
    active_session = session or _build_session()
    owns_session = session is None
    before: int | None = None
    seen_before: set[int] = set()
    seen_ids: set[str] = set()
    eligible_posts: list[tuple[str, datetime]] = []
    raw_records: list[dict[str, str]] = []

    try:
        while len(raw_records) < target:
            messages = _request_page(active_session, before)
            if not messages:
                break

            for message in sorted(
                messages,
                key=lambda item: item["message_id"],
                reverse=True,
            ):
                is_eligible, filter_reason = _post_filter(message)
                for post_id in message["status_ids"]:
                    if post_id in seen_ids:
                        continue
                    timestamp = _snowflake_timestamp(post_id)
                    if timestamp is None:
                        logger.warning("Skipping Telegram-linked post with invalid ID %s", post_id)
                        continue
                    seen_ids.add(post_id)
                    raw_records.append(
                        _raw_tg_record(
                            post_id,
                            timestamp,
                            message,
                            filter_reason,
                        ),
                    )
                    if is_eligible:
                        eligible_posts.append((post_id, timestamp))
                    if len(raw_records) == target:
                        break
                if len(raw_records) == target:
                    break

            if len(raw_records) == target:
                break
            next_before = min(message["message_id"] for message in messages)
            if next_before in seen_before or next_before <= 1:
                break
            seen_before.add(next_before)
            before = next_before
    finally:
        if owns_session:
            active_session.close()

    eligible_posts.sort(key=lambda item: (item[1], int(item[0])), reverse=True)
    posts = eligible_posts
    selected_ids = {post_id for post_id, _ in eligible_posts}
    for record in raw_records:
        if record["id"] in selected_ids:
            record["included_in_15m"] = "true"
    raw_records.sort(key=lambda item: int(item["id"]), reverse=True)
    _write_raw_tg_posts(raw_records)
    logger.info("Saved %d structured Telegram posts to %s", len(raw_records), TG_RAW_OUTPUT_PATH)
    if len(raw_records) < target:
        logger.warning(
            "Telegram @%s returned %d of %d requested total posts",
            TG_CHANNEL,
            len(raw_records),
            target,
        )
    return posts


def _posts_to_timestamp_csv(posts: list[tuple[str, datetime]]) -> bytes:
    frame = pd.DataFrame(
        {
            "id": [post_id for post_id, _ in posts],
            "timestamp": [timestamp.isoformat() for _, timestamp in posts],
        },
    )
    return frame.to_csv(index=False).encode(ENCODING)


def fetch_tg_posts_by_15min(
    n: int = DEFAULT_POST_COUNT,
    utc: bool = False,
) -> str:
    """Save ``n`` total posts, filter replies, and refresh both aggregates."""
    posts = fetch_latest_non_reply_posts(n)
    timestamp_csv = _posts_to_timestamp_csv(posts)
    et_bytes, utc_bytes = process_by_15min_pair(
        timestamp_csv,
        output_prefix=TG_OUTPUT_PREFIX,
        output_prefix_utc=TG_OUTPUT_PREFIX_UTC,
    )
    return (utc_bytes if utc else et_bytes).decode(ENCODING)
