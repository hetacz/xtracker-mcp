import logging
from typing import Any, Callable

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import StreamingResponse

from src.download import (
    get_avg_per_day,
    get_data_range,
    get_first_tweet_date,
    get_time_now,
    get_total_tweets,
    get_tweets_by_date,
    get_tweets_by_hour,
    get_tweets_by_week,
    get_tweets_by_weekday,
)

mcp = FastMCP(
    name="xtracker-mcp",
    instructions=(
        "This MCP server exposes tools that fetch and aggregate public tweet data "
        "for handle `elonmusk` on platform `X` via the XTracker API."
    ),
)


# ---------- MCP tools ----------
@mcp.tool()
def tweets_by_hour_grouped() -> str:
    """Return normalized tweet counts grouped by hour (ET) as CSV text."""
    return get_tweets_by_hour()


@mcp.tool()
def tweets_by_date_grouped() -> str:
    """Return tweet counts grouped by date (ET) as CSV text."""
    return get_tweets_by_date()


@mcp.tool()
def tweets_by_weekday_grouped() -> str:
    """Return tweet counts grouped by weekday (ET) as CSV text."""
    return get_tweets_by_weekday()


@mcp.tool()
def tweets_by_week_grouped() -> str:
    """Return tweet counts grouped by week (starts on Friday 12:00 ET) as CSV text."""
    return get_tweets_by_week()


@mcp.tool()
def total_tweet_count() -> int:
    """Return the total number of tweets."""
    return get_total_tweets()


@mcp.tool()
def avg_tweets_per_day() -> float:
    """Return the average tweets per day."""
    return get_avg_per_day()


@mcp.tool()
def iso_first_tweet_date() -> str:
    """Return the ISO timestamp of the first tweet (ET)."""
    return get_first_tweet_date()


@mcp.tool()
def iso_time_now() -> str:
    """Return the current ET ISO timestamp."""
    return get_time_now()


@mcp.tool()
def data_timespan() -> int:
    """Return the elapsed seconds between the first tweet and now (ET)."""
    return get_data_range()


# ---------- HTTP app and routes ----------
app = mcp.streamable_http_app()  # MCP routes live at /mcp/


def _make_stream_handler(func: Callable[[], Any]) -> Callable[[Request], StreamingResponse]:
    """
    Wrap a zero-arg callable into a Starlette route handler returning StreamingResponse.
    Ensures consistent media type and basic error handling without changing payload format.
    """

    def handler(request: Request) -> StreamingResponse:
        try:
            result = func()
            body = result if isinstance(result, (str, bytes)) else str(result)
            return StreamingResponse(body, media_type="text/event-stream")
        except Exception as exc:
            logging.getLogger(__name__).exception(
                "Unhandled error in handler for %s", getattr(func, "__name__", str(func)))
            return StreamingResponse(f"error: {exc}", status_code=500, media_type="text/event-stream")

    return handler


bump = _make_stream_handler(lambda: "ok!")
hour = _make_stream_handler(get_tweets_by_hour)
date = _make_stream_handler(get_tweets_by_date)
weekday = _make_stream_handler(get_tweets_by_weekday)
week = _make_stream_handler(get_tweets_by_week)
total = _make_stream_handler(get_total_tweets)
avg_day = _make_stream_handler(get_avg_per_day)
iso_first_tweet = _make_stream_handler(get_first_tweet_date)
now = _make_stream_handler(get_time_now)
data_span = _make_stream_handler(get_data_range)

# Starlette route registration
app.add_route("/", bump, methods=["GET", "POST"])  # healthcheck
app.add_route("/hour", hour, methods=["GET"])  # CSV
app.add_route("/date", date, methods=["GET"])  # CSV
app.add_route("/weekday", weekday, methods=["GET"])  # CSV
app.add_route("/week", week, methods=["GET"])  # CSV
app.add_route("/total", total, methods=["GET"])  # integer as text
app.add_route("/avg_per_day", avg_day, methods=["GET"])  # float as text
app.add_route("/first_tweet_date", iso_first_tweet, methods=["GET"])  # ISO string
app.add_route("/time_now", now, methods=["GET"])  # ISO string
app.add_route("/data_span", data_span, methods=["GET"])  # int seconds as text
