from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import StreamingResponse

from src.download import get_avg_per_day, get_data_range, get_first_tweet_date, get_time_now, \
    get_total_tweets, \
    get_tweets_by_date, \
    get_tweets_by_hour, get_tweets_by_weekday

mcp = FastMCP(
    name="xtracker-mcp",
    instructions=(
        "This MCP server exposes a single tool `fetch` that returns the "
        "plain‑text response from the XTracker API for handle `elonmusk` "
        "on platform `X`."
    )
)


@mcp.tool()
def tweets_by_hour_grouped():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return normalized tweets by hour.
    """
    return get_tweets_by_hour()


@mcp.tool()
def tweets_by_date_grouped():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return tweets by date.
    """
    return get_tweets_by_date()


@mcp.tool()
def tweets_by_weekday_grouped():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return tweets by weekday.
    """
    return get_tweets_by_weekday()


@mcp.tool()
def total_tweet_count():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return total number of tweets.
    """
    return get_total_tweets()


@mcp.tool()
def avg_tweets_per_day():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return average tweets per day.
    """
    return get_avg_per_day()


@mcp.tool()
def iso_first_tweet_date():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return the date of the first tweet.
    """
    return get_first_tweet_date()


@mcp.tool()
def iso_time_now():
    """
    return the current time.
    """
    return get_time_now()


@mcp.tool()
def data_timespan():
    """
    return the span over which the data was collected.
    """
    return get_data_range()


app = mcp.streamable_http_app()  # MCP routes live at /mcp/


def bump(request: Request) -> StreamingResponse:
    return StreamingResponse("ok!", media_type="text/event-stream")


def hour(request: Request):
    return StreamingResponse(get_tweets_by_hour(), media_type="text/event-stream")


def date(request: Request):
    return StreamingResponse(get_tweets_by_date(), media_type="text/event-stream")


def weekday(request: Request):
    return StreamingResponse(get_tweets_by_weekday(), media_type="text/event-stream")


def total(request: Request):
    return StreamingResponse(str(get_total_tweets()), media_type="text/event-stream")


def avg_day(request: Request):
    return StreamingResponse((str(get_avg_per_day())), media_type="text/event-stream")


def iso_first_tweet(request: Request):
    return StreamingResponse(
        (str(get_first_tweet_date())), media_type="text/event-stream")


def now(request: Request):
    return StreamingResponse((str(get_time_now())), media_type="text/event-stream")


def data_span(request: Request):
    return StreamingResponse((str(get_data_range())), media_type="text/event-stream")


# Starlette route registration
app.add_route("/", bump, methods=["GET", "POST"])
app.add_route("/hour", hour, methods=["GET"])
app.add_route("/date", date, methods=["GET"])
app.add_route("/weekday", weekday, methods=["GET"])
app.add_route("/total", total, methods=["GET"])
app.add_route("/avg_per_day", avg_day, methods=["GET"])
app.add_route("/first_tweet_date", iso_first_tweet, methods=["GET"])
app.add_route("/time_now", now, methods=["GET"])
app.add_route("/data_span", data_span, methods=["GET"])
