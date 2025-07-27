from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import StreamingResponse

from src.download import get_avg_per_day, get_data_timespan, get_first_tweet_date, get_time_now, get_total_tweets, \
    get_tweets_by_date, \
    get_tweets_by_hour

mcp = FastMCP(
    name="xtracker-mcp",
    instructions=(
        "This MCP server exposes a single tool `fetch` that returns the "
        "plain‑text response from the XTracker API for handle `elonmusk` "
        "on platform `X`."
    )
)


@mcp.tool()
def tweet_by_hour():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return normalized tweets by hour.
    """
    return get_tweets_by_hour()
    # return StreamingResponse(download(), media_type="text/event-stream")


@mcp.tool()
def tweet_by_date():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return tweets by date.
    """
    return get_tweets_by_date()
    # return StreamingResponse(download(), media_type="text/event-stream")


@mcp.tool()
def total_tweets():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return total number of tweets.
    """
    return get_total_tweets()


@mcp.tool()
def avg_per_day():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return average tweets per day.
    """
    return get_avg_per_day()


@mcp.tool()
def first_tweet_date():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return the date of the first tweet.
    """
    return get_first_tweet_date()


@mcp.tool()
def time_now():
    """
    return the current time.
    """
    return get_time_now()


@mcp.tool()
def data_timespan():
    """
    return the timespan of the data.
    """
    return get_data_timespan()


app = mcp.streamable_http_app()  # MCP routes live at /mcp/


def bump(request: Request) -> StreamingResponse:
    return StreamingResponse("ok!", media_type="text/event-stream")


def hour(request: Request):
    return StreamingResponse(get_tweets_by_hour(), media_type="text/event-stream")


def date(request: Request):
    return StreamingResponse(get_tweets_by_date(), media_type="text/event-stream")


def total(request: Request):
    return StreamingResponse(get_total_tweets().to_bytes().decode('utf-8'), media_type="text/event-stream")


def avg_per_day(request: Request):
    return StreamingResponse((str(get_avg_per_day())).encode('utf-8').decode('utf-8'), media_type="text/event-stream")


def first_tweet_date(request: Request):
    return StreamingResponse(
        (str(get_first_tweet_date())).encode('utf-8').decode('utf-8'), media_type="text/event-stream")


def time_now(request: Request):
    return StreamingResponse((str(get_time_now())).encode('utf-8').decode('utf-8'), media_type="text/event-stream")


def data_timespan(request: Request):
    return StreamingResponse((str(get_data_timespan())).encode('utf-8').decode('utf-8'), media_type="text/event-stream")


# Starlette route registration
app.add_route("/", bump, methods=["GET", "POST"])
app.add_route("/hour", hour, methods=["GET"])
app.add_route("/date", date, methods=["GET"])
app.add_route("/total", total, methods=["GET"])
app.add_route("/avg_per_day", avg_per_day, methods=["GET"])
app.add_route("/first_tweet_date", first_tweet_date, methods=["GET"])
app.add_route("/time_now", time_now, methods=["GET"])
app.add_route("/data_timespan", data_timespan, methods=["GET"])
