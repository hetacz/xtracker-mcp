from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import StreamingResponse

from src.download import get_tweets_by_date, get_tweets_by_hour

mcp = FastMCP(
    name="xtracker-mcp",
    instructions=(
        "This MCP server exposes a single tool `fetch` that returns the "
        "plain‑text response from the XTracker API for handle `elonmusk` "
        "on platform `X`."
    )
)


@mcp.tool()
def tweet_distribution_by_hour():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return response text.
    """
    return get_tweets_by_hour()
    # return StreamingResponse(download(), media_type="text/event-stream")


@mcp.tool()
def tweet_distribution_by_date():
    """
    POST fixed payload {handle: elonmusk, platform: X} → return response text.
    """
    return get_tweets_by_date()
    # return StreamingResponse(download(), media_type="text/event-stream")


app = mcp.streamable_http_app()  # MCP routes live at /mcp/


async def bump(request: Request) -> StreamingResponse:
    return StreamingResponse("ok!", media_type="text/event-stream")


def hour(request: Request):
    return StreamingResponse(get_tweets_by_hour(), media_type="text/event-stream")


def date(request: Request):
    return StreamingResponse(get_tweets_by_date(), media_type="text/event-stream")


# Starlette route registration
app.add_route("/", bump, methods=["GET", "POST"])
app.add_route("/hour", hour, methods=["GET"])
app.add_route("/date", date, methods=["GET"])
