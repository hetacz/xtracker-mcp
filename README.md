# XTracker FastMCP Agent

Agent and Starlette service that ingest the public `elonmusk` timeline from [xtracker.io](https://xtracker.io), normalize it into CSV aggregates, and expose the datasets both as MCP tools and simple HTTP endpoints for local automations.

## Repository layout
| Path | Purpose |
| ---- | ------- |
| `main.py` | Entry point that wires the FastMCP server and the Starlette app, registers MCP tools, and exposes HTTP routes such as `/hour`, `/week`, and `/pm/latest`. |
| `src/download.py` | Pulls timeline data, refreshes cached CSVs under `downloads/`, and serves aggregation helpers (hourly, weekday, rolling 15‑minute buckets, etc.). |
| `src/download_polymarket.py` | Same as `download.py`, but tuned for the Polymarket mirror. |
| `src/download_tg.py` | Fetches a fixed total of recent posts from public `@elonalert`, filters external replies, and writes raw plus paired ET/UTC 15-minute CSVs. |
| `src/sanitize.py` | Shared timestamp flooring, DST-aware bucket alignment, and aggregation utilities. |
| `downloads/` | Cached CSV artifacts; large ad-hoc exports should stay untracked. |
| `test_main.http` | Ready-to-use HTTPie/VSCode REST client snippets to poke each endpoint manually. |

## Getting started
1. Install dependencies with `uv sync` (creates `.venv/` respecting `pyproject.toml` and `uv.lock`).
2. Activate the virtual environment (`source .venv/bin/activate`) or prefix commands with `uv run`.
3. No credentials are required for the XTracker, Polymarket, or public Telegram paths.

## Running the services
- **MCP tools**: `uv run fastmcp dev main:mcp` exposes the suite documented in `main.py` (e.g., `tweets_by_hour_grouped`, `cc_csv_bytes_pm`). Use this mode when integrating with local LLM tooling.
- **HTTP façade**: `uv run uvicorn main:app --reload --port 8002` hosts the same functionality at `/hour`, `/date`, `/week?utc=1`, `/pm/15min`, and `/tg/fetch?n=1000&utc=false`. `test_main.http` contains request templates for curl/VSCode REST clients.

Both servers stream plain CSV or numeric text, so they are safe to `curl` or pipe into spreadsheets.

The TG fallback saves exactly `n` total Telegram-linked posts, including replies, and all available source metadata to `downloads/15m/tg/raw_elonmusk_tg.csv` (`n=1000` when omitted). It then excludes replies to other users while retaining self-replies before refreshing `downloads/15m/tg/elonmusk_tg.csv` and `downloads/15m/tg/elonmusk_tg_utc.csv`, so the aggregates normally contain fewer than `n` posts. In `@elonalert`, a blank reply target denotes a self-reply; an explicit `elonmusk` target is also retained. The raw file records `included_in_15m` and `filter_reason` for auditability alongside tweet text/type, timestamps, alert delay, source links, and canonical Telegram/X URLs. HTML response bodies are not retained. The `utc` flag only selects which aggregate the HTTP/MCP call returns.

See [Telegram fallback pipeline](docs/TELEGRAM_FALLBACK.md) for the complete source, filtering, bucketing, schema, and comparison contract.

## Development workflow
- Follow standard PEP 8 style with 4-space indentation and fully typed public callables.
- Prefer `logging.getLogger(__name__)` over ad-hoc prints when adding diagnostics.
- Keep sanitizing logic in helpers (usually in `src/sanitize.py`) to stay unit-testable.
- When adding new aggregates, pair them with fixtures plus malformed-input coverage under `tests/` and verify via `uv run pytest`.

## File locking requirement
CSV refreshes write into `downloads/` while HTTP/MCP requests may read the same files. The current pipeline assumes serialized execution; simultaneous invocations (e.g., a cron refresh overlapping with live requests) can corrupt the CSVs mid-write. Before deploying into anything multi-tenant or scheduling overlapping runs, introduce a file-locking mechanism—`fasteners.InterProcessLock`, `fcntl`, or an OS-specific lock file—to guard every read-modify-write cycle in `src/download.py` and `src/download_polymarket.py`. Until locking lands, avoid running multiple writers in parallel and prefer staging updates via a single process.

## Testing
`uv run pytest` runs the offline unit and local-database suites. Live Polymarket
contract checks are excluded by default; run them explicitly with `-m live`:

```bash
uv run pytest
uv run pytest -m live tests/test_polymarket_endpoint.py
curl -s 'http://localhost:8002/week?utc=1&force=1' | head
```

Stick to CSV schemas like `date_start_et,total_count` when extending outputs so clients remain compatible.
