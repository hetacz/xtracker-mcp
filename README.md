# XTracker FastMCP Agent

Agent and Starlette service that ingest the public `elonmusk` timeline from [xtracker.io](https://xtracker.io), normalize it into CSV aggregates, and expose the datasets both as MCP tools and simple HTTP endpoints for local automations.

## Repository layout
| Path | Purpose |
| ---- | ------- |
| `main.py` | Entry point that wires the FastMCP server and the Starlette app, registers MCP tools, and exposes HTTP routes such as `/hour`, `/week`, and `/pm/latest`. |
| `src/download.py` | Pulls timeline data, refreshes cached CSVs under `downloads/`, and serves aggregation helpers (hourly, weekday, rolling 15‑minute buckets, etc.). |
| `src/download_polymarket.py` | Same as `download.py`, but tuned for the Polymarket mirror. |
| `src/sanitize.py` | Shared timestamp flooring, DST-aware bucket alignment, and aggregation utilities. |
| `downloads/` | Cached CSV artifacts; large ad-hoc exports should stay untracked. |
| `test_main.http` | Ready-to-use HTTPie/VSCode REST client snippets to poke each endpoint manually. |

## Getting started
1. Install dependencies with `uv sync` (creates `.venv/` respecting `pyproject.toml` and `uv.lock`).
2. Activate the virtual environment (`source .venv/bin/activate`) or prefix commands with `uv run`.
3. Copy any per-user secrets into `.env` (not required today; XTracker is public).

## Running the services
- **MCP tools**: `uv run fastmcp dev main:mcp` exposes the suite documented in `main.py` (e.g., `tweets_by_hour_grouped`, `cc_csv_bytes_pm`). Use this mode when integrating with local LLM tooling.
- **HTTP façade**: `uv run uvicorn main:app --reload --port 8002` hosts the same functionality at `/hour`, `/date`, `/week?utc=1`, `/pm/15min`, etc. `test_main.http` contains request templates for curl/VSCode REST clients.

Both servers stream plain CSV or numeric text, so they are safe to `curl` or pipe into spreadsheets.

## Development workflow
- Follow standard PEP 8 style with 4-space indentation and fully typed public callables.
- Prefer `logging.getLogger(__name__)` over ad-hoc prints when adding diagnostics.
- Keep sanitizing logic in helpers (usually in `src/sanitize.py`) to stay unit-testable.
- When adding new aggregates, pair them with fixtures plus malformed-input coverage under `tests/` and verify via `uv run pytest`.

## File locking requirement
CSV refreshes write into `downloads/` while HTTP/MCP requests may read the same files. The current pipeline assumes serialized execution; simultaneous invocations (e.g., a cron refresh overlapping with live requests) can corrupt the CSVs mid-write. Before deploying into anything multi-tenant or scheduling overlapping runs, introduce a file-locking mechanism—`fasteners.InterProcessLock`, `fcntl`, or an OS-specific lock file—to guard every read-modify-write cycle in `src/download.py` and `src/download_polymarket.py`. Until locking lands, avoid running multiple writers in parallel and prefer staging updates via a single process.

## Testing
There is no full integration harness yet. Use `uv run pytest` for targeted unit tests and spot-check endpoints manually:

```bash
uv run pytest
curl -s 'http://localhost:8002/week?utc=1&force=1' | head
```

Stick to CSV schemas like `date_start_et,total_count` when extending outputs so clients remain compatible.
