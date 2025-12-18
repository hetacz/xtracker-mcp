# Repository Guidelines

## Project Structure & Module Organization
Core entry point `main.py` wires the FastMCP agent and Starlette app. The `src/` package holds the data pipeline: `download.py` manages the XTracker fetch and cache refresh, while `sanitize.py` normalizes timestamps and rolls up aggregations. Generated CSV artifacts live in `downloads/`; keep large, ad-hoc exports out of version control. Use `test_main.http` for quick manual endpoint checks during development.

## Build, Test, and Development Commands
Run `uv sync` from the repository root to install or update dependencies declared in `pyproject.toml` and `uv.lock`. Activate the created `.venv` (or use `uv run`) and execute tests with `uv run pytest` (equivalently `.venv/bin/pytest`). Launch the MCP tool server with `uv run fastmcp dev main:mcp` to expose tools over stdio. For the HTTP façade that powers manual checks, use `uv run uvicorn main:app --reload --port 8002` and query paths such as `/hour` or `/total`.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation, `snake_case` for functions and module-level constants in `UPPER_SNAKE_CASE`. Keep public callables typed; match existing signatures that return CSV text or numeric aggregates. Reuse `logging.getLogger(__name__)` for diagnostics instead of print statements, and prefer small helpers over in-line procedural code so sanitizer and aggregation logic stay testable.

## Testing Guidelines
There is no automated test harness yet; prioritize adding focused `pytest` cases under `tests/` when you extend the pipeline. Each new aggregation should include both a happy-path fixture and malformed-input coverage. Until then, exercise endpoints via `uv run fastmcp dev main:mcp` or the bundled `test_main.http`, confirming CSV schemas (`date_start_et,total_count`, etc.) before merging.

## Commit & Pull Request Guidelines
Commits mirror an imperative, succinct style (e.g., `process_by_15min fixed to properly display sunday`). Limit scope per commit and describe the observable change, not the implementation detail. Pull requests should outline the motivation, list touched endpoints or CSV outputs, and note any schema or caching side effects. Link back to the originating issue or discussion and attach sample command outputs when behavior changes.

## Data Handling & Security Notes
Do not hard-code credentials; the XTracker endpoint currently requires none. Scrub personal tokens before sharing logs. Cached CSVs in `downloads/` may include large datasets—rotate them with caution and avoid committing files exceeding practical review sizes.
