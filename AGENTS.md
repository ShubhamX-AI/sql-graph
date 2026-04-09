# Repository Guidelines

## Project Structure & Module Organization
This repository is a Python 3.12 NL2SQL pipeline with a small top-level entrypoint layer and modular code under `src/`.

- `run_pipeline.py`: one-time setup flow (extract schema, enrich with OpenAI, store in Neo4j).
- `query.py`: query-time CLI for natural language to SQL and result summarization.
- `src/core/`: shared config and OpenAI client helpers.
- `src/db/`: MySQL connection logic.
- `src/pipeline/`: extraction, enrichment, and relationship discovery stages.
- `src/graph/`: Neo4j storage and retrieval.
- `.env.example`: required environment variables template.

## Build, Test, and Development Commands
Use `uv` for dependency and runtime management.

- `uv sync`: install/lock dependencies into the local environment.
- `cp .env.example .env`: create local config file, then fill credentials.
- `uv run python run_pipeline.py`: run the full schema indexing pipeline.
- `uv run python query.py "Show me top 10 customers by revenue"`: run a query end-to-end.

## Coding Style & Naming Conventions
Follow the current project style:

- 4-space indentation, snake_case for functions/variables/modules, PascalCase for classes.
- Prefer type hints (`list[str]`, `set[str]`) on public/internal function signatures.
- Keep modules focused by responsibility (`src/pipeline/*` for pipeline stages, `src/graph/*` for Neo4j).
- Use concise docstrings/comments only where behavior is not obvious.
- The main coding agents are software engineers, so write code accordingly.
- Senior software engineers prefer simpler, more readable code over complex solutions.
- Move implementation toward simplicity, not complexity.
- Fix the exact thing you were asked to fix, not other unrelated things.

## Testing Guidelines
There is currently no automated `tests/` suite configured. Validate changes with targeted smoke tests:

- Pipeline smoke test: `uv run python run_pipeline.py`
- Query smoke test: `uv run python query.py "How many active projects per city?"`

When adding tests, place them under `tests/` and use `test_<module>.py` naming.

## Commit & Pull Request Guidelines
Current history uses short imperative commit subjects (for example, `Initial commit`, `Update files structure`).

- Keep commit titles brief, imperative, and scoped to one change.
- In PRs, include: purpose, affected modules, config changes, and manual verification steps.
- Link related issues when applicable and include sample query/output snippets for behavior changes.

## Security & Configuration Tips
- Never commit `.env` or raw credentials.
- Use read-only MySQL credentials where possible.
- Treat `pipeline_progress.json` as runtime state; remove only when intentionally rebuilding.
