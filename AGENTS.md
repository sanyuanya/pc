# Repository Guidelines

## Project Structure & Module Organization
- `main.py` hosts the executable entrypoint; keep core business logic here or in purpose-specific modules under new packages inside the repo root (for example, `pc/core/__init__.py`).
- `pyproject.toml` defines the package metadata and runtime requirements; update its dependency lists whenever you add imports outside the standard library.
- Tests should mirror the layout of the source files inside `tests/` (create the directory if absent), e.g., `tests/test_main.py` for features that originate in `main.py`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate` — create and activate an isolated environment.
- `pip install -e .` — install the package in editable mode so local changes are importable.
- `python main.py` — run the CLI entrypoint and confirm new logic behaves as expected.
- `python -m pytest -q` — execute the automated test suite from `tests/` with concise output.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation, `snake_case` for functions/modules, `PascalCase` for classes, and `UPPER_SNAKE_CASE` for constants.
- Annotate public functions with type hints and include short doctrings summarizing side effects.
- Before pushing, format code with `python -m black main.py tests/` and perform static checks with `ruff check .` (install `black`/`ruff` as dev dependencies if you have not already).

## Testing Guidelines
- Write unit tests with `pytest`; name files `test_<module>.py` and functions `test_<behavior>()` to make intent obvious.
- Prefer arranging tests using Arrange-Act-Assert comments so failures are actionable.
- When adding new modules, include at least one test that hits both the success path and a representative error path.
- Share setup via fixtures in `conftest.py` and stub external resources to keep runs deterministic.

## Commit & Pull Request Guidelines
- The history is currently empty; start with Conventional Commits (e.g., `feat: add cli parsing skeleton`, `fix: guard empty payload`) to make changelog generation trivial.
- Each commit should focus on one change set: update code, tests, and docs together and include why the change matters in the body when non-obvious.
- Pull requests need a short summary, testing notes (commands executed and their results), and references to any tracked issues; attach screenshots or logs if the change affects output.
