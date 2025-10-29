# MetricFlow Project Rules

This project uses **hatch** for dependency management and testing:

- Dependencies: Use `hatch` commands, not `pip install` directly
- Tests: Use `hatch run dev-env:pytest <path>` instead of `pytest` directly
- Snapshots: Add `--overwrite-snapshots` for new snapshot tests

After making a set of changes:

- Run `make lint` to detect and fix lint errors.
- Run `make test` to run all tests.

## Python Code Standards

- **Always add type annotations** to all Python functions (parameters and return types)
- Use `from __future__ import annotations` at the top of Python files
