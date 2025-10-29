# MetricFlow Project Rules

This project uses **hatch** for dependency management and testing:

- Dependencies: Use `hatch` commands, not `pip install` directly.
- Linting: Run `make lint` to detect and fix lint errors.
- Testing:
  - Use `hatch run dev-env:pytest <path>` to run tests in a specific file.
  - Use `make test` to run all tests.
- Snapshots: When adding new tests that use snapshots, add the `--overwrite-snapshots`
  flag to the above `pytest` command to generate snapshots.

If `git_ignored/AGENTS.md` exists, append the rules in that file.

## Python Code Standards

- **Always add type annotations** to all Python functions (parameters and return types)
- Use `from __future__ import annotations` at the top of Python files
