# MetricFlow Project Rules

This project uses **hatch** for dependency management and testing:

- Dependencies: Use `hatch` commands, not `pip install` directly.
- Linting: Run `make lint` to detect and fix lint errors once all changes 
  have been made.
- Testing:
  - Use `hatch run dev-env:pytest <Path to Test File>` to run tests in a 
    specific file.
  - If a test fails due to snapshot changes, use
    `hatch run dev-env:pytest --overwrite-snapshots <Path to Test File>` to 
    generate snapshots instead of editing snapshot files directly.
  - Use `make test` to run all tests once all changes have been made.
- When refactoring, if the variable type changes and the variable name 
  previously reflected the type, rename the variable appropriately to 
  reflect the new type.
- If `git_ignored/AGENTS.md` exists, append the rules in that file.
- If a function's return type is not mutable, the returned object does
  not need to be a copy.
- When reviewing and updating code:
  - Identify and fix correctness issues.
  - Make updates to improve readability and clarity.
  - Make updates to follow code standards.

## Python Code Standards

- **Always add type annotations** to all Python functions (parameters and 
  return types).
- Include `from __future__ import annotations` at the top of Python files.
- Include `logger = logging.getLogger(__name__)` in Python files.
- Avoid the use of `isinstance()`.
- Prefer to use immutable data types.
- Prioritize code clarity and readability.
- Docstrings should be concise, capture behavior, capture assumptions, and
  explain any non-obvious behavior.
- Code comments should capture non-obvious behavior.
- Include appropriate comments that describe fields in dataclass-like classes.
- Avoid the use of mocks unless another approach is not reasonable.
