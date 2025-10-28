from __future__ import annotations

from pathlib import Path


def read_statement_from_path(python_file_path: Path) -> str:
    """Given a path to a Python file, return the contents as a string to be used as a statement in a `timeit` call."""
    with open(python_file_path) as fp:
        # `from __future__ import annotations` can't be imported when using `timeit`.
        return "".join(line for line in fp.readlines() if line != "from __future__ import annotations\n")
