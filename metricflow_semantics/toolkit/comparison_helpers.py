from __future__ import annotations

from typing import Any, Protocol


class SupportsLessThan(Protocol):
    """Protocol describing an object that supports `<`.

    This should be replaced with an already-available implementation.
    """

    def __lt__(self, other: ComparisonOtherType) -> bool:
        """Standard Python `<` comparison."""
        ...


# Type used to annotate the `other` argument in standard comparison methods like `__lt__`.
# Helpful to reduce the need to put `# type: ignore` in many places.
ComparisonOtherType = Any  # type: ignore
