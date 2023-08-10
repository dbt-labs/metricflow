from __future__ import annotations


class QueryInterfaceMetric:
    """Metric in the query interface."""

    def __init__(self, name: str) -> None:  # noqa: D
        self.name = name

    def pct_growth(self) -> QueryInterfaceMetric:
        """The percentage growth."""
        raise NotImplementedError("percent growth is not implemented yet")

    def __str__(self) -> str:
        """The Metric's name."""
        return self.name
