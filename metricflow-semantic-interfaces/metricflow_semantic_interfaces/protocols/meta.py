from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Protocol


class SemanticLayerElementConfig(Protocol):  # noqa: D101
    """The config property allows you to configure additional resources/metadata."""

    @property
    @abstractmethod
    def meta(self) -> Dict[str, Any]:  # type: ignore[misc]
        """The meta field can be used to set metadata for a resource."""
        pass
