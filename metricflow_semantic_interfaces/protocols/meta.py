from abc import abstractmethod
from typing import Any, Dict, Protocol


class SemanticLayerElementConfig(Protocol):  # noqa: D
    """The config property allows you to configure additional resources/metadata."""

    @property
    @abstractmethod
    def meta(self) -> Dict[str, Any]:
        """The meta field can be used to set metadata for a resource."""
        pass
