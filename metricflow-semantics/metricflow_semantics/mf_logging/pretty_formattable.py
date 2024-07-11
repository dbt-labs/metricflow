from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional


class MetricFlowPrettyFormattable(ABC):
    """Changes behavior for pretty-formatting using `MetricFlowPrettyFormatter`.

    This interface is pending updates to allow for additional configuration and structured return types.
    """

    @property
    @abstractmethod
    def pretty_format(self) -> Optional[str]:
        """Return the pretty-formatted version of this object, or None if the default approach should be used."""
        raise NotImplementedError
