from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from typing import Optional

if typing.TYPE_CHECKING:
    from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext


class MetricFlowPrettyFormattable(ABC):
    """Changes behavior for pretty-formatting using `MetricFlowPrettyFormatter`.

    This interface is pending updates to allow for additional configuration and structured return types.
    """

    @abstractmethod
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        """Return the pretty-formatted version of this object, or `None` if the default approach should be used."""
        raise NotImplementedError
