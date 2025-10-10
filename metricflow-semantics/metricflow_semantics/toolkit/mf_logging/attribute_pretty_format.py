from __future__ import annotations

import logging
from abc import ABC
from collections.abc import Mapping
from functools import cached_property
from typing import Optional

from typing_extensions import override

from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)

AttributeMapping = Mapping[str, object]


class AttributePrettyFormattable(MetricFlowPrettyFormattable, ABC):
    """Mixin that allows classes to be pretty-printed with a defined set of attributes."""

    @cached_property
    def _attribute_mapping(self) -> AttributeMapping:
        """Return a mapping from the attribute name to the value for pretty printing."""
        return {}

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping=self._attribute_mapping,
        )
