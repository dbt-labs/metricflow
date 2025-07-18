from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


@singleton_dataclass(order=False)
class SemanticModelId(MetricFlowPrettyFormattable, Comparable):
    """Singleton replacement for `SemanticModelReference`.

    This is used as `SemanticModelReference` is defined in `dbt-semantic-interfaces` and is difficult to change. This
    may be replaced once that's done.
    """

    model_name: str

    @staticmethod
    def get_instance(model_name: str) -> SemanticModelId:  # noqa: D102
        return SemanticModelId(model_name=model_name)

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.model_name,)

    @override
    def __str__(self) -> str:
        return self.model_name

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self.model_name

    @cached_property
    def semantic_model_reference(self) -> SemanticModelReference:
        """In place for migration."""
        return SemanticModelReference(semantic_model_name=self.model_name)
