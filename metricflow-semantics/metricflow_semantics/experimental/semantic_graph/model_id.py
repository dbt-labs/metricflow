from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.singleton import Singleton

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class SemanticModelId(Singleton, MetricFlowPrettyFormattable, Comparable):
    """Singleton replacement for `SemanticModelReference`.

    This is used as `SemanticModelReference` is defined in `dbt-semantic-interfaces` and is difficult to change. This
    may be replaced once that's done.
    """

    model_name: str

    @classmethod
    def get_instance(cls, model_name: str) -> SemanticModelId:  # noqa: D102
        return cls._get_instance(model_name=model_name)

    @override
    @cached_property
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
