from __future__ import annotations

from metricflow_semantic_interfaces.enum_extension import ExtendedEnum


class SemanticManifestNodeType(ExtendedEnum):
    """Currently supported node types."""

    METRIC = "metric"
    SAVED_QUERY = "saved_query"
    SEMANTIC_MODEL = "semantic_model"
    TIME_SPINE = "time_spine"
