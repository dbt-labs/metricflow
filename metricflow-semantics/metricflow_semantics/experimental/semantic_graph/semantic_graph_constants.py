from __future__ import annotations

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow_semantics.experimental.semantic_graph.entity_id import VirtualEntityId


class EntityConstant:
    METRIC_TIME_ENTITY_ID = VirtualEntityId.get_instance(METRIC_TIME_ELEMENT_NAME)
