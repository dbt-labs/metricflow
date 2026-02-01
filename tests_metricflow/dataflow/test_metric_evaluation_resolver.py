from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.dataflow.metric_evaluation_resolver import MetricEvaluationResolver

logger = logging.getLogger(__name__)


def test_metric_evaluation_resolver(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    resolver = MetricEvaluationResolver(ManifestObjectLookup(simple_semantic_manifest))

    result = resolver.resolve_evaluation_for_query(
        metric_names=["bookings_per_listing", "bookings"],
        where_filters=[],
    )

    logger.info(LazyFormat("Generated result", result=result))
