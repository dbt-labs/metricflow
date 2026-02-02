from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.dataflow.metric_evaluation_resolver import MetricEvaluationCookbook, MetricEvaluationResolver

logger = logging.getLogger(__name__)


def test_metric_evaluation_resolver(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    resolver = MetricEvaluationResolver(ManifestObjectLookup(simple_semantic_manifest))

    evaluation_lookup = resolver.resolve_evaluation_lookup_for_query(
        metric_specs=[
            MetricSpec("bookings_per_listing"),
            MetricSpec("bookings"),
        ],
    )

    logger.info(LazyFormat("Generated evaluation_lookup", evaluation_lookup=evaluation_lookup))

    cookbook = MetricEvaluationCookbook(evaluation_lookup)
    cookbook.get_recipe()
