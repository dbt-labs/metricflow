from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.metric_spec import MetricSpec

from metricflow.dataflow.metric_evaluation.me_planner import JoinCountOptimizedMetricEvaluationPlanner

logger = logging.getLogger(__name__)


def test_me_planner(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    me_planner = JoinCountOptimizedMetricEvaluationPlanner(ManifestObjectLookup(simple_semantic_manifest))
    me_planner.build_plan(
        [
            MetricSpec(element_name="bookings_per_listing"),
            MetricSpec(element_name="bookings"),
        ]
    )
