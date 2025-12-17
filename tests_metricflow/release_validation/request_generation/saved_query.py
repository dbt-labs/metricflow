from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.protocols import SemanticManifest
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from tests_metricflow.release_validation.request_generation.request_generator import MetricFlowRequestGenerator


class SavedQueryRequestGenerator(MetricFlowRequestGenerator):
    """Generate a request for each saved query in the manifest."""

    @override
    def generate_requests(self, semantic_manifest: SemanticManifest) -> Sequence[MetricFlowQueryRequest]:
        return tuple(
            MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name)
            for saved_query in semantic_manifest.saved_queries
        )
