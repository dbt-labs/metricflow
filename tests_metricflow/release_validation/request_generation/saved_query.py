from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols import SavedQuery, SemanticManifest
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from tests_metricflow.release_validation.request_generation.request_generator import MetricFlowRequestGenerator


class SavedQueryRequestGenerator(MetricFlowRequestGenerator):
    """Generate a request for each saved query in the manifest."""

    def __init__(self, saved_query_names: Optional[Sequence[str]] = None) -> None:  # noqa: D107
        self._saved_query_names = tuple(saved_query_names) if saved_query_names is not None else None

    @override
    def generate_requests(self, semantic_manifest: SemanticManifest) -> Sequence[MetricFlowQueryRequest]:
        selected_saved_queries: list[SavedQuery] = []
        if self._saved_query_names is not None:
            name_to_saved_query = {saved_query.name: saved_query for saved_query in semantic_manifest.saved_queries}
            for saved_query_name in self._saved_query_names:
                saved_query = name_to_saved_query.get(saved_query_name)
                if saved_query is None:
                    raise ValueError(
                        LazyFormat(
                            "Unknown saved query name",
                            saved_query_name=saved_query_name,
                            valid_saved_query_names=name_to_saved_query.keys(),
                        )
                    )
                selected_saved_queries.append(saved_query)
        else:
            selected_saved_queries.extend(semantic_manifest.saved_queries)
        return tuple(
            MetricFlowQueryRequest.create(saved_query_name=saved_query.name) for saved_query in selected_saved_queries
        )
