from __future__ import annotations

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest

from metricflow.model.semantics.metric_lookup import MetricLookup
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow.protocols.semantics import SemanticModelAccessor


class SemanticManifestLookup:
    """Adds semantics information to the user configured model."""

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
        self._semantic_manifest = semantic_manifest
        self._semantic_model_lookup = SemanticModelLookup(semantic_manifest)
        self._metric_lookup = MetricLookup(self._semantic_manifest, self._semantic_model_lookup)

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D
        return self._semantic_manifest

    @property
    def semantic_model_lookup(self) -> SemanticModelAccessor:  # noqa: D
        return self._semantic_model_lookup

    @property
    def metric_lookup(self) -> MetricLookup:  # noqa: D
        return self._metric_lookup
