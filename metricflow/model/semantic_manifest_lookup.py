from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from metricflow.model.semantics.semantic_model_semantics import SemanticModelSemantics
from metricflow.model.semantics.metric_semantics import MetricSemantics
from metricflow.protocols.semantics import SemanticModelSemanticsAccessor, MetricSemanticsAccessor


class SemanticManifestLookup:
    """Adds semantics information to the user configured model."""

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
        self._semantic_manifest = semantic_manifest
        self._semantic_model_semantics = SemanticModelSemantics(semantic_manifest)
        self._metric_semantics = MetricSemantics(self._semantic_manifest, self._semantic_model_semantics)

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D
        return self._semantic_manifest

    @property
    def semantic_model_semantics(self) -> SemanticModelSemanticsAccessor:  # noqa: D
        return self._semantic_model_semantics

    @property
    def metric_semantics(self) -> MetricSemanticsAccessor:  # noqa: D
        return self._metric_semantics
