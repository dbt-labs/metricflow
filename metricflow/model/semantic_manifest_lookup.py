from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.semantic_model_semantics import SemanticModelSemantics
from metricflow.model.semantics.metric_semantics import MetricSemantics
from metricflow.protocols.semantics import SemanticModelSemanticsAccessor, MetricSemanticsAccessor


class SemanticManifestLookup:
    """Adds semantics information to the user configured model."""

    def __init__(self, user_configured_model: UserConfiguredModel) -> None:  # noqa: D
        self._user_configured_model = user_configured_model
        self._semantic_model_semantics = SemanticModelSemantics(user_configured_model)
        self._metric_semantics = MetricSemantics(self._user_configured_model, self._semantic_model_semantics)

    @property
    def user_configured_model(self) -> UserConfiguredModel:  # noqa: D
        return self._user_configured_model

    @property
    def semantic_model_semantics(self) -> SemanticModelSemanticsAccessor:  # noqa: D
        return self._semantic_model_semantics

    @property
    def metric_semantics(self) -> MetricSemanticsAccessor:  # noqa: D
        return self._metric_semantics
