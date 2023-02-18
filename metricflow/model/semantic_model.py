from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.semantics.entity_container import PydanticMetricFlowEntityContainer
from metricflow.model.semantics.entity_semantics import MetricFlowEntitySemantics
from metricflow.model.semantics.metric_semantics import MetricSemantics
from metricflow.protocols.semantics import MetricFlowEntitySemanticsAccessor, MetricSemanticsAccessor


class SemanticModel:
    """Adds semantics information to the user configured model."""

    def __init__(self, user_configured_model: UserConfiguredModel) -> None:  # noqa: D
        self._user_configured_model = user_configured_model
        self._entity_semantics = MetricFlowEntitySemantics(
            user_configured_model, PydanticMetricFlowEntityContainer(user_configured_model.entities)
        )
        self._metric_semantics = MetricSemantics(self._user_configured_model, self._entity_semantics)

    @property
    def user_configured_model(self) -> UserConfiguredModel:  # noqa: D
        return self._user_configured_model

    @property
    def entity_semantics(self) -> MetricFlowEntitySemanticsAccessor:  # noqa: D
        return self._entity_semantics

    @property
    def metric_semantics(self) -> MetricSemanticsAccessor:  # noqa: D
        return self._metric_semantics
