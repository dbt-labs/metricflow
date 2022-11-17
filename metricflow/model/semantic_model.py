from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.data_source_container import PydanticDataSourceContainer
from metricflow.model.semantics.data_source_semantics import DataSourceSemantics
from metricflow.model.semantics.metric_semantics import MetricSemantics
from metricflow.protocols.semantics import DataSourceSemanticsAccessor, MetricSemanticsAccessor


class SemanticModel:
    """Adds semantics information to the user configured model."""

    def __init__(self, user_configured_model: UserConfiguredModel) -> None:  # noqa: D
        self._user_configured_model = user_configured_model
        self._data_source_semantics = DataSourceSemantics(
            user_configured_model, PydanticDataSourceContainer(user_configured_model.data_sources)
        )
        self._metric_semantics = MetricSemantics(self._user_configured_model, self._data_source_semantics)

    @property
    def user_configured_model(self) -> UserConfiguredModel:  # noqa: D
        return self._user_configured_model

    @property
    def data_source_semantics(self) -> DataSourceSemanticsAccessor:  # noqa: D
        return self._data_source_semantics

    @property
    def metric_semantics(self) -> MetricSemanticsAccessor:  # noqa: D
        return self._metric_semantics
