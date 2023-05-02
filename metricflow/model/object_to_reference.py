from dbt_semantic_interfaces.objects.data_source import DataSource
from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metric import Metric

from dbt_semantic_interfaces.references import (
    DataSourceReference,
    DimensionReference,
    MeasureReference,
    MetricReference,
    EntityReference,
)


class ObjectToReference:
    """Converting model objects to a model reference.

    With moving all the model objects to dbt-semantic-interfaces and having *Reference objects
    remain in MetricFlow, all the `Object.reference` properties should be removed from the model
    objects. This means, we would need to replace all usages of `Object.reference` to
    instantiating the *Reference object directly. This class organizes and keeps note of
    where those use cases were.
    """

    @staticmethod
    def from_data_source(data_source: DataSource) -> DataSourceReference:  # noqa: D
        return DataSourceReference(data_source_name=data_source.name)

    @staticmethod
    def from_measure(measure: Measure) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=measure.name)

    @staticmethod
    def from_identifier(identifier: Entity) -> EntityReference:  # noqa: D
        return EntityReference(element_name=identifier.name)

    @staticmethod
    def from_metric(metric: Metric) -> MetricReference:  # noqa: D
        return MetricReference(element_name=metric.name)

    @staticmethod
    def from_dimension(dimension: Dimension) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=dimension.name)
