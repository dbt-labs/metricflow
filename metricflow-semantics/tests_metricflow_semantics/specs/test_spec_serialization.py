from __future__ import annotations

from dbt_semantic_interfaces.dataclass_serialization import DataClassDeserializer, DataclassSerializer
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums import DatePart, DimensionType, TimeGranularity
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import (
    LinkableDimension,
    LinkableElementUnion,
    SemanticModelJoinPath,
    SemanticModelJoinPathElement,
)
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameter, SqlBindParameters, SqlBindParameterValue
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


def test_where_filter_spec_serialization() -> None:  # noqa: D103
    where_filter_spec = WhereFilterSpec(
        where_sql="where_sql",
        bind_parameters=SqlBindParameters(
            param_items=(SqlBindParameter(key="key", value=SqlBindParameterValue(str_value="str_value")),)
        ),
        linkable_element_unions=(
            LinkableElementUnion(
                linkable_dimension=LinkableDimension(
                    properties=(LinkableElementProperty.JOINED,),
                    defined_in_semantic_model=SemanticModelReference(semantic_model_name="semantic_model_name"),
                    element_name="element_name",
                    dimension_type=DimensionType.CATEGORICAL,
                    entity_links=(EntityReference(element_name="element_name"),),
                    join_path=SemanticModelJoinPath(
                        left_semantic_model_reference=SemanticModelReference(semantic_model_name="semantic_model_name"),
                        path_elements=(
                            SemanticModelJoinPathElement(
                                semantic_model_reference=SemanticModelReference(
                                    semantic_model_name="semantic_model_name"
                                ),
                                join_on_entity=EntityReference(element_name="element_name"),
                            ),
                        ),
                    ),
                    date_part=DatePart.DAY,
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                ),
            ),
        ),
        linkable_spec_set=LinkableSpecSet(
            dimension_specs=(
                DimensionSpec(
                    element_name="element_name",
                    entity_links=(EntityReference(element_name="element_name"),),
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="element_name",
                    entity_links=(EntityReference(element_name="element_name"),),
                    time_granularity=TimeGranularity.YEAR,
                ),
            ),
            entity_specs=(
                EntitySpec(
                    element_name="element_name",
                    entity_links=(EntityReference(element_name="element_name"),),
                ),
            ),
            group_by_metric_specs=(
                GroupByMetricSpec(
                    element_name="element_name",
                    entity_links=(EntityReference(element_name="element_name"),),
                    metric_subquery_entity_links=(EntityReference(element_name="element_name"),),
                ),
            ),
        ),
    )

    serializer = DataclassSerializer()
    deserializer = DataClassDeserializer()

    serialized_spec = serializer.pydantic_serialize(where_filter_spec)
    deserialized_spec = deserializer.pydantic_deserialize(WhereFilterSpec, serialized_spec)

    assert where_filter_spec == deserialized_spec
