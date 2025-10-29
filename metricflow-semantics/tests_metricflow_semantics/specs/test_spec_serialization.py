from __future__ import annotations

import logging

from dbt_semantic_interfaces.dataclass_serialization import DataClassDeserializer, DataclassSerializer
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameter, SqlBindParameterSet, SqlBindParameterValue
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


def test_where_filter_spec_serialization() -> None:  # noqa: D103
    time_grain = ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY)
    where_filter_spec = WhereFilterSpec(
        where_sql="where_sql",
        bind_parameters=SqlBindParameterSet(
            param_items=(SqlBindParameter(key="key", value=SqlBindParameterValue(str_value="str_value")),)
        ),
        element_set=GroupByItemSet.create(
            AnnotatedSpec.create(
                element_type=LinkableElementType.TIME_DIMENSION,
                element_name="element_name",
                entity_links=(EntityReference(element_name="element_name"),),
                date_part=None,
                metric_subquery_entity_links=None,
                time_grain=time_grain,
                properties=(GroupByItemProperty.LOCAL,),
                origin_model_ids=(SemanticModelId.get_instance("model_name"),),
                derived_from_semantic_models=(SemanticModelReference("model_name"),),
            ),
        ),
    )

    serializer = DataclassSerializer()
    deserializer = DataClassDeserializer()

    serialized_spec = serializer.pydantic_serialize(where_filter_spec)
    deserialized_spec = deserializer.pydantic_deserialize(WhereFilterSpec, serialized_spec)

    assert where_filter_spec == deserialized_spec
