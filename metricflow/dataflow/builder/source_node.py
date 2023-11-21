from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.references import TimeDimensionReference

from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    MetricTimeDimensionTransformNode,
    ReadSqlSourceNode,
)
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.time_spine import TimeSpineSource


class SourceNodeBuilder:
    """Helps build source nodes to use in the dataflow plan builder.

    The current use case is for creating a set of input nodes from a data set to support multiple aggregation time
    dimensions. Each data set is converted into k DataFlowPlan nodes.
    """

    def __init__(self, semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._semantic_manifest_lookup = semantic_manifest_lookup

    def create_from_data_sets(self, data_sets: Sequence[SemanticModelDataSet]) -> Sequence[BaseOutput]:
        """Creates source nodes from SemanticModelDataSets."""
        source_nodes: List[BaseOutput] = []
        for data_set in data_sets:
            read_node = ReadSqlSourceNode(data_set)
            agg_time_dim_to_measures_grouper = (
                self._semantic_manifest_lookup.semantic_model_lookup.get_aggregation_time_dimensions_with_measures(
                    data_set.semantic_model_reference
                )
            )

            # Dimension sources may not have any measures -> no aggregation time dimensions.
            time_dimension_references = agg_time_dim_to_measures_grouper.keys
            if len(time_dimension_references) == 0:
                source_nodes.append(read_node)
            else:
                # Splits the measures by distinct aggregate time dimension.
                for time_dimension_reference in time_dimension_references:
                    source_nodes.append(
                        MetricTimeDimensionTransformNode(
                            parent_node=read_node,
                            aggregation_time_dimension_reference=time_dimension_reference,
                        )
                    )
        return source_nodes

    def create_read_nodes_from_data_sets(
        self, data_sets: Sequence[SemanticModelDataSet]
    ) -> Sequence[ReadSqlSourceNode]:
        """Creates read nodes from SemanticModelDataSets."""
        return [ReadSqlSourceNode(data_set) for data_set in data_sets]

    @staticmethod
    def build_time_spine_source_node(
        time_spine_source: TimeSpineSource, data_set_converter: SemanticModelToDataSetConverter
    ) -> MetricTimeDimensionTransformNode:
        """Build a source node from the time spine source table."""
        time_spine_data_set = data_set_converter.build_time_spine_source_data_set(time_spine_source)
        time_dim_reference = TimeDimensionReference(element_name=time_spine_source.time_column_name)
        return MetricTimeDimensionTransformNode(
            parent_node=ReadSqlSourceNode(data_set=time_spine_data_set),
            aggregation_time_dimension_reference=time_dim_reference,
        )
