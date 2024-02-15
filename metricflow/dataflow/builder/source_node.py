from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

from dbt_semantic_interfaces.references import TimeDimensionReference

from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    MetricTimeDimensionTransformNode,
    ReadSqlSourceNode,
)
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.specs.column_assoc import ColumnAssociationResolver


@dataclass(frozen=True)
class SourceNodeSet:
    """Contains nodes / components that are used by the `DataflowPlanBuilder` as root building blocks.

    The components in this set do not need to be dynamically generated on a per-query basis for a given semantic
    manifest.
    """

    # Semantic models without measures are 1:1 mapped to a ReadSqlSourceNode. Semantic models containing measures are
    # mapped to components with a transformation node to add `metric_time` / to support multiple aggregation time
    # dimensions. Each semantic model containing measures with k different aggregation time dimensions is mapped to k
    # components.
    source_nodes_for_metric_queries: Tuple[BaseOutput, ...]

    # Semantic models are 1:1 mapped to a ReadSqlSourceNode. The tuple also contains the same `time_spine_node` as
    # below. See usage in `DataflowPlanBuilder`.
    source_nodes_for_group_by_item_queries: Tuple[BaseOutput, ...]

    # Provides the time spine.
    time_spine_node: MetricTimeDimensionTransformNode

    @property
    def all_nodes(self) -> Sequence[BaseOutput]:  # noqa: D
        return (
            self.source_nodes_for_metric_queries + self.source_nodes_for_group_by_item_queries + (self.time_spine_node,)
        )


class SourceNodeBuilder:
    """Helps build a `SourceNodeSet` - refer to that class for more details."""

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
    ) -> None:
        self._semantic_manifest_lookup = semantic_manifest_lookup
        data_set_converter = SemanticModelToDataSetConverter(column_association_resolver)
        time_spine_source = self._semantic_manifest_lookup.time_spine_source
        time_spine_data_set = data_set_converter.build_time_spine_source_data_set(time_spine_source)
        time_dim_reference = TimeDimensionReference(element_name=time_spine_source.time_column_name)
        self._time_spine_source_node = MetricTimeDimensionTransformNode(
            parent_node=ReadSqlSourceNode(data_set=time_spine_data_set),
            aggregation_time_dimension_reference=time_dim_reference,
        )

    def create_from_data_sets(self, data_sets: Sequence[SemanticModelDataSet]) -> SourceNodeSet:
        """Creates a `SourceNodeSet` from SemanticModelDataSets."""
        group_by_item_source_nodes: List[BaseOutput] = []
        source_nodes_for_metric_queries: List[BaseOutput] = []

        for data_set in data_sets:
            read_node = ReadSqlSourceNode(data_set)
            group_by_item_source_nodes.append(read_node)
            agg_time_dim_to_measures_grouper = (
                self._semantic_manifest_lookup.semantic_model_lookup.get_aggregation_time_dimensions_with_measures(
                    data_set.semantic_model_reference
                )
            )

            # Dimension sources may not have any measures -> no aggregation time dimensions.
            time_dimension_references = agg_time_dim_to_measures_grouper.keys
            if len(time_dimension_references) == 0:
                source_nodes_for_metric_queries.append(read_node)
            else:
                # Splits the measures by distinct aggregate time dimension.
                for time_dimension_reference in time_dimension_references:
                    metric_time_transform_node = MetricTimeDimensionTransformNode(
                        parent_node=read_node,
                        aggregation_time_dimension_reference=time_dimension_reference,
                    )
                    source_nodes_for_metric_queries.append(metric_time_transform_node)

        return SourceNodeSet(
            time_spine_node=self._time_spine_source_node,
            source_nodes_for_group_by_item_queries=tuple(group_by_item_source_nodes) + (self._time_spine_source_node,),
            source_nodes_for_metric_queries=tuple(source_nodes_for_metric_queries),
        )
