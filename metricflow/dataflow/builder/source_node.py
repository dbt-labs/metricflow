from typing import Sequence, List

from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    FilterElementsNode,
    MetricTimeDimensionTransformNode,
    ReadSqlSourceNode,
    SemiAdditiveJoinNode,
)
from metricflow.dataflow.builder.measure_additiveness import group_measure_specs_by_additiveness
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.model.semantic_model import SemanticModel
from metricflow.specs import LinklessIdentifierSpec, NonAdditiveDimensionSpec, TimeDimensionSpec, InstanceSpec
from metricflow.plan_conversion.instance_converters import RemoveMeasures


class SourceNodeBuilder:
    """Helps build source nodes to use in the dataflow plan builder.

    The current use case is for creating a set of input nodes from a data set to support multiple aggregation time
    dimensions. Each data set is converted into k DataFlowPlan nodes, one per distinct
    (aggregation time dimension, additive_group_property) pair used in the definition of a measure.
    """

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D
        self._semantic_model = semantic_model

    def _build_semi_additive_source_node(
        self,
        parent_node: BaseOutput[DataSourceDataSet],
        non_additive_dimension_spec: NonAdditiveDimensionSpec,
        filter_specs: Sequence[InstanceSpec],
    ) -> SemiAdditiveJoinNode[DataSourceDataSet]:
        """Builds a SemiAdditiveJoinNode given measures and non-additive dimension attributes."""
        filter_semi_additive_measure_node = FilterElementsNode[DataSourceDataSet](
            parent_node=parent_node,
            include_specs=filter_specs,
            replace_description="Pass Only Semi-additive Measures",
        )

        time_dimension_spec = TimeDimensionSpec.from_name(non_additive_dimension_spec.name)
        window_groupings = tuple(
            LinklessIdentifierSpec.from_element_name(name) for name in non_additive_dimension_spec.window_groupings
        )
        return SemiAdditiveJoinNode[DataSourceDataSet](
            parent_node=filter_semi_additive_measure_node,
            identifier_specs=window_groupings,
            time_dimension_spec=time_dimension_spec,
            agg_by_function=non_additive_dimension_spec.window_choice,
        )

    def create_from_data_sets(self, data_sets: Sequence[DataSourceDataSet]) -> Sequence[BaseOutput[DataSourceDataSet]]:
        """Creates source nodes from DataSourceDataSets.

        Semi-additive measures are handled by grouping all measures with the same non-additive dimension attribute
        into one DataFlowPlan node.
        """
        source_nodes: List[BaseOutput[DataSourceDataSet]] = []
        for data_set in data_sets:
            read_node = ReadSqlSourceNode[DataSourceDataSet](data_set)
            agg_time_dim_to_measures_grouper = (
                self._semantic_model.data_source_semantics.get_aggregation_time_dimensions_with_measures(
                    data_set.data_source_reference
                )
            )
            instance_set_with_no_measures = data_set.instance_set.transform(RemoveMeasures())

            # Dimension sources may not have any measures -> no aggregation time dimensions.
            time_dimension_references = agg_time_dim_to_measures_grouper.keys
            if len(time_dimension_references) == 0:
                source_nodes.append(read_node)
            else:
                # Splits the measures by distinct aggregate time dimension.
                for time_dimension_reference in time_dimension_references:
                    measures = agg_time_dim_to_measures_grouper.get_values(time_dimension_reference)
                    grouped_measures_by_additiveness = group_measure_specs_by_additiveness(measures)
                    additive_measure_specs = grouped_measures_by_additiveness.additive_measures

                    # Build source node for additive measures
                    if additive_measure_specs:
                        filter_elements_node = FilterElementsNode[DataSourceDataSet](
                            parent_node=read_node,
                            include_specs=tuple(instance_set_with_no_measures.spec_set.all_specs)
                            + additive_measure_specs,
                            replace_description="Pass Only Additive Measures",
                        )
                        source_nodes.append(
                            MetricTimeDimensionTransformNode[DataSourceDataSet](
                                parent_node=filter_elements_node,
                                aggregation_time_dimension_reference=time_dimension_reference,
                            )
                        )

                    # Build a SemiAdditiveJoinNode for each semi-additive measure grouping.
                    grouped_semi_additive_measures = grouped_measures_by_additiveness.grouped_semi_additive_measures
                    for measure_group in grouped_semi_additive_measures:
                        non_additive_dimension_spec = measure_group[0].non_additive_dimension_spec
                        assert non_additive_dimension_spec
                        semi_additive_join_node = self._build_semi_additive_source_node(
                            parent_node=read_node,
                            non_additive_dimension_spec=non_additive_dimension_spec,
                            filter_specs=tuple(instance_set_with_no_measures.spec_set.all_specs) + measure_group,
                        )
                        source_nodes.append(
                            MetricTimeDimensionTransformNode[DataSourceDataSet](
                                parent_node=semi_additive_join_node,
                                aggregation_time_dimension_reference=time_dimension_reference,
                            )
                        )
        return source_nodes
