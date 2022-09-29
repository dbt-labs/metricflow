from typing import Sequence, List

from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    MetricTimeDimensionTransformNode,
    ReadSqlSourceNode,
)
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.model.semantic_model import SemanticModel


class SourceNodeBuilder:
    """Helps build source nodes to use in the dataflow plan builder.

    The current use case is for creating a set of input nodes from a data set to support multiple aggregation time
    dimensions. Each data set is converted into k DataFlowPlan nodes.
    """

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D
        self._semantic_model = semantic_model

    def create_from_data_sets(self, data_sets: Sequence[DataSourceDataSet]) -> Sequence[BaseOutput[DataSourceDataSet]]:
        """Creates source nodes from DataSourceDataSets."""
        source_nodes: List[BaseOutput[DataSourceDataSet]] = []
        for data_set in data_sets:
            read_node = ReadSqlSourceNode[DataSourceDataSet](data_set)
            agg_time_dim_to_measures_grouper = (
                self._semantic_model.data_source_semantics.get_aggregation_time_dimensions_with_measures(
                    data_set.data_source_reference
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
                        MetricTimeDimensionTransformNode[DataSourceDataSet](
                            parent_node=read_node,
                            aggregation_time_dimension_reference=time_dimension_reference,
                        )
                    )
        return source_nodes
