from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.format_option import PrettyFormatOption
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.toolkit.table_helpers import IsolatedTabulateRunner
from typing_extensions import override

from metricflow.metric_evaluation.plan.me_nodes import (
    ConversionMetricQueryNode,
    CumulativeMetricQueryNode,
    DerivedMetricsQueryNode,
    MetricQueryNode,
    MetricQueryNodeVisitor,
    SimpleMetricsQueryNode,
    TopLevelQueryNode,
)
from metricflow.metric_evaluation.plan.me_plan import (
    MetricEvaluationPlan,
)

logger = logging.getLogger(__name__)


class MetricEvaluationPlanTableFormatter:
    """Formats a `MetricEvaluationPlan` into a set of text tables for easier review.

    Example overview table:
        ```
        id     src_ids    computed_outputs      passthrough_outputs    source_metrics        semantic_model
        -----  ---------  --------------------  ---------------------  --------------------  ----------------
        smp_0             listings                                                           listings_latest
        smp_1             bookings                                                           bookings_source
        drv_0  smp_1      bookings_per_listing                         bookings
               smp_0                                                   listings
        tpl_0  drv_0                            bookings_per_listing   bookings_per_listing
        ```

    Example node output table:
        ```
        id     output_specs                                     output_properties
        -----  -----------------------------------------------  -------------------------------------------------
        smp_0  MetricSpec(element_name='listings')              MetricQueryPropertySet(
                                                                  group_by_item_specs=['metric_time__day'],
                                                                  pushdown_enabled_types={TIME_RANGE_CONSTRAINT},
                                                                )
        smp_1  MetricSpec(element_name='bookings')              MetricQueryPropertySet(
                                                                  group_by_item_specs=['metric_time__day'],
                                                                  pushdown_enabled_types={TIME_RANGE_CONSTRAINT},
                                                                )
        drv_0  MetricSpec(element_name='bookings_per_listing')  MetricQueryPropertySet(
                                                                  group_by_item_specs=['metric_time__day'],
                                                                  pushdown_enabled_types={TIME_RANGE_CONSTRAINT},
                                                                )
        tpl_0  MetricSpec(element_name='bookings_per_listing')  MetricQueryPropertySet(
                                                                  group_by_item_specs=['metric_time__day'],
                                                                  pushdown_enabled_types={TIME_RANGE_CONSTRAINT},
                                                                )
        ```
    """

    def __init__(self) -> None:  # noqa: D107
        self._table_format = "simple_grid"

    def format_plan(self, me_plan: MetricEvaluationPlan) -> TableFormatResult:
        """Return text-tables that describe the given plan."""
        # Build the result by adding rows for each of the nodes.
        format_result = _TableFormatRowsResult()
        row_adder = MetricEvaluationPlanTableFormatter._QueryNodeTableRowAdder(
            me_plan=me_plan,
            format_result=format_result,
        )

        nodes_in_dfs_order = me_plan.nodes_in_dfs_order()

        for node in nodes_in_dfs_order:
            node.accept(row_adder)

        overview_table_headers = (
            _TableFormatRowsResult.COMMON_HEADER__NODE_ID,
            _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__SOURCE_NODE_IDS,
            _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__COMPUTED_METRICS,
            _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__PASSTHROUGH_METRICS,
            _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__SOURCE_METRICS,
            _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__MODEL_ID,
        )
        overview_table = IsolatedTabulateRunner.tabulate(
            [[row.get(header) for header in overview_table_headers] for row in format_result.overview_table_rows],
            headers=overview_table_headers,
            tablefmt=self._table_format,
        )

        node_output_table_headers = (
            _TableFormatRowsResult.COMMON_HEADER__NODE_ID,
            _TableFormatRowsResult.NODE_OUTPUT_TABLE_HEADER__OUTPUT_METRIC_SPECS,
            _TableFormatRowsResult.NODE_OUTPUT_TABLE_HEADER__OUTPUT_PROPERTIES,
        )
        node_output_table = IsolatedTabulateRunner.tabulate(
            [[row.get(header) for header in node_output_table_headers] for row in format_result.node_output_table_rows],
            headers=node_output_table_headers,
            tablefmt=self._table_format,
        )

        return TableFormatResult(
            overview_table=overview_table,
            node_output_table=node_output_table,
        )

    class _QueryNodeTableRowAdder(MetricQueryNodeVisitor[None]):
        """Visitor to add a rows for a given node."""

        def __init__(
            self,
            me_plan: MetricEvaluationPlan,
            format_result: _TableFormatRowsResult,
        ) -> None:
            self._me_plan = me_plan
            self._format_result = format_result
            self._column_format_option = PrettyFormatOption(
                max_line_length=60,
            )

        @override
        def visit_simple_metrics_query_node(self, node: SimpleMetricsQueryNode) -> None:
            self._add_row_to_overview_table(node=node, computed_metric_specs=node.metric_specs, model_id=node.model_id)
            self._add_row_to_output_table(node)

        @override
        def visit_cumulative_metric_query_node(self, node: CumulativeMetricQueryNode) -> None:
            self._add_row_to_overview_table(
                node=node,
                computed_metric_specs=[node.metric_spec],
            )
            self._add_row_to_output_table(node)

        @override
        def visit_conversion_metric_query_node(self, node: ConversionMetricQueryNode) -> None:
            self._add_row_to_overview_table(
                node=node,
                computed_metric_specs=[node.metric_spec],
            )
            self._add_row_to_output_table(node)

        @override
        def visit_derived_metrics_query_node(self, node: DerivedMetricsQueryNode) -> None:
            self._add_row_to_overview_table(
                node=node,
                computed_metric_specs=node.computed_metric_specs,
                passthrough_metrics_specs=node.passthrough_metric_specs,
                source_metrics_specs=FrozenOrderedSet(
                    edge.source_node_output_spec for edge in self._me_plan.source_edges(node)
                ),
            )
            self._add_row_to_output_table(node)

        @override
        def visit_top_level_query_node(self, node: TopLevelQueryNode) -> None:
            self._add_row_to_overview_table(
                node=node,
                passthrough_metrics_specs=node.passthrough_metric_specs,
                source_metrics_specs=[edge.source_node_output_spec for edge in self._me_plan.source_edges(node)],
            )
            self._add_row_to_output_table(node)

        def _add_row_to_overview_table(
            self,
            node: MetricQueryNode,
            computed_metric_specs: Iterable[MetricSpec] = (),
            passthrough_metrics_specs: Iterable[MetricSpec] = (),
            source_metrics_specs: Iterable[MetricSpec] = (),
            model_id: Optional[SemanticModelId] = None,
        ) -> None:
            self._format_result.overview_table_rows.append(
                {
                    _TableFormatRowsResult.COMMON_HEADER__NODE_ID: node.node_id.str_value,
                    _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__SOURCE_NODE_IDS: "\n".join(
                        source_node.node_id.str_value for source_node in self._me_plan.source_nodes(node)
                    ),
                    _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__COMPUTED_METRICS: "\n".join(
                        self._short_format_metric_spec(metric_spec) for metric_spec in computed_metric_specs
                    ),
                    _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__PASSTHROUGH_METRICS: "\n".join(
                        self._short_format_metric_spec(spec) for spec in passthrough_metrics_specs
                    ),
                    _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__SOURCE_METRICS: "\n".join(
                        self._short_format_metric_spec(spec) for spec in source_metrics_specs
                    ),
                    _TableFormatRowsResult.OVERVIEW_TABLE_HEADER__MODEL_ID: model_id.model_name
                    if model_id is not None
                    else None,
                }
            )

        def _add_row_to_output_table(self, node: MetricQueryNode) -> None:
            self._format_result.node_output_table_rows.append(
                {
                    _TableFormatRowsResult.COMMON_HEADER__NODE_ID: node.node_id.str_value,
                    _TableFormatRowsResult.NODE_OUTPUT_TABLE_HEADER__OUTPUT_METRIC_SPECS: "\n".join(
                        mf_pformat(spec, format_option=self._column_format_option) for spec in node.output_metric_specs
                    ),
                    _TableFormatRowsResult.NODE_OUTPUT_TABLE_HEADER__OUTPUT_PROPERTIES: mf_pformat(
                        node.query_properties, format_option=self._column_format_option
                    ),
                }
            )

        def _short_format_metric_spec(self, metric_spec: MetricSpec) -> str:
            """Render a metric spec in a short format like `metric_name [AS metric_alias]`.

            This compact representation excludes some details like filter for the metric, but it yields a more narrow
            table.
            """
            if metric_spec.alias is None:
                return metric_spec.element_name
            else:
                return f"{metric_spec.element_name} AS {metric_spec.alias}"


@dataclass
class _TableFormatRowsResult:
    """Internal class to store intermediate data while rendering thee result tables.."""

    # Header names

    # Node ID
    COMMON_HEADER__NODE_ID = "id"

    # Headers for the overview table for nodes.
    OVERVIEW_TABLE_HEADER__SOURCE_NODE_IDS = "src_ids"
    OVERVIEW_TABLE_HEADER__COMPUTED_METRICS = "computed_outputs"
    OVERVIEW_TABLE_HEADER__PASSTHROUGH_METRICS = "passthrough_outputs"
    OVERVIEW_TABLE_HEADER__SOURCE_METRICS = "source_metrics"
    OVERVIEW_TABLE_HEADER__MODEL_ID = "semantic_model"

    # Headers for the table that describe the output of the node.
    NODE_OUTPUT_TABLE_HEADER__OUTPUT_METRIC_SPECS = "output_specs"
    NODE_OUTPUT_TABLE_HEADER__OUTPUT_PROPERTIES = "output_properties"

    overview_table_rows: list[Mapping[str, Optional[str]]] = dataclasses.field(default_factory=list)
    node_output_table_rows: list[Mapping[str, Optional[str]]] = dataclasses.field(default_factory=list)


@fast_frozen_dataclass()
class TableFormatResult:
    """Result object to return rendered text tables."""

    # Table that contains a simplified view of nodes and dependency relationships.
    overview_table: str
    # Table that describes the output of the node.
    node_output_table: str
