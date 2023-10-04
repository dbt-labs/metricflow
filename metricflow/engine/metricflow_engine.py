from __future__ import annotations

import datetime
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimensionTypeParams
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.references import EntityReference, MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow.assert_one_arg import assert_exactly_one_arg_set
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import (
    DataflowPlanNodeOutputDataSetResolver,
)
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import DataflowPlan
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import (
    SourceScanOptimizer,
)
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.dataset import DataSet
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.models import Dimension, Entity, Measure, Metric
from metricflow.engine.time_source import ServerTimeSource
from metricflow.errors.errors import ExecutionException
from metricflow.execution.execution_plan import ExecutionPlan, SqlQuery
from metricflow.execution.execution_plan_to_text import execution_plan_to_text
from metricflow.execution.executor import SequentialPlanExecutor
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.formatting import indent_log_line
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.model.semantics.linkable_element_properties import (
    LinkableElementProperties,
)
from metricflow.model.semantics.linkable_spec_resolver import LinkableDimension
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_execution import (
    DataflowToExecutionPlanConverter,
)
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.query_parameter import GroupByParameter, MetricQueryParameter, OrderByQueryParameter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.random_id import random_id
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import InstanceSpecSet, MetricFlowQuerySpec
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_source import TimeSource

logger = logging.getLogger(__name__)
_telemetry_reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
_telemetry_reporter.add_python_log_handler()
_telemetry_reporter.add_rudderstack_handler()


@dataclass(frozen=True)
class MetricFlowRequestId:
    """Uniquely identifies a request to the MF engine."""

    mf_rid: str


class MetricFlowQueryType(Enum):
    """An enum to designate what type of MetricFlow query it is."""

    METRIC = "metric"
    DIMENSION_VALUES = "dimension_values"


@dataclass(frozen=True)
class MetricFlowQueryRequest:
    """Encapsulates the parameters for a metric query.

    metric_names: Names of the metrics to query.
    metrics: Metric objects to query.
    group_by_names: Names of the dimensions and entities to query.
    group_by: Dimension or entity objects to query.
    limit: Limit the result to this many rows.
    time_constraint_start: Get data for the start of this time range.
    time_constraint_end: Get data for the end of this time range.
    where_constraint: A SQL string using group by names that can be used like a where clause on the output data.
    order_by_names: metric and group by names to order by. A "-" can be used to specify reverse order e.g. "-ds".
    order_by: metric, dimension, or entity objects to order by.
    output_table: If specified, output the result data to this table instead of a result dataframe.
    sql_optimization_level: The level of optimization for the generated SQL.
    query_type: Type of MetricFlow query.
    """

    request_id: MetricFlowRequestId
    metric_names: Optional[Sequence[str]] = None
    metrics: Optional[Sequence[MetricQueryParameter]] = None
    group_by_names: Optional[Sequence[str]] = None
    group_by: Optional[Tuple[GroupByParameter, ...]] = None
    limit: Optional[int] = None
    time_constraint_start: Optional[datetime.datetime] = None
    time_constraint_end: Optional[datetime.datetime] = None
    where_constraint: Optional[str] = None
    order_by_names: Optional[Sequence[str]] = None
    order_by: Optional[Sequence[OrderByQueryParameter]] = None
    output_table: Optional[str] = None
    sql_optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4
    query_type: MetricFlowQueryType = MetricFlowQueryType.METRIC

    @staticmethod
    def create_with_random_request_id(  # noqa: D
        metric_names: Optional[Sequence[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        group_by_names: Optional[Sequence[str]] = None,
        group_by: Optional[Tuple[GroupByParameter, ...]] = None,
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[str] = None,
        order_by_names: Optional[Sequence[str]] = None,
        order_by: Optional[Sequence[OrderByQueryParameter]] = None,
        output_table: Optional[str] = None,
        sql_optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4,
        query_type: MetricFlowQueryType = MetricFlowQueryType.METRIC,
    ) -> MetricFlowQueryRequest:
        assert_exactly_one_arg_set(metric_names=metric_names, metrics=metrics)
        assert not (
            group_by_names and group_by
        ), "Both group_by_names and group_by were set, but if a group by is specified you should only use one of these!"
        assert not (
            order_by_names and order_by
        ), "Both order_by_names and order_by were set, but if an order by is specified you should only use one of these!"
        return MetricFlowQueryRequest(
            request_id=MetricFlowRequestId(mf_rid=f"{random_id()}"),
            metric_names=metric_names,
            metrics=metrics,
            group_by_names=group_by_names,
            group_by=group_by,
            limit=limit,
            time_constraint_start=time_constraint_start,
            time_constraint_end=time_constraint_end,
            where_constraint=where_constraint,
            order_by_names=order_by_names,
            order_by=order_by,
            output_table=output_table,
            sql_optimization_level=sql_optimization_level,
            query_type=query_type,
        )


@dataclass(frozen=True)
class MetricFlowQueryResult:  # noqa: D
    """The result of a query and context on how it was generated."""

    query_spec: MetricFlowQuerySpec
    dataflow_plan: DataflowPlan
    sql: str
    result_df: Optional[pd.DataFrame] = None
    result_table: Optional[SqlTable] = None


@dataclass(frozen=True)
class MetricFlowExplainResult:
    """Returns plans for resolving a query."""

    query_spec: MetricFlowQuerySpec
    dataflow_plan: DataflowPlan
    execution_plan: ExecutionPlan
    output_table: Optional[SqlTable] = None

    @property
    def rendered_sql(self) -> SqlQuery:
        """Return the SQL query that would be run for the given query."""
        if len(self.execution_plan.tasks) != 1:
            raise NotImplementedError(
                f"Multiple tasks in the execution plan not yet supported. Got tasks: {self.execution_plan.tasks}"
            )

        sql_query = self.execution_plan.tasks[0].sql_query
        if not sql_query:
            raise NotImplementedError(
                f"Execution plan tasks without a SQL query not yet supported. Got tasks: {self.execution_plan.tasks}"
            )

        return sql_query

    @property
    def rendered_sql_without_descriptions(self) -> SqlQuery:
        """Return the SQL query without the inline descriptions."""
        sql_query = self.rendered_sql
        return SqlQuery(
            sql_query="\n".join(
                filter(
                    lambda line: not line.strip().startswith("--"),
                    sql_query.sql_query.split("\n"),
                )
            ),
            bind_parameters=sql_query.bind_parameters,
        )


class AbstractMetricFlowEngine(ABC):
    """Query interface for clients."""

    @abstractmethod
    def query(
        self,
        mf_request: MetricFlowQueryRequest,
    ) -> MetricFlowQueryResult:
        """Query for metrics."""
        pass

    @abstractmethod
    def explain(
        self,
        mf_request: MetricFlowQueryRequest,
    ) -> MetricFlowExplainResult:
        """Similar to query - returns the query that would have been executed."""
        pass

    @abstractmethod
    def simple_dimensions_for_metrics(
        self, metric_names: List[str], without_any_property: Sequence[LinkableElementProperties]
    ) -> List[Dimension]:
        """Retrieves a list of all common dimensions for metric_names.

        "simple" dimensions are the ones that people expect from a UI perspective. For example, if "ds" is a time
        dimension at a day granularity, this would not list "ds__week".

        Args:
            metric_names: Names of metrics to get common dimensions from.
            without_any_property: Return dimensions not matching these properties.

        Returns:
            A list of Dimension objects containing metadata.
        """
        pass

    @abstractmethod
    def entities_for_metrics(self, metric_names: List[str]) -> List[Entity]:
        """Retrieves a list of all entities for metric_names.

        Args:
            metric_names: Names of metrics to get common entities from.

        Returns:
            A list of Entity objects containing metadata.
        """
        pass

    @abstractmethod
    def list_metrics(self) -> List[Metric]:
        """Retrieves a list of metric names.

        Returns:
            A list of Metric objects containing metadata.
        """
        pass

    @abstractmethod
    def get_dimension_values(
        self,
        metric_names: List[str],
        get_group_by_values: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> List[str]:
        """Retrieves a list of dimension values given a [metric_name, get_group_by_values].

        Args:
            metric_name: Names of metrics that contain the group_by.
            get_group_by_values: Name of group_by to get values from.
            time_constraint_start: Get data for the start of this time range.
            time_constraint_end: Get data for the end of this time range.

        Returns:
            A list of dimension values as string.
        """
        pass

    @abstractmethod
    def explain_get_dimension_values(  # noqa: D
        self,
        metric_names: Optional[List[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        get_group_by_values: Optional[str] = None,
        group_by: Optional[GroupByParameter] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> MetricFlowExplainResult:
        """Returns the SQL query for get_dimension_values.

        Args:
            metric_name: Names of metrics that contain the group_by.
            get_group_by_values: Name of group_by to get values from.
            time_constraint_start: Get data for the start of this time range.
            time_constraint_end: Get data for the end of this time range.

        Returns:
            An object with the rendered SQL and generated plans.
        """
        pass


class MetricFlowEngine(AbstractMetricFlowEngine):
    """Main entry point for queries.

    Attributes on this class should be treated as in use by our APIs.
    TODO: provide a more stable API layer instead of assuming this class is stable.
    """

    def __init__(
        self,
        semantic_manifest_lookup: SemanticManifestLookup,
        sql_client: SqlClient,
        time_source: TimeSource = ServerTimeSource(),
        column_association_resolver: Optional[ColumnAssociationResolver] = None,
    ) -> None:
        """Initializer for MetricFlowEngine.

        For direct calls to construct MetricFlowEngine, do not pass the following parameters,
        - time_source
        - column_association_resolver
        - time_spine_source
        These parameters are mainly there to be overridden during tests.
        """
        self._semantic_manifest_lookup = semantic_manifest_lookup
        self._sql_client = sql_client
        self._column_association_resolver = column_association_resolver or (
            DunderColumnAssociationResolver(semantic_manifest_lookup)
        )
        self._time_source = time_source
        self._time_spine_source = semantic_manifest_lookup.time_spine_source

        self._source_data_sets: List[SemanticModelDataSet] = []
        converter = SemanticModelToDataSetConverter(column_association_resolver=self._column_association_resolver)
        for semantic_model in sorted(
            self._semantic_manifest_lookup.semantic_manifest.semantic_models, key=lambda model: model.name
        ):
            data_set = converter.create_sql_source_data_set(semantic_model)
            self._source_data_sets.append(data_set)
            logger.info(f"Created source dataset from semantic model '{semantic_model.name}'")

        source_node_builder = SourceNodeBuilder(self._semantic_manifest_lookup)
        source_nodes = source_node_builder.create_from_data_sets(self._source_data_sets)

        node_output_resolver = DataflowPlanNodeOutputDataSetResolver(
            column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup),
            semantic_manifest_lookup=semantic_manifest_lookup,
        )

        self._dataflow_plan_builder = DataflowPlanBuilder(
            source_nodes=source_nodes,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )
        self._to_sql_query_plan_converter = DataflowToSqlQueryPlanConverter(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )
        self._to_execution_plan_converter = DataflowToExecutionPlanConverter(
            sql_plan_converter=self._to_sql_query_plan_converter,
            sql_plan_renderer=self._sql_client.sql_query_plan_renderer,
            sql_client=sql_client,
        )
        self._executor = SequentialPlanExecutor()

        self._query_parser = MetricFlowQueryParser(
            column_association_resolver=self._column_association_resolver,
            model=self._semantic_manifest_lookup,
            source_nodes=source_nodes,
            node_output_resolver=node_output_resolver,
        )

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def query(self, mf_request: MetricFlowQueryRequest) -> MetricFlowQueryResult:  # noqa: D
        logger.info(f"Starting query request:\n" f"{indent_log_line(pformat_big_objects(mf_request))}")
        explain_result = self._create_execution_plan(mf_request)
        execution_plan = explain_result.execution_plan

        if len(execution_plan.tasks) != 1:
            raise NotImplementedError("Multiple tasks not yet supported.")

        task = execution_plan.tasks[0]

        logger.info(f"Sequentially running tasks in:\n" f"{execution_plan_to_text(execution_plan)}")
        execution_results = self._executor.execute_plan(execution_plan)
        logger.info("Finished running tasks in execution plan")

        if execution_results.contains_task_errors:
            raise ExecutionException(f"Got errors while executing tasks:\n{execution_results.get_result(task.task_id)}")

        task_execution_result = execution_results.get_result(task.task_id)

        assert task_execution_result.sql, "Task execution should have returned SQL that was run"

        logger.info(f"Finished query request: {mf_request.request_id}")
        return MetricFlowQueryResult(
            query_spec=explain_result.query_spec,
            dataflow_plan=explain_result.dataflow_plan,
            sql=task_execution_result.sql,
            result_df=task_execution_result.df,
            result_table=explain_result.output_table,
        )

    @property
    def all_time_constraint(self) -> TimeRangeConstraint:
        """TimeRangeConstraint representing the min & max dates supported."""
        return TimeRangeConstraint.all_time()

    def _create_execution_plan(self, mf_query_request: MetricFlowQueryRequest) -> MetricFlowExplainResult:
        query_spec = self._query_parser.parse_and_validate_query(
            metric_names=mf_query_request.metric_names,
            metrics=mf_query_request.metrics,
            group_by_names=mf_query_request.group_by_names,
            group_by=mf_query_request.group_by,
            limit=mf_query_request.limit,
            time_constraint_start=mf_query_request.time_constraint_start,
            time_constraint_end=mf_query_request.time_constraint_end,
            where_constraint_str=mf_query_request.where_constraint,
            order_by_names=mf_query_request.order_by_names,
            order_by=mf_query_request.order_by,
        )
        logger.info(f"Query spec is:\n{pformat_big_objects(query_spec)}")

        if self._semantic_manifest_lookup.metric_lookup.contains_cumulative_or_time_offset_metric(
            tuple(m.as_reference for m in query_spec.metric_specs)
        ):
            if self._time_spine_source.time_column_granularity != TimeGranularity.DAY:
                raise RuntimeError(
                    f"A time spine source with a granularity {self._time_spine_source.time_column_granularity} is not "
                    f"yet supported."
                )
            logger.warning(
                f"Query spec requires a time spine dataset conforming to the following spec: {self._time_spine_source}. "
            )
            time_constraint_updated = False
            if not mf_query_request.time_constraint_start:
                time_constraint_start = self._time_source.get_time() - datetime.timedelta(days=365)
                logger.warning(
                    "A start time has not be supplied while querying for cumulative metrics. To avoid an excessive "
                    f"number of rows, the start time will be changed to {time_constraint_start.isoformat()}"
                )
                time_constraint_updated = True
            if not mf_query_request.time_constraint_end:
                time_constraint_end = self._time_source.get_time()
                logger.warning(
                    "A end time has not be supplied while querying for cumulative metrics. To avoid an excessive "
                    f"number of rows, the end time will be changed to {time_constraint_end.isoformat()}"
                )
                time_constraint_updated = True
            if time_constraint_updated:
                query_spec = self._query_parser.parse_and_validate_query(
                    metric_names=mf_query_request.metric_names,
                    group_by_names=mf_query_request.group_by_names,
                    group_by=mf_query_request.group_by,
                    limit=mf_query_request.limit,
                    time_constraint_start=mf_query_request.time_constraint_start,
                    time_constraint_end=mf_query_request.time_constraint_end,
                    where_constraint_str=mf_query_request.where_constraint,
                    order_by_names=mf_query_request.order_by_names,
                    order_by=mf_query_request.order_by,
                )
                logger.warning(f"Query spec updated to:\n{pformat_big_objects(query_spec)}")

        output_table: Optional[SqlTable] = None
        if mf_query_request.output_table is not None:
            output_table = SqlTable.from_string(mf_query_request.output_table)

        output_selection_specs: Optional[InstanceSpecSet] = None
        if mf_query_request.query_type == MetricFlowQueryType.DIMENSION_VALUES:
            # Filter result by dimension columns if it's a dimension values query
            if len(query_spec.entity_specs) > 0:
                raise InvalidQueryException("Querying dimension values for entities is not allowed.")
            output_selection_specs = InstanceSpecSet(
                dimension_specs=query_spec.dimension_specs,
                time_dimension_specs=query_spec.time_dimension_specs,
            )

        dataflow_plan = self._dataflow_plan_builder.build_plan(
            query_spec=query_spec,
            output_sql_table=output_table,
            output_selection_specs=output_selection_specs,
            optimizers=(SourceScanOptimizer(),),
        )

        if len(dataflow_plan.sink_output_nodes) > 1:
            raise NotImplementedError(
                f"Multiple output nodes in the dataflow plan not yet supported. "
                f"Got tasks: {dataflow_plan.sink_output_nodes}"
            )

        execution_plan = self._to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

        return MetricFlowExplainResult(
            query_spec=query_spec,
            dataflow_plan=dataflow_plan,
            execution_plan=execution_plan,
            output_table=output_table,
        )

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def explain(self, mf_request: MetricFlowQueryRequest) -> MetricFlowExplainResult:  # noqa: D
        return self._create_execution_plan(mf_request)

    def get_measures_for_metrics(self, metric_names: List[str]) -> List[Measure]:  # noqa: D
        metrics = self._semantic_manifest_lookup.metric_lookup.get_metrics(
            metric_references=[MetricReference(element_name=metric_name) for metric_name in metric_names]
        )
        semantic_model_lookup = self._semantic_manifest_lookup.semantic_model_lookup

        measures = set()
        for metric in metrics:
            for input_measure in metric.input_measures:
                measure_reference = MeasureReference(element_name=input_measure.name)
                # populate new obj
                measure = semantic_model_lookup.get_measure(measure_reference=measure_reference)
                measures.add(
                    Measure(
                        name=measure.name,
                        agg=measure.agg,
                        agg_time_dimension=semantic_model_lookup.get_agg_time_dimension_for_measure(
                            measure_reference=measure_reference
                        ).element_name,
                        description=measure.description,
                        expr=measure.expr,
                        agg_params=measure.agg_params,
                    )
                )
        return list(measures)

    def simple_dimensions_for_metrics(  # noqa: D
        self,
        metric_names: List[str],
        without_any_property: Sequence[LinkableElementProperties] = (
            LinkableElementProperties.ENTITY,
            LinkableElementProperties.DERIVED_TIME_GRANULARITY,
            LinkableElementProperties.LOCAL_LINKED,
        ),
    ) -> List[Dimension]:
        path_key_to_linkable_dimensions = (
            self._semantic_manifest_lookup.metric_lookup.linkable_set_for_metrics(
                metric_references=[MetricReference(element_name=mname) for mname in metric_names],
                without_any_property=frozenset(without_any_property),
            )
        ).path_key_to_linkable_dimensions

        dimensions: List[Dimension] = []
        linkable_dimensions_tuple: Tuple[LinkableDimension, ...]
        for (
            path_key,
            linkable_dimensions_tuple,
        ) in path_key_to_linkable_dimensions.items():
            for linkable_dimension in linkable_dimensions_tuple:
                semantic_model = self._semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
                    linkable_dimension.semantic_model_origin
                )
                assert semantic_model

                if LinkableElementProperties.METRIC_TIME in linkable_dimension.properties:
                    metric_time_name = DataSet.metric_time_dimension_name()
                    assert linkable_dimension.element_name == metric_time_name, (
                        f"{linkable_dimension} has the {LinkableElementProperties.METRIC_TIME}, but the name does not"
                        f"match."
                    )

                    dimensions.append(
                        Dimension(
                            name=metric_time_name,
                            qualified_name=StructuredLinkableSpecName(
                                element_name=metric_time_name,
                                entity_link_names=tuple(
                                    entity_reference.element_name
                                    for entity_reference in linkable_dimension.entity_links
                                ),
                                time_granularity=linkable_dimension.time_granularity,
                            ).qualified_name,
                            description="Event time for metrics.",
                            metadata=None,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=linkable_dimension.time_granularity,
                                validity_params=None,
                            ),
                            is_partition=False,
                            type=DimensionType.TIME,
                        )
                    )
                else:
                    dimensions.append(
                        Dimension.from_pydantic(
                            pydantic_dimension=SemanticModelLookup.get_dimension_from_semantic_model(
                                semantic_model=semantic_model,
                                dimension_reference=linkable_dimension.reference,
                            ),
                            path_key=path_key,
                        )
                    )
        return sorted(dimensions, key=lambda dimension: dimension.qualified_name)

    def entities_for_metrics(self, metric_names: List[str]) -> List[Entity]:  # noqa: D
        path_key_to_linkable_entities = (
            self._semantic_manifest_lookup.metric_lookup.linkable_set_for_metrics(
                metric_references=[MetricReference(element_name=mname) for mname in metric_names],
                with_any_property=frozenset(
                    {
                        LinkableElementProperties.ENTITY,
                    }
                ),
            )
        ).path_key_to_linkable_entities

        entities: List[Entity] = []
        for (
            path_key,
            linkable_entity_tuple,
        ) in path_key_to_linkable_entities.items():
            for linkable_entity in linkable_entity_tuple:
                semantic_model = self._semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
                    linkable_entity.semantic_model_origin
                )
                assert semantic_model
                entities.append(
                    Entity.from_pydantic(
                        pydantic_entity=SemanticModelLookup.get_entity_from_semantic_model(
                            semantic_model=semantic_model,
                            entity_reference=EntityReference(element_name=linkable_entity.element_name),
                        )
                    )
                )
        return entities

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_metrics(self) -> List[Metric]:  # noqa: D
        metric_references = self._semantic_manifest_lookup.metric_lookup.metric_references
        metrics = self._semantic_manifest_lookup.metric_lookup.get_metrics(metric_references)
        return [
            Metric.from_pydantic(
                pydantic_metric=metric,
                dimensions=self.simple_dimensions_for_metrics([metric.name]),
            )
            for metric in metrics
        ]

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def get_dimension_values(  # noqa: D
        self,
        metric_names: List[str],
        get_group_by_values: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> List[str]:
        # Run query
        query_result = self.query(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=metric_names,
                group_by_names=[get_group_by_values],
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
                query_type=MetricFlowQueryType.DIMENSION_VALUES,
            )
        )
        result_dataframe = query_result.result_df
        if result_dataframe is None:
            return []

        # Snowflake likes upper-casing things in result output, so we lower-case all names
        # before operating on the dataframe.
        metric_names = [metric_name.lower() for metric_name in metric_names]
        result_dataframe.columns = result_dataframe.columns.str.lower()

        # Get dimension values regardless of input name -> output dimension mapping. This is necessary befcause
        # granularity adjustments on time dimensions produce different output names for dimension values.
        # Note: this only works as long as we have exactly one column of group by values
        # and no other extraneous output columns
        dim_vals = result_dataframe[result_dataframe.columns[~result_dataframe.columns.isin(metric_names)]].iloc[:, 0]

        return sorted([str(val) for val in dim_vals])

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def explain_get_dimension_values(  # noqa: D
        self,
        metric_names: Optional[List[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        get_group_by_values: Optional[str] = None,
        group_by: Optional[GroupByParameter] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> MetricFlowExplainResult:
        assert not (
            get_group_by_values and group_by
        ), "Both get_group_by_values and group_by were set, but if a group by is specified you should only use one of these!"
        return self._create_execution_plan(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=metric_names,
                metrics=metrics,
                group_by_names=(get_group_by_values,) if get_group_by_values else None,
                group_by=(group_by,) if group_by else None,
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
                query_type=MetricFlowQueryType.DIMENSION_VALUES,
            )
        )
