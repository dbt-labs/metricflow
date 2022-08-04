from __future__ import annotations

import datetime
import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Sequence

import pandas as pd

from metricflow.configuration.constants import CONFIG_DWH_SCHEMA
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import DataflowPlan
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.convert_data_source import DataSourceToDataSetConverter
from metricflow.engine.models import Dimension, Materialization, Metric
from metricflow.engine.time_source import ServerTimeSource
from metricflow.engine.utils import build_user_configured_model_from_config
from metricflow.execution.execution_plan import ExecutionPlan, SqlQuery
from metricflow.execution.execution_plan_to_text import execution_plan_to_text
from metricflow.execution.executor import SequentialPlanExecutor
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.semantics.linkable_spec_resolver import LinkableElementProperties
from metricflow.object_utils import pformat_big_objects, random_id
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_execution import DataflowToExecutionPlanConverter
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.time_spine import TimeSpineSource, TimeSpineTableBuilder
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs import ColumnAssociationResolver
from metricflow.specs import MetricSpec, MetricFlowQuerySpec
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.time.time_source import TimeSource
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.errors.errors import ExecutionException, MaterializationNotFoundError
from metricflow.sql_clients.sql_utils import make_sql_client_from_config
from metricflow.sql_clients.common_client import not_empty
from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call

logger = logging.getLogger(__name__)
_telemetry_reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
_telemetry_reporter.add_python_log_handler()
_telemetry_reporter.add_rudderstack_handler()


@dataclass(frozen=True)
class MetricFlowRequestId:
    """Uniquely identifies a request to the MF engine."""

    mf_rid: str


@dataclass(frozen=True)
class MetricFlowQueryRequest:
    """Encapsulates the parameters for a metric query.

    metric_names: Names of the metrics to query.
    group_by_names: Names of the dimensions and identifiers to query.
    limit: Limit the result to this many rows.
    time_constraint_start: Get data for the start of this time range.
    time_constraint_end: Get data for the end of this time range.
    where_constraint: A SQL string using group by names that can be used like a where clause on the output data.
    order_by_names: metric and group by names to order by. A "-" can be used to specify reverse order e.g. "-ds"
    output_table: If specified, output the result data to this table instead of a result dataframe.
    sql_optimization_level: The level of optimization for the generated SQL.
    """

    request_id: MetricFlowRequestId
    metric_names: Sequence[str]
    group_by_names: Sequence[str]
    limit: Optional[int] = None
    time_constraint_start: Optional[datetime.datetime] = None
    time_constraint_end: Optional[datetime.datetime] = None
    where_constraint: Optional[str] = None
    order_by_names: Optional[Sequence[str]] = None
    output_table: Optional[str] = None
    sql_optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4

    @staticmethod
    def create_with_random_request_id(  # noqa: D
        metric_names: Sequence[str],
        group_by_names: Sequence[str],
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[str] = None,
        order_by_names: Optional[Sequence[str]] = None,
        output_table: Optional[str] = None,
        sql_optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4,
    ) -> MetricFlowQueryRequest:
        return MetricFlowQueryRequest(
            request_id=MetricFlowRequestId(mf_rid=f"{random_id()}"),
            metric_names=metric_names,
            group_by_names=group_by_names,
            limit=limit,
            time_constraint_start=time_constraint_start,
            time_constraint_end=time_constraint_end,
            where_constraint=where_constraint,
            order_by_names=order_by_names,
            output_table=output_table,
            sql_optimization_level=sql_optimization_level,
        )


@dataclass(frozen=True)
class MetricFlowQueryResult:  # noqa: D
    """The result of a query and context on how it was generated."""

    query_spec: MetricFlowQuerySpec
    dataflow_plan: DataflowPlan[DataSourceDataSet]
    sql: str
    result_df: Optional[pd.DataFrame] = None
    result_table: Optional[SqlTable] = None


@dataclass(frozen=True)
class MetricFlowExplainResult:
    """Returns plans for resolving a query."""

    query_spec: MetricFlowQuerySpec
    dataflow_plan: DataflowPlan[DataSourceDataSet]
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
                filter(lambda line: not line.strip().startswith("--"), sql_query.sql_query.split("\n"))
            ),
            bind_parameters=sql_query.bind_parameters,
        )


class AbstractMetricFlowEngine(ABC):
    """Query interface for clients"""

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
    def simple_dimensions_for_metrics(self, metric_names: List[str]) -> List[Dimension]:
        """Retrieves a list of all common dimensions for metric_names.

        "simple" dimensions are the ones that people expect from a UI perspective. For example, if "ds" is a time
        dimension at a day granularity, this would not list "ds__week".

        Args:
            metric_names: Names of metrics to get common dimensions from.

        Returns:
            A list of Dimension objects containing metadata.
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
        metric_name: str,
        get_group_by_values: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> List[str]:
        """Retrieves a list of dimension values given a [metric_name, get_group_by_values].

        Args:
            metric_name: Name of metric that contains the group_by.
            get_group_by_values: Name of group_by to get values from.
            time_constraint_start: Get data for the start of this time range.
            time_constraint_end: Get data for the end of this time range.

        Returns:
            A list of dimension values as string.
        """
        pass

    @abstractmethod
    def list_materializations(self) -> List[Materialization]:
        """Retrieves a list of materialization names.

        Returns:
            A list of Materialization objects containing metadata.
        """
        pass

    @abstractmethod
    def materialize(
        self,
        materialization_name: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> SqlTable:
        """Builds a table containing metrics and dimensions from a materialization definition.

        This can be very expensive if a large time range is provided.

        Args:
            materialization_name: Name of materialization
            time_constraint_start: Materialized for the start of this time range.
            time_constraint_end: Materialized for the end of this time range.

        Returns:
            SqlTable object of the materialized table.
        """
        pass

    @abstractmethod
    def drop_materialization(self, materialization_name: str) -> bool:
        """Drops the table associated with a materialization definition.

        Args:
            materialization_name: Name of materialization to drop.

        Returns:
            True if a table has been drop, False if table doesn't exist.
        """
        pass


class MetricFlowEngine(AbstractMetricFlowEngine):
    """Main entry point for queries."""

    _LOGGING_INDENT = "   "

    @staticmethod
    def from_config(handler: YamlFileHandler) -> MetricFlowEngine:
        """Initialize MetricFlowEngine via yaml config file."""
        sql_client = make_sql_client_from_config(handler)
        semantic_model = SemanticModel(build_user_configured_model_from_config(handler))
        system_schema = not_empty(handler.get_value(CONFIG_DWH_SCHEMA), CONFIG_DWH_SCHEMA, handler.url)
        return MetricFlowEngine(
            semantic_model=semantic_model,
            sql_client=sql_client,
            system_schema=system_schema,
        )

    def __init__(
        self,
        semantic_model: SemanticModel,
        sql_client: SqlClient,
        system_schema: str,
        time_source: TimeSource = ServerTimeSource(),
        column_association_resolver: Optional[ColumnAssociationResolver] = None,
        time_spine_source: Optional[TimeSpineSource] = None,
    ) -> None:
        """Initializer for MetricFlowEngine

        For direct calls to construct MetricFlowEngine, do not pass the following parameters,
        - time_source
        - column_association_resolver
        - time_spine_source
        These parameters are mainly there to be overridden during tests.
        """

        self._semantic_model = semantic_model
        self._sql_client = sql_client
        self._column_association_resolver = column_association_resolver or (
            DefaultColumnAssociationResolver(semantic_model)
        )
        self._time_source = time_source
        self._time_spine_source = time_spine_source or TimeSpineSource(schema_name=system_schema)
        self._time_spine_table_builder = TimeSpineTableBuilder(
            time_spine_source=self._time_spine_source, sql_client=self._sql_client
        )

        self._schema = system_schema

        self._source_data_sets: List[DataSourceDataSet] = []
        converter = DataSourceToDataSetConverter(column_association_resolver=self._column_association_resolver)
        for data_source in self._semantic_model.user_configured_model.data_sources:
            data_set = converter.create_sql_source_data_set(data_source)
            self._source_data_sets.append(data_set)
            logger.info(f"Created source dataset from data source '{data_source.name}'")

        source_node_builder = SourceNodeBuilder(self._semantic_model)
        source_nodes = source_node_builder.create_from_data_sets(self._source_data_sets)

        node_output_resolver = DataflowPlanNodeOutputDataSetResolver[DataSourceDataSet](
            column_association_resolver=DefaultColumnAssociationResolver(semantic_model),
            semantic_model=semantic_model,
            time_spine_source=self._time_spine_source,
        )

        self._dataflow_plan_builder = DataflowPlanBuilder[DataSourceDataSet](
            source_nodes=source_nodes,
            semantic_model=self._semantic_model,
            time_spine_source=self._time_spine_source,
        )
        self._to_sql_query_plan_converter = DataflowToSqlQueryPlanConverter[DataSourceDataSet](
            column_association_resolver=self._column_association_resolver,
            semantic_model=self._semantic_model,
            time_spine_source=self._time_spine_source,
        )
        self._to_execution_plan_converter = DataflowToExecutionPlanConverter[DataSourceDataSet](
            sql_plan_converter=self._to_sql_query_plan_converter,
            sql_plan_renderer=self._sql_client.sql_engine_attributes.sql_query_plan_renderer,
            sql_client=sql_client,
        )
        self._executor = SequentialPlanExecutor()

        self._query_parser = MetricFlowQueryParser(
            model=self._semantic_model,
            source_nodes=source_nodes,
            node_output_resolver=node_output_resolver,
        )

    def _get_materialization_by_name(self, materialization_name: str) -> Optional[Materialization]:
        materializations = self.list_materializations()
        for mat in materializations:
            if mat.name == materialization_name:
                return mat
        return None

    def _generate_sql_table(self, table_name: str) -> SqlTable:
        return SqlTable.from_string(f"{self._schema}.{table_name}")

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def query(self, mf_request: MetricFlowQueryRequest) -> MetricFlowQueryResult:  # noqa: D
        logger.info(
            f"Starting query request:\n"
            f"{textwrap.indent(pformat_big_objects(mf_request), prefix=MetricFlowEngine._LOGGING_INDENT)}"
        )
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

    def _create_execution_plan(self, mf_query_request: MetricFlowQueryRequest) -> MetricFlowExplainResult:
        query_spec = self._query_parser.parse_and_validate_query(
            metric_names=mf_query_request.metric_names,
            group_by_names=mf_query_request.group_by_names,
            limit=mf_query_request.limit,
            time_constraint_start=mf_query_request.time_constraint_start,
            time_constraint_end=mf_query_request.time_constraint_end,
            where_constraint_str=mf_query_request.where_constraint,
            order=mf_query_request.order_by_names,
        )
        logger.info(f"Query spec is:\n{pformat_big_objects(query_spec)}")

        if self._semantic_model.metric_semantics.contains_cumulative_metric(query_spec.metric_specs):
            self._time_spine_table_builder.create_if_necessary()
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
                    limit=mf_query_request.limit,
                    time_constraint_start=mf_query_request.time_constraint_start,
                    time_constraint_end=mf_query_request.time_constraint_end,
                    where_constraint_str=mf_query_request.where_constraint,
                    order=mf_query_request.order_by_names,
                )
                logger.warning(f"Query spec updated to:\n{pformat_big_objects(query_spec)}")

        output_table: Optional[SqlTable] = None
        if mf_query_request.output_table is not None:
            output_table = SqlTable.from_string(mf_query_request.output_table)

        dataflow_plan = self._dataflow_plan_builder.build_plan(query_spec, output_table)

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

    def simple_dimensions_for_metrics(self, metric_names: List[str]) -> List[Dimension]:  # noqa: D
        return [
            Dimension(name=dim.qualified_name)
            for dim in self._semantic_model.metric_semantics.element_specs_for_metrics(
                metric_specs=[MetricSpec(element_name=mname) for mname in metric_names],
                without_any_property=frozenset(
                    {
                        LinkableElementProperties.IDENTIFIER,
                        LinkableElementProperties.DERIVED_TIME_GRANULARITY,
                        LinkableElementProperties.LOCAL_LINKED,
                    }
                ),
            )
        ]

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_materializations(self) -> List[Materialization]:  # noqa: D
        return [
            Materialization(
                name=mat.name, metrics=mat.metrics, dimensions=mat.dimensions, destination_table=mat.destination_table
            )
            for mat in self._semantic_model.user_configured_model.materializations
        ]

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_metrics(self) -> List[Metric]:  # noqa: D
        metric_specs = self._semantic_model.metric_semantics.metric_names
        metrics = self._semantic_model.metric_semantics.get_metrics(metric_specs)
        return [
            Metric(
                name=metric.name,
                dimensions=self.simple_dimensions_for_metrics([metric.name]),
            )
            for metric in metrics
        ]

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def get_dimension_values(  # noqa: D
        self,
        metric_name: str,
        get_group_by_values: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> List[str]:

        # Run query
        query_result = self.query(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=[metric_name],
                group_by_names=[get_group_by_values],
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
            )
        )
        result_dataframe = query_result.result_df
        if result_dataframe is None:
            return []

        # Process the dimension values
        result_dataframe.dropna(inplace=True)
        dimension_values = [str(val) for val in result_dataframe[get_group_by_values]]
        return dimension_values

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def materialize(  # noqa: D
        self,
        materialization_name: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> SqlTable:
        materialization = self._get_materialization_by_name(materialization_name)
        if materialization is None:
            raise MaterializationNotFoundError(
                f"Unable to find materialization `{materialization_name}`. Perhaps it has not been registered"
            )

        # Use destination_table if exists else materialization_name
        output_table = materialization.destination_table or self._generate_sql_table(materialization_name)
        self._sql_client.drop_table(output_table)

        # Executes the query with output_table
        query_result = self.query(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=materialization.metrics,
                group_by_names=materialization.dimensions,
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
                output_table=output_table.sql,
            )
        )
        assert query_result.result_table
        return query_result.result_table

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def drop_materialization(self, materialization_name: str) -> bool:  # noqa: D
        materialization = self._get_materialization_by_name(materialization_name)
        if materialization is None:
            raise MaterializationNotFoundError(
                f"Unable to find materialization `{materialization_name}`. Perhaps it has not been registered"
            )

        table = materialization.destination_table or self._generate_sql_table(materialization_name)

        if self._sql_client.table_exists(table):
            self._sql_client.drop_table(table)
            return True
        return False
