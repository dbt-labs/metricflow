from __future__ import annotations

import datetime
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimensionTypeParams
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MetricReference,
    SemanticModelElementReference,
)
from dbt_semantic_interfaces.type_enums import DimensionType
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.errors.error_classes import (
    ExecutionException,
    InvalidQueryException,
    MetricFlowInternalError,
    UnknownMetricError,
)
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseGroupByItemSet
from metricflow_semantics.model.semantics.semantic_model_helper import SemanticModelHelper
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.protocols.query_parameter import (
    GroupByQueryParameter,
    MetricQueryParameter,
    OrderByQueryParameter,
)
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.query_param_implementations import SavedQueryParameter
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_source import TimeSource
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from metricflow_semantics.toolkit.id_helpers import mf_random_id
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.runtime import log_block_runtime
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item
from typing_extensions import TypeVar

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.dataflow.builder.builder_cache import DataflowPlanBuilderCache
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import DataflowPlan
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.dataset_classes import DataSet
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.models import Dimension, Entity, Metric, SavedQuery, SearchableElement
from metricflow.engine.time_source import ServerTimeSource
from metricflow.execution.convert_to_execution_plan import ConvertToExecutionPlanResult
from metricflow.execution.dataflow_to_execution import (
    DataflowToExecutionPlanConverter,
)
from metricflow.execution.execution_plan import ExecutionPlan, SqlStatement
from metricflow.execution.executor import SequentialPlanExecutor
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call

logger = logging.getLogger(__name__)
_telemetry_reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
_telemetry_reporter.add_python_log_handler()

SIMPLE_DIMENSIONS_WITHOUT_ANY_PROPERTIES: Set[GroupByItemProperty] = {
    GroupByItemProperty.ENTITY,
    GroupByItemProperty.DERIVED_TIME_GRANULARITY,
    GroupByItemProperty.DATE_PART,
    GroupByItemProperty.LOCAL_LINKED,
    GroupByItemProperty.METRIC,
}
ENTITY_WITH_ANY_PROPERTIES: Set[GroupByItemProperty] = {GroupByItemProperty.ENTITY}

SearchableElementGeneric = TypeVar("SearchableElementGeneric", bound=SearchableElement)


class GroupByOrderByAttribute(Enum):
    """Supported dimension attributes to order them by."""

    SEMANTIC_MODEL_NAME = "semantic_model_name"
    DUNDER_NAME = "dunder_name"


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

    TODO: This has turned into a bag of parameters that make it difficult to use without a bunch of conditionals.

    metric_names: Names of the metrics to query.
    metrics: Metric objects to query.
    group_by_names: Names of the dimensions and entities to query.
    group_by: Dimension or entity objects to query.
    limit: Limit the result to this many rows.
    time_constraint_start: Get data for the start of this time range.
    time_constraint_end: Get data for the end of this time range.
    where_constraints: A sequence of SQL strings that can be used like a where clause on the output data.
    order_by_names: metric and group by names to order by. A "-" can be used to specify reverse order e.g. "-ds".
    order_by: metric, dimension, or entity objects to order by.
    output_table: If specified, output the result data to this table instead of a result data_table.
    sql_optimization_level: The level of optimization for the generated SQL.
    query_type: Type of MetricFlow query.
    order_output_columns_by_input_order: The columns in the output are arranged in groups as described in
    `CreateSelectColumnsForInstances`. If this is set to True, the order of the columns in each group follows the order
    as described by inputs (e.g. `metric_names`).
    """

    request_id: MetricFlowRequestId
    saved_query_name: Optional[str]
    metric_names: Optional[Sequence[str]]
    metrics: Optional[Sequence[MetricQueryParameter]]
    group_by_names: Optional[Sequence[str]]
    group_by: Optional[Tuple[GroupByQueryParameter, ...]]
    limit: Optional[int]
    time_constraint_start: Optional[datetime.datetime]
    time_constraint_end: Optional[datetime.datetime]
    where_constraints: Optional[Sequence[str]]
    order_by_names: Optional[Sequence[str]]
    order_by: Optional[Sequence[OrderByQueryParameter]]
    min_max_only: bool
    apply_group_by: bool
    sql_optimization_level: SqlOptimizationLevel
    dataflow_plan_optimizations: FrozenSet[DataflowPlanOptimization]
    query_type: MetricFlowQueryType
    order_output_columns_by_input_order: bool

    @staticmethod
    def create_with_random_request_id(  # noqa: D102
        saved_query_name: Optional[str] = None,
        metric_names: Optional[Sequence[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        group_by_names: Optional[Sequence[str]] = None,
        group_by: Optional[Sequence[GroupByQueryParameter]] = None,
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraints: Optional[Sequence[str]] = None,
        order_by_names: Optional[Sequence[str]] = None,
        order_by: Optional[Sequence[OrderByQueryParameter]] = None,
        sql_optimization_level: SqlOptimizationLevel = SqlOptimizationLevel.default_level(),
        dataflow_plan_optimizations: FrozenSet[
            DataflowPlanOptimization
        ] = DataflowPlanOptimization.enabled_optimizations(),
        query_type: MetricFlowQueryType = MetricFlowQueryType.METRIC,
        min_max_only: bool = False,
        apply_group_by: bool = True,
        order_output_columns_by_input_order: bool = False,
    ) -> MetricFlowQueryRequest:
        return MetricFlowQueryRequest(
            request_id=MetricFlowRequestId(mf_rid=f"{mf_random_id()}"),
            saved_query_name=saved_query_name,
            metric_names=metric_names,
            metrics=metrics,
            group_by_names=group_by_names,
            group_by=tuple(group_by) if group_by is not None else None,
            limit=limit,
            time_constraint_start=time_constraint_start,
            time_constraint_end=time_constraint_end,
            where_constraints=where_constraints,
            order_by_names=order_by_names,
            order_by=order_by,
            sql_optimization_level=sql_optimization_level,
            dataflow_plan_optimizations=dataflow_plan_optimizations,
            query_type=query_type,
            min_max_only=min_max_only,
            apply_group_by=apply_group_by,
            order_output_columns_by_input_order=order_output_columns_by_input_order,
        )

    def with_request_id(self, request_id: MetricFlowRequestId) -> MetricFlowQueryRequest:  # noqa: D102
        return MetricFlowQueryRequest(
            request_id=request_id,
            saved_query_name=self.saved_query_name,
            metric_names=self.metric_names,
            metrics=self.metrics,
            group_by_names=self.group_by_names,
            group_by=self.group_by,
            limit=self.limit,
            time_constraint_start=self.time_constraint_start,
            time_constraint_end=self.time_constraint_end,
            where_constraints=self.where_constraints,
            order_by_names=self.order_by_names,
            order_by=self.order_by,
            min_max_only=self.min_max_only,
            apply_group_by=self.apply_group_by,
            sql_optimization_level=self.sql_optimization_level,
            dataflow_plan_optimizations=self.dataflow_plan_optimizations,
            query_type=self.query_type,
            order_output_columns_by_input_order=self.order_output_columns_by_input_order,
        )


@dataclass(frozen=True)
class MetricFlowQueryResult:
    """The result of a query and context on how it was generated."""

    query_spec: MetricFlowQuerySpec
    dataflow_plan: DataflowPlan
    sql: str
    result_df: Optional[MetricFlowDataTable] = None
    result_table: Optional[SqlTable] = None


@dataclass(frozen=True)
class MetricFlowExplainResult:
    """Returns plans for resolving a query."""

    query_spec: MetricFlowQuerySpec
    dataflow_plan: DataflowPlan
    convert_to_execution_plan_result: ConvertToExecutionPlanResult
    output_table: Optional[SqlTable] = None

    @property
    def sql_statement(self) -> SqlStatement:
        """Return the SQL query that would be run for the given query."""
        execution_plan = self.execution_plan
        if len(execution_plan.tasks) != 1:
            raise NotImplementedError(
                LazyFormat(
                    "Multiple tasks in the execution plan not yet supported.",
                    tasks=[task.task_id for task in execution_plan.tasks],
                )
            )

        sql_statement = execution_plan.tasks[0].sql_statement
        if not sql_statement:
            raise NotImplementedError(
                LazyFormat(
                    "Execution plan tasks without a SQL statement are not yet supported.",
                    tasks=[task.task_id for task in execution_plan.tasks],
                )
            )

        return sql_statement

    @property
    def execution_plan(self) -> ExecutionPlan:  # noqa: D102
        return self.convert_to_execution_plan_result.execution_plan


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
        self,
        metric_names: List[str],
        without_any_property: Sequence[GroupByItemProperty],
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
    def list_metrics(self, include_dimensions: bool = True) -> List[Metric]:
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
            metric_names: Names of metrics that contain the group_by.
            get_group_by_values: Name of group_by to get values from.
            time_constraint_start: Get data for the start of this time range.
            time_constraint_end: Get data for the end of this time range.

        Returns:
            A list of dimension values as string.
        """
        pass

    @abstractmethod
    def explain_get_dimension_values(
        self,
        metric_names: Optional[List[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        get_group_by_values: Optional[str] = None,
        group_by: Optional[GroupByQueryParameter] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> MetricFlowExplainResult:
        """Returns the SQL query for get_dimension_values.

        Args:
            metric_names: Names of metrics that contain the group_by.
            metrics: Similar to `metric_names`, but specified via parameter objects.
            get_group_by_values: Name of group_by to get values from.
            group_by: Similar to `get_group_by_values`, but specified via parameter objects.
            time_constraint_start: Get data for the start of this time range.
            time_constraint_end: Get data for the end of this time range.

        Returns:
            An object with the rendered SQL and generated plans.
        """
        pass

    @abstractmethod
    def list_dimensions(self, metric_names: Optional[List[str]] = None) -> List[Dimension]:
        """List all dimensions in the semantic manifest, with optional filters."""
        pass

    @abstractmethod
    def list_group_bys(
        self,
        metric_names: Optional[List[str]] = None,
        include_derived_time_granularities: bool = False,
        order_by: GroupByOrderByAttribute = GroupByOrderByAttribute.DUNDER_NAME,
    ) -> List[Entity | Dimension]:
        """List all group bys in the semantic manifest, with optional filters."""
        pass

    @abstractmethod
    def list_saved_queries(self) -> List[SavedQuery]:
        """List all saved queries in the semantic manifest, with optional filters."""
        pass


class MetricFlowEngine(AbstractMetricFlowEngine):
    """Main entry point for queries.

    Attributes on this class should be treated as in use by our APIs.
    TODO: provide a more stable API layer instead of assuming this class is stable.
    """

    # When generating IDs in the initializer, start from this value.
    _ID_ENUMERATION_START_VALUE_FOR_INITIALIZER = 10000
    # When generating IDs in queries, start from this value.
    _ID_ENUMERATION_START_VALUE_FOR_QUERIES = 0

    def __init__(
        self,
        semantic_manifest_lookup: SemanticManifestLookup,
        sql_client: SqlClient,
        time_source: TimeSource = ServerTimeSource(),
        query_parser: Optional[MetricFlowQueryParser] = None,
        column_association_resolver: Optional[ColumnAssociationResolver] = None,
        consistent_id_enumeration: Optional[bool] = True,
    ) -> None:
        """Initializer for MetricFlowEngine.

        consistent_id_enumeration can be set to True to reset the numbering of sequentially generated IDs on each query. This
        will help generate consistent SQL between queries as aliases will be the same.

        For direct calls to construct MetricFlowEngine, do not pass the following parameters,
        - time_source
        - column_association_resolver
        - time_spine_source

        These parameters are mainly there to be overridden during tests.
        """
        self._reset_id_enumeration = consistent_id_enumeration
        if self._reset_id_enumeration:
            # Some of the objects that are created below use generated IDs. To avoid collision with IDs that are
            # generated for queries, set the ID generation numbering to start at a high enough number.
            logger.debug(
                LazyFormat(
                    lambda: f"For creating setup objects, setting numbering of generated IDs to start at: "
                    f"{MetricFlowEngine._ID_ENUMERATION_START_VALUE_FOR_INITIALIZER}"
                )
            )
            SequentialIdGenerator.reset(MetricFlowEngine._ID_ENUMERATION_START_VALUE_FOR_INITIALIZER)
        self._semantic_manifest_lookup = semantic_manifest_lookup
        self._sql_client = sql_client
        self._column_association_resolver = column_association_resolver or (DunderColumnAssociationResolver())
        self._time_source = time_source
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(
            semantic_manifest_lookup.semantic_manifest
        )
        self._source_data_sets: List[SemanticModelDataSet] = []
        converter = SemanticModelToDataSetConverter(
            column_association_resolver=self._column_association_resolver,
            manifest_lookup=self._semantic_manifest_lookup,
        )
        model_lookup = semantic_manifest_lookup.semantic_model_lookup
        for model_reference, semantic_model in model_lookup.model_reference_to_model.items():
            data_set = converter.create_sql_source_data_set(model_reference)
            self._source_data_sets.append(data_set)
            logger.debug(LazyFormat("Created source dataset from semantic model", model_reference=model_reference))

        source_node_builder = SourceNodeBuilder(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )
        source_node_set = source_node_builder.create_from_data_sets(self._source_data_sets)

        node_output_resolver = DataflowNodeToSqlSubqueryVisitor(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )
        node_output_resolver.cache_output_data_sets(source_node_set.all_nodes)

        self._dataflow_plan_builder_cache = DataflowPlanBuilderCache()
        self._dataflow_plan_builder = DataflowPlanBuilder(
            source_node_set=source_node_set,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
            column_association_resolver=self._column_association_resolver,
            node_output_resolver=node_output_resolver,
            source_node_builder=source_node_builder,
            dataflow_plan_builder_cache=self._dataflow_plan_builder_cache,
        )
        self._to_sql_plan_converter = DataflowToSqlPlanConverter(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )
        self._executor = SequentialPlanExecutor()
        self._query_parser = query_parser or MetricFlowQueryParser(
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def query(self, mf_request: MetricFlowQueryRequest) -> MetricFlowQueryResult:  # noqa: D102
        logger.info(LazyFormat("Starting query request", mf_request=mf_request))
        explain_result = self._create_execution_plan(mf_request)
        execution_plan = explain_result.convert_to_execution_plan_result.execution_plan

        if len(execution_plan.tasks) != 1:
            raise NotImplementedError("Multiple tasks not yet supported.")

        task = execution_plan.tasks[0]

        logger.debug(LazyFormat(lambda: f"Sequentially running tasks in:\n" f"{execution_plan.structure_text()}"))
        execution_results = self._executor.execute_plan(execution_plan)
        logger.debug(LazyFormat(lambda: "Finished running tasks in execution plan"))

        if execution_results.contains_task_errors:
            raise ExecutionException(f"Got errors while executing tasks:\n{execution_results.get_result(task.task_id)}")

        task_execution_result = execution_results.get_result(task.task_id)

        assert task_execution_result.sql, "Task execution should have returned SQL that was run"

        logger.info(LazyFormat("Finished query request", request_id=mf_request.request_id))
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
        if self._reset_id_enumeration:
            logger.debug(
                LazyFormat(
                    lambda: f"Setting ID generation to start at: {MetricFlowEngine._ID_ENUMERATION_START_VALUE_FOR_QUERIES}"
                )
            )
            SequentialIdGenerator.reset(MetricFlowEngine._ID_ENUMERATION_START_VALUE_FOR_QUERIES)

        if mf_query_request.saved_query_name is not None:
            if mf_query_request.metrics or mf_query_request.metric_names:
                raise InvalidQueryException("Metrics can't be specified with a saved query.")
            if mf_query_request.group_by or mf_query_request.group_by_names:
                raise InvalidQueryException("Group by items can't be specified with a saved query.")
            query_spec = self._query_parser.parse_and_validate_saved_query(
                saved_query_parameter=SavedQueryParameter(mf_query_request.saved_query_name),
                where_filters=(
                    [
                        PydanticWhereFilter(where_sql_template=where_constraint)
                        for where_constraint in mf_query_request.where_constraints
                    ]
                    if mf_query_request.where_constraints is not None
                    else None
                ),
                limit=mf_query_request.limit,
                time_constraint_start=mf_query_request.time_constraint_start,
                time_constraint_end=mf_query_request.time_constraint_end,
                order_by_names=mf_query_request.order_by_names,
                order_by_parameters=mf_query_request.order_by,
                apply_group_by=mf_query_request.apply_group_by,
            ).query_spec
        else:
            query_spec = self._query_parser.parse_and_validate_query(
                metric_names=mf_query_request.metric_names,
                metrics=mf_query_request.metrics,
                group_by_names=mf_query_request.group_by_names,
                group_by=mf_query_request.group_by,
                limit=mf_query_request.limit,
                time_constraint_start=mf_query_request.time_constraint_start,
                time_constraint_end=mf_query_request.time_constraint_end,
                where_constraint_strs=mf_query_request.where_constraints,
                order_by_names=mf_query_request.order_by_names,
                order_by=mf_query_request.order_by,
                min_max_only=mf_query_request.min_max_only,
                apply_group_by=mf_query_request.apply_group_by,
            ).query_spec
        logger.debug(LazyFormat("Parsed query", query_spec=query_spec))

        output_selection_specs: Optional[InstanceSpecSet] = None
        if mf_query_request.query_type == MetricFlowQueryType.DIMENSION_VALUES:
            # Filter result by dimension columns if it's a dimension values query
            if len(query_spec.entity_specs) > 0:
                raise InvalidQueryException("Querying dimension values for entities is not allowed.")
            output_selection_specs = InstanceSpecSet(
                dimension_specs=query_spec.dimension_specs,
                time_dimension_specs=query_spec.time_dimension_specs,
            )
        if query_spec.metric_specs:
            logger.info(
                LazyFormat(
                    "Building dataflow plan", dataflow_plan_optimizations=mf_query_request.dataflow_plan_optimizations
                )
            )
            dataflow_plan = self._dataflow_plan_builder.build_plan(
                query_spec=query_spec,
                output_selection_specs=output_selection_specs,
                optimizations=mf_query_request.dataflow_plan_optimizations,
            )
        else:
            logger.info(
                LazyFormat(
                    "Building dataflow plan for distinct values",
                    dataflow_plan_optimizations=mf_query_request.dataflow_plan_optimizations,
                )
            )

            dataflow_plan = self._dataflow_plan_builder.build_plan_for_distinct_values(
                query_spec=query_spec, optimizations=mf_query_request.dataflow_plan_optimizations
            )

        if len(dataflow_plan.sink_nodes) > 1:
            raise NotImplementedError(
                f"Multiple output nodes in the dataflow plan not yet supported. "
                f"Got tasks: {dataflow_plan.sink_nodes}"
            )

        logger.info(LazyFormat("Building execution plan"))
        _to_execution_plan_converter = DataflowToExecutionPlanConverter(
            sql_plan_converter=self._to_sql_plan_converter,
            sql_plan_renderer=self._sql_client.sql_plan_renderer,
            sql_client=self._sql_client,
            sql_optimization_level=mf_query_request.sql_optimization_level,
        )

        convert_to_execution_plan_result = _to_execution_plan_converter.convert_to_execution_plan(
            dataflow_plan=dataflow_plan,
            spec_output_order=(
                query_spec.spec_output_order
                # Need to check on how the min/max case should be handled.
                if mf_query_request.order_output_columns_by_input_order and not query_spec.min_max_only
                else ()
            ),
        )
        return MetricFlowExplainResult(
            query_spec=query_spec,
            dataflow_plan=dataflow_plan,
            convert_to_execution_plan_result=convert_to_execution_plan_result,
        )

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def explain(self, mf_request: MetricFlowQueryRequest) -> MetricFlowExplainResult:  # noqa: D102
        with log_block_runtime("explain"):
            return self._create_execution_plan(mf_request)

    def _build_metric_time_dimension(self, time_grain: Optional[ExpandedTimeGranularity]) -> Dimension:
        metric_time_name = DataSet.metric_time_dimension_name()

        return Dimension(
            name=metric_time_name,
            dunder_name=StructuredLinkableSpecName(
                element_name=metric_time_name,
                entity_link_names=(),
                time_granularity_name=(time_grain.name if time_grain is not None else None),
            ).dunder_name,
            entity_links=(),
            description="Event time for metrics.",
            metadata=None,
            type_params=PydanticDimensionTypeParams(
                time_granularity=(time_grain.base_granularity if time_grain is not None else None),
                validity_params=None,
            ),
            is_partition=False,
            type=DimensionType.TIME,
            semantic_model_reference=None,
        )

    def _check_metric_names(self, metric_names: Iterable[str]) -> None:
        """Raise an error that indicates unknown metric names.

        TODO: Check 0-length case. Seems like it's allowed / return an empty result.
        """
        unknown_metric_names = tuple(
            metric_name
            for metric_name in metric_names
            if MetricReference(metric_name) not in self._semantic_manifest_lookup.metric_lookup.metric_references
        )

        if len(unknown_metric_names) > 0:
            raise UnknownMetricError(unknown_metric_names)

    def simple_dimensions_for_metrics(  # noqa: D102
        self,
        metric_names: List[str],
        without_any_property: Sequence[GroupByItemProperty] = tuple(SIMPLE_DIMENSIONS_WITHOUT_ANY_PROPERTIES),
    ) -> List[Dimension]:
        self._check_metric_names(metric_names)

        group_by_item_set = self._semantic_manifest_lookup.metric_lookup.get_common_group_by_items(
            metric_references=tuple(MetricReference(element_name=mname) for mname in metric_names),
            set_filter=GroupByItemSetFilter.create(
                any_properties_denylist=without_any_property,
            ),
        )
        return self._filter_simple_linkable_dimensions(group_by_item_set=group_by_item_set)

    def _filter_simple_linkable_dimensions(self, group_by_item_set: BaseGroupByItemSet) -> List[Dimension]:
        dimensions: List[Dimension] = []

        for annotated_spec in group_by_item_set.annotated_specs:
            properties = annotated_spec.property_set
            element_type = annotated_spec.element_type
            if element_type is LinkableElementType.TIME_DIMENSION:
                # Simple dimensions shouldn't show date part items.
                if GroupByItemProperty.DATE_PART in properties:
                    continue
                if GroupByItemProperty.METRIC_TIME in properties:
                    dimensions.append(self._build_metric_time_dimension(time_grain=annotated_spec.time_grain))
                else:
                    dimensions.extend(self._create_dimension_from_spec(annotated_spec))
            elif element_type is LinkableElementType.DIMENSION:
                dimensions.extend(self._create_dimension_from_spec(annotated_spec))
            elif element_type is LinkableElementType.ENTITY or element_type is LinkableElementType.METRIC:
                pass
            else:
                assert_values_exhausted(element_type)

        return sorted(set(dimensions), key=lambda x: x.default_search_and_sort_attribute)

    def _create_dimension_from_spec(self, annotated_spec: AnnotatedSpec) -> Sequence[Dimension]:
        dimensions: list[Dimension] = []
        for origin_semantic_model_reference in annotated_spec.origin_semantic_model_references:
            assert (
                origin_semantic_model_reference != SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE
            ), "Only metric_time can a virtual model ID."
            semantic_model = self._semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
                origin_semantic_model_reference
            )
            if semantic_model is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Unable to find the semantic model associated with a spec",
                        spec=annotated_spec.spec,
                        origin_semantic_model_reference=origin_semantic_model_reference,
                    )
                )

            dimensions.append(
                Dimension.from_pydantic(
                    pydantic_dimension=SemanticModelHelper.get_dimension_from_semantic_model(
                        semantic_model=semantic_model,
                        dimension_reference=DimensionReference(annotated_spec.element_name),
                    ),
                    entity_links=annotated_spec.spec.entity_links,
                    semantic_model_reference=origin_semantic_model_reference,
                )
            )
        return dimensions

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_dimensions(
        self,
        metric_names: Optional[List[str]] = None,
        order_by: GroupByOrderByAttribute = GroupByOrderByAttribute.DUNDER_NAME,
    ) -> List[Dimension]:
        """Get full dimension object for all dimensions in the semantic manifest."""
        dimensions: List[Dimension] = []
        if metric_names:
            dimensions = self.simple_dimensions_for_metrics(metric_names=metric_names)
        else:
            semantic_model_lookup = self._semantic_manifest_lookup.semantic_model_lookup
            for dimension_reference in semantic_model_lookup.get_dimension_references():
                for semantic_model in semantic_model_lookup.get_semantic_models_for_dimension(dimension_reference):
                    dimension = Dimension.from_pydantic(
                        pydantic_dimension=SemanticModelHelper.get_dimension_from_semantic_model(
                            semantic_model=semantic_model, dimension_reference=dimension_reference
                        ),
                        entity_links=(SemanticModelHelper.resolved_primary_entity(semantic_model),),
                        semantic_model_reference=semantic_model.reference,
                    )
                    dimensions.append(dimension)

        def sort_dimensions(dimension: Dimension) -> Tuple[str, ...]:
            if order_by == GroupByOrderByAttribute.DUNDER_NAME:
                return (dimension.dunder_name,)
            elif order_by == GroupByOrderByAttribute.SEMANTIC_MODEL_NAME:
                return (
                    (
                        dimension.semantic_model_reference.semantic_model_name
                        if dimension.semantic_model_reference
                        else ""
                    ),
                    dimension.dunder_name,
                )
            else:
                assert_values_exhausted(order_by)

        return sorted(set(dimensions), key=sort_dimensions)

    def entities_for_metrics(self, metric_names: List[str]) -> List[Entity]:  # noqa: D102
        group_by_item_set = self._semantic_manifest_lookup.metric_lookup.get_common_group_by_items(
            metric_references=tuple(MetricReference(element_name=mname) for mname in metric_names),
            set_filter=GroupByItemSetFilter.create(
                any_properties_allowlist=ENTITY_WITH_ANY_PROPERTIES,
            ),
        )

        entities = self._filter_linkable_entities(group_by_item_set=group_by_item_set)
        return sorted(set(entities), key=lambda x: x.default_search_and_sort_attribute)

    def _filter_linkable_entities(self, group_by_item_set: BaseGroupByItemSet) -> List[Entity]:
        entities: List[Entity] = []

        for annotated_spec in group_by_item_set.annotated_specs:
            element_type = annotated_spec.element_type
            if element_type is LinkableElementType.ENTITY:
                semantic_model = self._semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
                    mf_first_item(annotated_spec.origin_model_ids).semantic_model_reference
                )
                assert semantic_model
                pydantic_entity = self._semantic_manifest_lookup.semantic_model_lookup.get_entity_in_semantic_model(
                    SemanticModelElementReference(
                        semantic_model_name=mf_first_item(
                            annotated_spec.origin_semantic_model_references
                        ).semantic_model_name,
                        element_name=annotated_spec.spec.element_name,
                    )
                )
                if pydantic_entity:
                    entity = Entity.from_pydantic(
                        pydantic_entity=pydantic_entity,
                        semantic_model_reference=semantic_model.reference,
                    )
                    # Dedupe. Duplicates currently show up because of local linked entities.
                    if entity not in entities:
                        entities.append(entity)
            elif (
                element_type is LinkableElementType.DIMENSION
                or element_type is LinkableElementType.METRIC
                or element_type is LinkableElementType.TIME_DIMENSION
            ):
                pass
            else:
                assert_values_exhausted(element_type)
        return entities

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_metrics(self, include_dimensions: bool = True) -> List[Metric]:
        """List all metrics in semantic manifest matching params. Sorted automatically."""
        metric_lookup = self._semantic_manifest_lookup.metric_lookup
        metrics: List[Metric] = []
        for pydantic_metric in metric_lookup.get_metrics(metric_lookup.metric_references):
            if pydantic_metric.type_params.is_private:
                continue
            metric = Metric.from_pydantic(
                pydantic_metric=pydantic_metric,
                dimensions=self.simple_dimensions_for_metrics([pydantic_metric.name]) if include_dimensions else [],
                semantic_models=list(
                    self._semantic_manifest_lookup.metric_lookup.get_derived_from_semantic_models(
                        MetricReference(pydantic_metric.name)
                    )
                ),
            )
            metrics.append(metric)
        return sorted(metrics, key=lambda m: m.default_search_and_sort_attribute)

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_saved_queries(self) -> List[SavedQuery]:  # noqa: D102
        saved_queries: List[SavedQuery] = []
        for pydantic_saved_query in self._semantic_manifest_lookup.semantic_manifest.saved_queries:
            saved_query = SavedQuery.from_pydantic(pydantic_saved_query)
            saved_queries.append(saved_query)
        return sorted(saved_queries, key=lambda x: x.default_search_and_sort_attribute)

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def get_dimension_values(  # noqa: D102
        self,
        metric_names: List[str],
        get_group_by_values: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> List[str]:
        # Run query
        query_result: MetricFlowQueryResult = self.query(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=metric_names,
                group_by_names=[get_group_by_values],
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
                query_type=MetricFlowQueryType.DIMENSION_VALUES,
            )
        )
        if query_result.result_df is None:
            return []

        return sorted([str(val) for val in query_result.result_df.column_values_iterator(0)])

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def explain_get_dimension_values(  # noqa: D102
        self,
        metric_names: Optional[List[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        get_group_by_values: Optional[str] = None,
        group_by: Optional[GroupByQueryParameter] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        min_max_only: bool = False,
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
                min_max_only=min_max_only,
                query_type=MetricFlowQueryType.DIMENSION_VALUES,
            )
        )

    @log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
    def list_group_bys(
        self,
        metric_names: Optional[List[str]] = None,
        include_derived_time_granularities: bool = False,
        order_by: GroupByOrderByAttribute = GroupByOrderByAttribute.DUNDER_NAME,
    ) -> List[Entity | Dimension]:
        """List all possible group bys, or all group bys allowed for the selected metrics."""
        if metric_names:
            without_any_of = SIMPLE_DIMENSIONS_WITHOUT_ANY_PROPERTIES - ENTITY_WITH_ANY_PROPERTIES
            if include_derived_time_granularities:
                without_any_of = without_any_of - {GroupByItemProperty.DERIVED_TIME_GRANULARITY}
            group_by_item_set = self._semantic_manifest_lookup.metric_lookup.get_common_group_by_items(
                metric_references=tuple(MetricReference(element_name=mname) for mname in metric_names),
                set_filter=GroupByItemSetFilter.create(
                    any_properties_denylist=without_any_of,
                ),
            )
            group_bys: Sequence = self._filter_linkable_entities(
                group_by_item_set=group_by_item_set
            ) + self._filter_simple_linkable_dimensions(group_by_item_set=group_by_item_set)
        else:
            # TODO: better support for querying entities without metrics; include entities here at that time
            group_bys = self.list_dimensions()

        def sort_group_bys(group_by: Entity | Dimension) -> Tuple[str, ...]:
            name_attr = group_by.dunder_name if isinstance(group_by, Dimension) else group_by.name
            if order_by == GroupByOrderByAttribute.DUNDER_NAME:
                return (name_attr,)
            elif order_by == GroupByOrderByAttribute.SEMANTIC_MODEL_NAME:
                return (
                    (
                        group_by.semantic_model_reference.semantic_model_name
                        if group_by.semantic_model_reference
                        else ""
                    ),
                    name_attr,
                )
            else:
                assert_values_exhausted(order_by)

        return sorted(set(group_bys), key=sort_group_bys)

    def group_by_exists(self, structured_name: StructuredLinkableSpecName) -> bool:
        """Check if a group by exists in the semantic manifest by its element name."""
        qualified_name = structured_name.granularity_free_dunder_name
        return (
            qualified_name == METRIC_TIME_ELEMENT_NAME
            or (
                qualified_name
                in self._semantic_manifest_lookup.semantic_model_lookup.dimension_lookup.dimensions_by_dunder_name
            )
            or (
                EntityReference(structured_name.element_name)
                in self._semantic_manifest_lookup.semantic_model_lookup.entity_index
            )
        )
