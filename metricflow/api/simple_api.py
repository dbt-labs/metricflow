from typing import Dict, List, Optional

from metricflow.configuration.config_handler import ConfigHandler
from metricflow.configuration.constants import CONFIG_DWH_SCHEMA
from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import (
    MetricFlowEngine,
    MetricFlowExplainResult,
    MetricFlowQueryRequest,
    MetricFlowQueryResult,
)
from metricflow.engine.models import Dimension, Materialization, Metric
from metricflow.engine.utils import build_user_configured_model_from_config, convert_to_datetime
from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.validations.validator_helpers import ValidationIssueType
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.sql_clients.sql_utils import make_sql_client_from_config


class MetricFlowSimpleAPI:
    """Simple MetricFlow Python Interface."""

    def __init__(
        self,
        sql_client: Optional[SqlClient] = None,
        semantic_model: Optional[SemanticModel] = None,
        system_schema: Optional[str] = None,
    ):
        """If parameters not passed, build via config."""
        handler = ConfigHandler()
        self._sql_client = sql_client or make_sql_client_from_config(handler)
        self._system_schema = system_schema or handler.get_value(CONFIG_DWH_SCHEMA)
        self._semantic_model = semantic_model or SemanticModel(build_user_configured_model_from_config(handler))
        self._mf = MetricFlowEngine(
            semantic_model=self._semantic_model,
            sql_client=self._sql_client,
            system_schema=self._system_schema,
        )

    @property
    def user_configured_model(self) -> UserConfiguredModel:  # noqa: D
        return self._semantic_model.user_configured_model

    @property
    def mf_engine(self) -> MetricFlowEngine:  # noqa: D
        return self._mf

    @property
    def system_schema(self) -> str:  # noqa: D
        return self._system_schema

    def _create_mf_request(
        self,
        metrics: List[str],
        dimensions: List[str],
        limit: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        where: Optional[str] = None,
        order: Optional[List[str]] = None,
        as_table: Optional[str] = None,
        sql_optimization_level: int = 4,
    ):
        """Build MetricFlowQueryRequest given common query parameters."""
        parsed_optimization_level = SqlQueryOptimizationLevel(f"O{sql_optimization_level}")
        parsed_start_time = convert_to_datetime(start_time)
        parsed_end_time = convert_to_datetime(end_time)
        return MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=metrics,
            group_by_names=dimensions,
            limit=limit,
            time_constraint_start=parsed_start_time,
            time_constraint_end=parsed_end_time,
            where_constraint=where,
            order_by_names=order,
            output_table=as_table,
            sql_optimization_level=parsed_optimization_level,
        )

    def query(
        self,
        metrics: List[str],
        dimensions: List[str],
        limit: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        where: Optional[str] = None,
        order: Optional[List[str]] = None,
        as_table: Optional[str] = None,
        sql_optimization_level: int = 4,
    ) -> MetricFlowQueryResult:
        """Makes a query for a metric.

        Args:
            metrics: Names of the metrics to query.
            dimensions: Names of the dimensions and identifiers to query.
            limit: Limit the result to this many rows.
            start_time: Get data for the start of this time range.
            end_time: Get data for the end of this time range.
            where: A SQL string using group by names that can be used like a where clause on the output data.
            order: metric and group by names to order by. A "-" can be used to specify reverse order e.g. "-ds"
            as_table: If specified, output the result data to this table instead of a result dataframe.
            sql_optimization_level: The level of optimization for the generated SQL. Pass integer from 0-4.

        Returns:
            MetricFlowQueryResult that contains the result and context of the query.
        """
        mf_request = self._create_mf_request(
            metrics=metrics,
            dimensions=dimensions,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            where=where,
            order=order,
            as_table=as_table,
            sql_optimization_level=sql_optimization_level,
        )
        return self.mf_engine.query(mf_request=mf_request)

    def explain(
        self,
        metrics: List[str],
        dimensions: List[str],
        limit: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        where: Optional[str] = None,
        order: Optional[List[str]] = None,
        as_table: Optional[str] = None,
        sql_optimization_level: int = 4,
    ) -> MetricFlowExplainResult:
        """Returns the plan for resolving a query.

        Args:
            metrics: Names of the metrics to query.
            dimensions: Names of the dimensions and identifiers to query.
            limit: Limit the result to this many rows.
            start_time: Get data for the start of this time range.
            end_time: Get data for the end of this time range.
            where: A SQL string using group by names that can be used like a where clause on the output data.
            order: metric and group by names to order by. A "-" can be used to specify reverse order e.g. "-ds"
            as_table: If specified, output the result data to this table instead of a result dataframe.
            sql_optimization_level: The level of optimization for the generated SQL. Pass integer from 0-4.

        Returns:
            MetricFlowExplainResult that contains the context of the query.
        """
        mf_request = self._create_mf_request(
            metrics=metrics,
            dimensions=dimensions,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            where=where,
            order=order,
            as_table=as_table,
            sql_optimization_level=sql_optimization_level,
        )
        return self.mf_engine.explain(mf_request=mf_request)

    def list_metrics(self) -> Dict[str, Metric]:
        """Retrieves a list of metric names.

        Returns:
            A dictionary with metric names as the key and the corresponding Metric object as the value.
        """
        return {m.name: m for m in self.mf_engine.list_metrics()}

    def list_dimensions(self, metric_names: List[str]) -> List[Dimension]:
        """Retrieves a list of all common dimensions for metric_names.

        "simple" dimensions are the ones that people expect from a UI perspective. For example, if "ds" is a time
        dimension at a day granularity, this would not list "ds__week".

        Args:
            metric_names: Names of metrics to get common dimensions from.

        Returns:
            A list of Dimension objects containing metadata.
        """
        return self.mf_engine.simple_dimensions_for_metrics(metric_names=metric_names)

    def list_materializations(self) -> List[Materialization]:
        """Retrieves a list of materialization names.

        Returns:
            A list of Materialization objects containing metadata.
        """
        return self.mf_engine.list_materializations()

    def get_dimension_values(
        self, metric_name: str, dimension_name: str, start_time: Optional[str] = None, end_time: Optional[str] = None
    ) -> List[str]:
        """Retrieves a list of dimension values given a [metric_name, dimension_name].

        Args:
            metric_name: Name of metric that contains the group_by.
            dimension_name: Name of group_by to get values from.
            start_time: Get data for the start of this time range.
            end_time: Get data for the end of this time range.

        Returns:
            A list of dimension values as string.
        """
        parsed_start_time = convert_to_datetime(start_time)
        parsed_end_time = convert_to_datetime(end_time)
        return self.mf_engine.get_dimension_values(
            metric_name=metric_name,
            get_group_by_values=dimension_name,
            time_constraint_start=parsed_start_time,
            time_constraint_end=parsed_end_time,
        )

    def materialize(
        self, materialization_name: str, start_time: Optional[str] = None, end_time: Optional[str] = None
    ) -> SqlTable:
        """Builds a table containing metrics and dimensions from a materialization definition.

        This can be very expensive if a large time range is provided.

        Args:
            materialization_name: Name of materialization
            start_time: Materialized for the start of this time range.
            end_time: Materialized for the end of this time range.

        Returns:
            SqlTable object of the materialized table.
        """
        parsed_start_time = convert_to_datetime(start_time)
        parsed_end_time = convert_to_datetime(end_time)
        return self.mf_engine.materialize(
            materialization_name=materialization_name,
            time_constraint_start=parsed_start_time,
            time_constraint_end=parsed_end_time,
        )

    def drop_materialization(self, materialization_name: str) -> bool:
        """Drops the table associated with a materialization definition.

        Args:
            materialization_name: Name of materialization to drop.

        Returns:
            True if a table has been drop, False if table doesn't exist.
        """
        return self.mf_engine.drop_materialization(materialization_name=materialization_name)

    def validate_configs(self) -> List[ValidationIssueType]:
        """Validate a model according to configured rules.

        Returns:
            List of validation issues with the model provided.
        """
        issues = ModelValidator.validate_model(self.user_configured_model).issues
        return [issue for issue in issues] if issues is not None else []
