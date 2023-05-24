from __future__ import annotations

import collections
import traceback
from copy import deepcopy
from dataclasses import dataclass, field
from functools import partial
from math import floor
from time import perf_counter
from typing import Callable, DefaultDict, Dict, List, Optional, Sequence, Tuple, TypeVar

from dbt_semantic_interfaces.objects.elements.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    MetricModelReference,
    SemanticModelElementReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    ModelValidationResults,
    SemanticModelContext,
    SemanticModelElementContext,
    SemanticModelElementType,
    ValidationContext,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
)

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import BaseOutput, FilterElementsNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.dataset import DataSet
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import DimensionSpec, InstanceSpecSet, LinkableInstanceSpec, MeasureSpec
from metricflow.sql.sql_bind_parameters import SqlBindParameters


@dataclass
class QueryRenderingTools:
    """General tools that data warehosue validations use for rendering the validation queries.

    This is necessary for the validation steps that generate raw partial queries against the individual semantic models
    (e.g., selecting a single dimension column).
    """

    semantic_manifest_lookup: SemanticManifestLookup
    source_node_builder: SourceNodeBuilder
    converter: SemanticModelToDataSetConverter
    time_spine_source: TimeSpineSource
    plan_converter: DataflowToSqlQueryPlanConverter

    def __init__(self, model: SemanticManifest, system_schema: str) -> None:  # noqa: D
        self.semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest=model)
        self.source_node_builder = SourceNodeBuilder(semantic_manifest_lookup=self.semantic_manifest_lookup)
        self.time_spine_source = TimeSpineSource(schema_name=system_schema)
        self.converter = SemanticModelToDataSetConverter(
            column_association_resolver=DunderColumnAssociationResolver(
                semantic_manifest_lookup=self.semantic_manifest_lookup
            )
        )
        self.plan_converter = DataflowToSqlQueryPlanConverter(
            column_association_resolver=DunderColumnAssociationResolver(self.semantic_manifest_lookup),
            semantic_manifest_lookup=self.semantic_manifest_lookup,
            time_spine_source=self.time_spine_source,
        )
        self.node_resolver = DataflowPlanNodeOutputDataSetResolver[SemanticModelDataSet](
            column_association_resolver=DunderColumnAssociationResolver(self.semantic_manifest_lookup),
            semantic_manifest_lookup=self.semantic_manifest_lookup,
            time_spine_source=self.time_spine_source,
        )


@dataclass
class DataWarehouseValidationTask:
    """Dataclass for defining a task to be used in the DataWarehouseModelValidator."""

    query_and_params_callable: Callable[[], Tuple[str, SqlBindParameters]]
    error_message: str
    context: Optional[ValidationContext] = None
    on_fail_subtasks: List[DataWarehouseValidationTask] = field(default_factory=lambda: [])


LinkableInstanceSpecT = TypeVar("LinkableInstanceSpecT", bound=LinkableInstanceSpec)


class DataWarehouseTaskBuilder:
    """Task builder for standard data warehouse validation tasks."""

    @staticmethod
    def _remove_entity_link_specs(specs: Tuple[LinkableInstanceSpecT, ...]) -> Tuple[LinkableInstanceSpecT, ...]:
        """For the purposes of data warehouse validation, specs with entity_links are unnecesary."""
        return tuple(spec for spec in specs if not spec.entity_links)

    @staticmethod
    def _semantic_model_nodes(
        render_tools: QueryRenderingTools, semantic_model: SemanticModel
    ) -> Sequence[BaseOutput[SemanticModelDataSet]]:
        """Builds and returns the SemanticModelDataSet node for the given semantic model."""
        semantic_model_lookup = render_tools.semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
            SemanticModelReference(semantic_model_name=semantic_model.name)
        )
        assert semantic_model_lookup

        source_nodes = render_tools.source_node_builder.create_from_data_sets(
            (render_tools.converter.create_sql_source_data_set(semantic_model_lookup),)
        )

        assert len(source_nodes) >= 1
        return source_nodes

    @staticmethod
    def renderize(
        sql_client: SqlClient, plan_converter: DataflowToSqlQueryPlanConverter, plan_id: str, nodes: FilterElementsNode
    ) -> Tuple[str, SqlBindParameters]:
        """Generates a sql query plan and returns the rendered sql and bind_parameters."""
        sql_plan = plan_converter.convert_to_sql_query_plan(
            sql_engine_attributes=sql_client.sql_engine_attributes,
            sql_query_plan_id=plan_id,
            dataflow_plan_node=nodes,
        )

        rendered_plan = sql_client.sql_engine_attributes.sql_query_plan_renderer.render_sql_query_plan(sql_plan)
        return (rendered_plan.sql, rendered_plan.bind_parameters)

    @classmethod
    def gen_semantic_model_tasks(
        cls, model: SemanticManifest, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the semantic models of the model."""
        # we need a dimension to query that we know exists (i.e. the dimension
        # is guaranteed to not cause a problem) on each semantic model.
        # Additionally, we don't want to modify the original model, so we
        # first make a deep copy of it
        model = deepcopy(model)
        for semantic_model in model.semantic_models:
            semantic_model.dimensions = list(semantic_model.dimensions) + [
                Dimension(name=f"validation_dim_for_{semantic_model.name}", type=DimensionType.CATEGORICAL, expr="1")
            ]

        render_tools = QueryRenderingTools(model=model, system_schema=system_schema)

        tasks: List[DataWarehouseValidationTask] = []
        for semantic_model in model.semantic_models:
            source_node = cls._semantic_model_nodes(render_tools=render_tools, semantic_model=semantic_model)[0]
            spec = DimensionSpec.from_name(name=f"validation_dim_for_{semantic_model.name}")
            filter_elements_node = FilterElementsNode(
                parent_node=source_node, include_specs=InstanceSpecSet(dimension_specs=(spec,))
            )

            tasks.append(
                DataWarehouseValidationTask(
                    query_and_params_callable=partial(
                        cls.renderize,
                        sql_client=sql_client,
                        plan_converter=render_tools.plan_converter,
                        plan_id=f"{semantic_model.name}_validation",
                        nodes=filter_elements_node,
                    ),
                    context=SemanticModelContext(
                        file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                        semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
                    ),
                    error_message=f"Unable to access semantic model `{semantic_model.name}` in data warehouse",
                )
            )

        return tasks

    @classmethod
    def gen_dimension_tasks(
        cls, model: SemanticManifest, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the dimensions of the model.

        The high level tasks returned are "short cut" queries which try to
        query all the dimensions for a given semantic model. If that query fails,
        one or more of the dimensions is incorrectly specified. Thus if the
        query fails, there are subtasks which query the individual dimensions
        on the semantic model to identify which have issues.
        """
        render_tools = QueryRenderingTools(model=model, system_schema=system_schema)

        tasks: List[DataWarehouseValidationTask] = []
        for semantic_model in model.semantic_models:
            if not semantic_model.dimensions:
                continue

            source_node = cls._semantic_model_nodes(render_tools=render_tools, semantic_model=semantic_model)[0]

            semantic_model_sub_tasks: List[DataWarehouseValidationTask] = []
            dataset = render_tools.converter.create_sql_source_data_set(semantic_model)

            dimension_specs = DataWarehouseTaskBuilder._remove_entity_link_specs(
                dataset.instance_set.spec_set.dimension_specs
            )

            spec_filter_tuples = []
            for spec in dimension_specs:
                spec_filter_tuples.append(
                    (
                        spec,
                        FilterElementsNode(
                            parent_node=source_node, include_specs=InstanceSpecSet(dimension_specs=(spec,))
                        ),
                    )
                )

            time_dimension_specs = DataWarehouseTaskBuilder._remove_entity_link_specs(
                dataset.instance_set.spec_set.time_dimension_specs
            )
            for spec in time_dimension_specs:
                spec_filter_tuples.append(
                    (
                        spec,
                        FilterElementsNode(
                            parent_node=source_node, include_specs=InstanceSpecSet(time_dimension_specs=(spec,))
                        ),
                    )
                )

            for spec, filter_elements_node in spec_filter_tuples:
                semantic_model_sub_tasks.append(
                    DataWarehouseValidationTask(
                        query_and_params_callable=partial(
                            cls.renderize,
                            sql_client=sql_client,
                            plan_converter=render_tools.plan_converter,
                            plan_id=f"{semantic_model.name}_dim_{spec.element_name}_validation",
                            nodes=filter_elements_node,
                        ),
                        context=SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=spec.element_name
                            ),
                            element_type=SemanticModelElementType.DIMENSION,
                        ),
                        error_message=f"Unable to query dimension `{spec.element_name}` on semantic model `{semantic_model.name}` in data warehouse",
                    )
                )

            filter_elements_node = FilterElementsNode(
                parent_node=source_node,
                include_specs=InstanceSpecSet(
                    dimension_specs=dimension_specs,
                    time_dimension_specs=time_dimension_specs,
                ),
            )
            tasks.append(
                DataWarehouseValidationTask(
                    query_and_params_callable=partial(
                        cls.renderize,
                        sql_client=sql_client,
                        plan_converter=render_tools.plan_converter,
                        plan_id=f"{semantic_model.name}_all_dimensions_validation",
                        nodes=filter_elements_node,
                    ),
                    context=SemanticModelContext(
                        file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                        semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
                    ),
                    error_message=f"Failed to query dimensions in data warehouse for semantic model `{semantic_model.name}`",
                    on_fail_subtasks=semantic_model_sub_tasks,
                )
            )
        return tasks

    @classmethod
    def gen_entity_tasks(
        cls, model: SemanticManifest, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the entities of the model.

        The high level tasks returned are "short cut" queries which try to
        query all the entities for a given semantic model. If that query fails,
        one or more of the entities is incorrectly specified. Thus if the
        query fails, there are subtasks which query the individual entities
        on the semantic model to identify which have issues.
        """
        render_tools = QueryRenderingTools(model=model, system_schema=system_schema)

        tasks: List[DataWarehouseValidationTask] = []
        for semantic_model in model.semantic_models:
            if not semantic_model.entities:
                continue
            source_node = cls._semantic_model_nodes(render_tools=render_tools, semantic_model=semantic_model)[0]

            semantic_model_sub_tasks: List[DataWarehouseValidationTask] = []
            dataset = render_tools.converter.create_sql_source_data_set(semantic_model)
            semantic_model_specs = DataWarehouseTaskBuilder._remove_entity_link_specs(
                dataset.instance_set.spec_set.entity_specs
            )
            for spec in semantic_model_specs:
                filter_elements_node = FilterElementsNode(
                    parent_node=source_node, include_specs=InstanceSpecSet(entity_specs=(spec,))
                )
                semantic_model_sub_tasks.append(
                    DataWarehouseValidationTask(
                        query_and_params_callable=partial(
                            cls.renderize,
                            sql_client=sql_client,
                            plan_converter=render_tools.plan_converter,
                            plan_id=f"{semantic_model.name}_entity_{spec.element_name}_validation",
                            nodes=filter_elements_node,
                        ),
                        context=SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=spec.element_name
                            ),
                            element_type=SemanticModelElementType.ENTITY,
                        ),
                        error_message=f"Unable to query entity `{spec.element_name}` on semantic model `{semantic_model.name}` in data warehouse",
                    )
                )

            filter_elements_node = FilterElementsNode(
                parent_node=source_node,
                include_specs=InstanceSpecSet(
                    entity_specs=tuple(semantic_model_specs),
                ),
            )
            tasks.append(
                DataWarehouseValidationTask(
                    query_and_params_callable=partial(
                        cls.renderize,
                        sql_client=sql_client,
                        plan_converter=render_tools.plan_converter,
                        plan_id=f"{semantic_model.name}_all_entities_validation",
                        nodes=filter_elements_node,
                    ),
                    context=SemanticModelContext(
                        file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                        semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
                    ),
                    error_message=f"Failed to query entities in data warehouse for semantic model `{semantic_model.name}`",
                    on_fail_subtasks=semantic_model_sub_tasks,
                )
            )
        return tasks

    @classmethod
    def gen_measure_tasks(
        cls, model: SemanticManifest, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the measures of the model.

        The high level tasks returned are "short cut" queries which try to
        query all the measures for a given semantic model. If that query fails,
        one or more of the measures is incorrectly specified. Thus if the
        query fails, there are subtasks which query the individual measures
        on the semantic model to identify which have issues.
        """
        render_tools = QueryRenderingTools(model=model, system_schema=system_schema)

        tasks: List[DataWarehouseValidationTask] = []
        for semantic_model in model.semantic_models:
            if not semantic_model.measures:
                continue

            source_nodes = cls._semantic_model_nodes(render_tools=render_tools, semantic_model=semantic_model)
            dataset = render_tools.converter.create_sql_source_data_set(semantic_model)
            semantic_model_specs = dataset.instance_set.spec_set.measure_specs

            source_node_by_measure_spec: Dict[MeasureSpec, BaseOutput[SemanticModelDataSet]] = {}
            measure_specs_source_node_pair = []
            for source_node in source_nodes:
                measure_specs = render_tools.node_resolver.get_output_data_set(
                    source_node
                ).instance_set.spec_set.measure_specs
                source_node_by_measure_spec.update({measure_spec: source_node for measure_spec in measure_specs})
                measure_specs_source_node_pair.append((measure_specs, source_node))

            source_node_to_sub_task: DefaultDict[
                BaseOutput[SemanticModelDataSet], List[DataWarehouseValidationTask]
            ] = collections.defaultdict(list)
            for spec in semantic_model_specs:
                obtained_source_node = source_node_by_measure_spec.get(spec)
                assert obtained_source_node, f"Unable to find generated source node for measure: {spec.element_name}"

                filter_elements_node = FilterElementsNode(
                    parent_node=obtained_source_node,
                    include_specs=InstanceSpecSet(
                        measure_specs=(spec,),
                    ),
                )
                source_node_to_sub_task[obtained_source_node].append(
                    DataWarehouseValidationTask(
                        query_and_params_callable=partial(
                            cls.renderize,
                            sql_client=sql_client,
                            plan_converter=render_tools.plan_converter,
                            plan_id=f"{semantic_model.name}_measure_{spec.element_name}_validation",
                            nodes=filter_elements_node,
                        ),
                        context=SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=spec.element_name
                            ),
                            element_type=SemanticModelElementType.MEASURE,
                        ),
                        error_message=f"Unable to query measure `{spec.element_name}` on semantic model `{semantic_model.name}` in data warehouse",
                    )
                )

            for measure_specs, source_node in measure_specs_source_node_pair:
                filter_elements_node = FilterElementsNode(
                    parent_node=source_node, include_specs=InstanceSpecSet(measure_specs=measure_specs)
                )
                tasks.append(
                    DataWarehouseValidationTask(
                        query_and_params_callable=partial(
                            cls.renderize,
                            sql_client=sql_client,
                            plan_converter=render_tools.plan_converter,
                            plan_id=f"{semantic_model.name}_all_measures_validation",
                            nodes=filter_elements_node,
                        ),
                        context=SemanticModelContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model=SemanticModelReference(semantic_model_name=semantic_model.name),
                        ),
                        error_message=f"Failed to query measures in data warehouse for semantic model `{semantic_model.name}`",
                        on_fail_subtasks=source_node_to_sub_task[source_node],
                    )
                )
        return tasks

    @staticmethod
    def _gen_metric_task_query_and_params(
        metric: Metric, mf_engine: MetricFlowEngine
    ) -> Tuple[str, SqlBindParameters]:  # noqa: D
        mf_query = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=[metric.name], group_by_names=[DataSet.metric_time_dimension_name()]
        )
        explain_result: MetricFlowExplainResult = mf_engine.explain(mf_request=mf_query)
        return (explain_result.rendered_sql.sql_query, explain_result.rendered_sql.bind_parameters)

    @classmethod
    def gen_metric_tasks(
        cls, model: SemanticManifest, sql_client: AsyncSqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the metrics of the model."""
        mf_engine = MetricFlowEngine(
            semantic_manifest_lookup=SemanticManifestLookup(semantic_manifest=model),
            sql_client=sql_client,
            system_schema=system_schema,
        )
        tasks: List[DataWarehouseValidationTask] = []
        for metric in model.metrics:
            tasks.append(
                DataWarehouseValidationTask(
                    query_and_params_callable=partial(
                        cls._gen_metric_task_query_and_params,
                        metric=metric,
                        mf_engine=mf_engine,
                    ),
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    error_message=f"Unable to query metric `{metric.name}`.",
                )
            )
        return tasks


class DataWarehouseModelValidator:
    """A Validator for checking specific tasks for the model against the Data Warehouse.

    Data Warehouse Validations are validations that are done against the data
    warehouse based on the model configured by the user. Their purpose is to
    ensure that queries generated by MetricFlow won't fail when you go to use
    them (assuming the model has passed these validations before use).
    """

    def __init__(self, sql_client: AsyncSqlClient, system_schema: str) -> None:  # noqa: D
        self._sql_client = sql_client
        self._sql_schema = system_schema

    def run_tasks(
        self, tasks: List[DataWarehouseValidationTask], timeout: Optional[int] = None
    ) -> ModelValidationResults:
        """Runs the list of tasks as queries agains the data warehouse, returning any found issues.

        Args:
            tasks: A list of tasks to run against the data warehouse
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues discovered when running the passed in tasks against the data warehosue
        """
        # Used for keeping track if we go past the max time
        start_time = perf_counter()

        issues: List[ValidationIssue] = []
        # TODO: Asyncio implementation
        for index, task in enumerate(tasks):
            if timeout is not None and perf_counter() - start_time > timeout:
                issues.append(
                    ValidationWarning(
                        context=None,
                        message=f"Hit timeout before completing all tasks. Completed {index}/{len(tasks)} tasks.",
                    )
                )
                break
            try:
                (query_string, query_params) = task.query_and_params_callable()
                self._sql_client.dry_run(stmt=query_string, sql_bind_parameters=query_params)
            except Exception as e:
                issues.append(
                    ValidationError(
                        context=task.context,
                        message=task.error_message + f"\nRecieved following error from data warehouse:\n{e}",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )
                if task.on_fail_subtasks:
                    sub_task_timeout = floor(timeout - (perf_counter() - start_time)) if timeout else None
                    issues += self.run_tasks(tasks=task.on_fail_subtasks, timeout=sub_task_timeout).all_issues

        return ModelValidationResults.from_issues_sequence(issues)

    def validate_semantic_models(
        self, model: SemanticManifest, timeout: Optional[int] = None
    ) -> ModelValidationResults:
        """Generates a list of tasks for validating the semantic models of the model and then runs them.

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues discovered when running the passed in tasks against the data warehosue
        """
        tasks = DataWarehouseTaskBuilder.gen_semantic_model_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_dimensions(self, model: SemanticManifest, timeout: Optional[int] = None) -> ModelValidationResults:
        """Generates a list of tasks for validating the dimensions of the model and then runs them.

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """
        tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_entities(self, model: SemanticManifest, timeout: Optional[int] = None) -> ModelValidationResults:
        """Generates a list of tasks for validating the entities of the model and then runs them.

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """
        tasks = DataWarehouseTaskBuilder.gen_entity_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_measures(self, model: SemanticManifest, timeout: Optional[int] = None) -> ModelValidationResults:
        """Generates a list of tasks for validating the measures of the model and then runs them.

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """
        tasks = DataWarehouseTaskBuilder.gen_measure_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_metrics(self, model: SemanticManifest, timeout: Optional[int] = None) -> ModelValidationResults:
        """Generates a list of tasks for validating the metrics of the model and then runs them.

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """
        tasks = DataWarehouseTaskBuilder.gen_metric_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)
